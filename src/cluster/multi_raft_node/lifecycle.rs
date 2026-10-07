use std::collections::{BTreeSet, HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::watch;

use openraft::type_config::async_runtime::WatchReceiver;

use crate::cluster::network::{
    raft_rpc, RaftNetworkClientFactory, RaftServiceDispatcher, RaftServiceImpl,
};
use crate::cluster::node::OpenRaftNode;
use crate::cluster::sharded_storage::ShardedStorage;
use crate::cluster::types::{ClusterError, NodeId, RaftNodeConfig};
use crate::error::Result;

use super::{group_restart_backoff, GroupRestartState, LifecycleConfig, MultiRaftNode};

impl MultiRaftNode {
    /// 启动统一 gRPC 服务 (绑定单一端口, 所有数据 Group 共享).
    ///
    /// 数据 Group 不启动自己的 gRPC server, 而是通过此统一端口接收 RPC.
    pub async fn start(&self, addr: SocketAddr, max_message_size: u64) -> Result<()> {
        use raft_rpc::raft_service_server::RaftServiceServer;
        use tokio::net::TcpListener;

        let listener = TcpListener::bind(addr).await.map_err(ClusterError::Io)?;
        let service = RaftServiceImpl::new(self.grpc_dispatcher.clone());
        let server = RaftServiceServer::new(service)
            .max_decoding_message_size(max_message_size as usize)
            .max_encoding_message_size(max_message_size as usize);

        let handle = tokio::spawn(async move {
            crate::cluster::network::raft_server()
                .add_service(server)
                .serve_with_incoming(crate::cluster::network::tcp_incoming(listener))
                .await
                .ok();
        });
        *self.server_handle.lock() = Some(handle);
        Ok(())
    }

    /// 启动 LifecycleManager 后台 task (仅日志, 无实际 Group 创建/销毁).
    ///
    /// 返回 `watch::Receiver<bool>`, 调用方可通过 `changed()` 监听关闭信号.
    pub fn start_lifecycle(&self) -> watch::Receiver<bool> {
        self.start_lifecycle_impl(None)
    }

    /// 启动 LifecycleManager 后台 task, 并自动创建/销毁数据 Group.
    pub fn start_lifecycle_with_data(&self, config: LifecycleConfig) -> watch::Receiver<bool> {
        self.start_lifecycle_impl(Some(config))
    }

    fn start_lifecycle_impl(
        &self,
        mut lifecycle_config: Option<LifecycleConfig>,
    ) -> watch::Receiver<bool> {
        if let Some(ref mut cfg) = lifecycle_config {
            self.inject_shared_statistics(cfg);
        }
        let (tx, rx) = watch::channel(false);
        *self.shutdown_tx.lock() = Some(tx);
        let lifecycle = self.lifecycle.clone();
        let groups = self.groups.clone();
        let storages = self.storages.clone();
        let dispatcher = self.grpc_dispatcher.clone();
        let node_id = self.node_id;
        let stats = Arc::clone(&self.stats);
        // 复用 self.network_factory (而非每次启动 lifecycle 都新建一个孤立实例),
        // 使 get_key_from_group_remote/read_migration_tip 的远程 fallback 能
        // 拿到与本地 group 对等 raft 通信同一份 (node_id -> addr) 缓存 (FIX-0056-A1).
        let net_factory = self.network_factory.clone();
        let restart_state: Arc<RwLock<HashMap<u64, GroupRestartState>>> =
            Arc::new(RwLock::new(HashMap::new()));

        tokio::spawn(async move {
            let mut shutdown_rx = rx;
            let cfg = lifecycle_config;
            let rpc_timeout = cfg.as_ref().map_or(
                RaftNodeConfig::default().rpc_timeout_ms,
                |c: &LifecycleConfig| c.raft_node_config.rpc_timeout_ms,
            );
            let msg_size = cfg
                .as_ref()
                .map_or(RaftNodeConfig::default().grpc_max_message_size, |c| {
                    c.raft_node_config.grpc_max_message_size
                });
            *net_factory.write() = RaftNetworkClientFactory::new_with_stats(
                node_id,
                0,
                rpc_timeout,
                msg_size,
                Some(Arc::clone(&stats)),
            );

            let mut next_delay = std::time::Duration::ZERO;
            loop {
                tokio::select! {
                  _ = tokio::time::sleep(next_delay) => {
                    next_delay = lifecycle.tick_interval();
                    let tick_result = lifecycle.tick();
                    if let Some(ref cfg) = cfg {
                      // 从 MetaRaft 元数据判断本节点是否为 group 的 leader (is_leader=true).
                      // 只有 leader 初始化为 Voter; 其他成员等待 drift 对账被加入.
                      let meta = lifecycle.meta_raft().get_cluster_meta();
                      for gid in tick_result.groups_to_create {
                        let is_leader = meta
                          .groups
                          .get(&gid)
                          .and_then(|g| g.replicas.iter().find(|r| r.node_id == node_id))
                          .map(|r| r.is_leader)
                          .unwrap_or(false);
                        Self::create_group_inner(
                          gid,
                          is_leader,
                          meta.nodes.get(&node_id).map(|n| n.rpc_addr.as_str()),
                          &groups,
                          &storages,
                          &dispatcher,
                          cfg,
                          &net_factory,
                        )
                        .await;
                      }
                      for gid in tick_result.groups_to_remove {
                        Self::remove_group_inner(gid, &groups, &storages, &dispatcher).await;
                      }

                      // Fail-fast 自愈: apply.rs 遇到真实存储故障时会让 openraft
                      // 把该 group 标记为 Fatal 并停止服务 (见 stage2-apply).
                      // 这里检测 Fatal 状态并尝试就地重开 group, 避免每次真实
                      // 磁盘故障之外的偶发错误都需要人工介入重启整个进程.
                      Self::supervise_groups(
                        &groups,
                        &storages,
                        &dispatcher,
                        cfg,
                        &net_factory,
                        &restart_state,
                      )
                      .await;
                    } else {
                      if !tick_result.groups_to_create.is_empty() {
                        tracing::info!(groups_to_create = ?tick_result.groups_to_create, "group lifecycle tick (no-op)");
                      }
                      if !tick_result.groups_to_remove.is_empty() {
                        tracing::info!(groups_to_remove = ?tick_result.groups_to_remove, "group lifecycle tick (no-op)");
                      }
                    }

                    // Drift detection and reconciliation (only when cfg is Some).
                    if cfg.is_some() {
                      let mut applied = 0;
                      for (group_id, expected_members) in &tick_result.expected_memberships {
                        // 每次 tick 最多处理 1 个 drift, 避免批量 joint-consensus 操作引发集群不稳定.
                        if applied >= 1 {
                          break;
                        }

                        // 1. 获取实际 Raft 成员集合
                        let maybe_node = {
                          let groups_read = groups.read();
                          groups_read.get(group_id).cloned()
                        };
                        let actual = match maybe_node {
                          Some(node) => node.get_members().await,
                          None => continue,
                        };

                        // 2. 计算 drift
                        let to_add: Vec<NodeId> =
                          expected_members.difference(&actual).copied().collect();
                        let to_remove: Vec<NodeId> =
                          actual.difference(expected_members).copied().collect();
                        if to_add.is_empty() && to_remove.is_empty() {
                          continue;
                        }

                        // 3. Leader 检查 — 确保 drop lock 后再 .await
                        let maybe_node = {
                          let groups_read = groups.read();
                          groups_read.get(group_id).cloned()
                        };
                        let is_leader = match maybe_node {
                          Some(node) => node.is_leader().await,
                          None => false,
                        };
                        if !is_leader {
                          continue;
                        }

                        // 4. 执行对账
                        tracing::info!(
                          group_id = *group_id,
                          ?to_add,
                          ?to_remove,
                          "reconciling group membership drift"
                        );

                        let cluster_meta = lifecycle.meta_raft().get_cluster_meta();
                        if let Err(e) = Self::apply_membership_change(
                          *group_id,
                          expected_members,
                          &to_add,
                          &groups,
                          &cluster_meta,
                        )
                        .await
                        {
                          tracing::warn!(
                            group_id = *group_id,
                            error = %e,
                            "membership reconcile failed, will retry next tick"
                          );
                        }
                        applied += 1;
                      }
                    }
                  }
                  _ = shutdown_rx.changed() => {
                    tracing::info!("lifecycle manager shutting down");
                    break;
                  }
                }
            }
        });

        // 返回一个新的 subscriber, 调用方可接收关闭通知
        self.shutdown_tx.lock().as_ref().unwrap().subscribe()
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn create_group_inner(
        group_id: u64,
        init_as_voter: bool,
        init_rpc_addr: Option<&str>,
        groups: &Arc<RwLock<HashMap<u64, Arc<OpenRaftNode>>>>,
        storages: &Arc<RwLock<HashMap<u64, ShardedStorage>>>,
        dispatcher: &Arc<RaftServiceDispatcher>,
        cfg: &LifecycleConfig,
        net_factory: &Arc<RwLock<RaftNetworkClientFactory>>,
    ) {
        if groups.read().contains_key(&group_id) {
            return;
        }
        match Self::open_group(group_id, cfg, net_factory).await {
            Ok((node, storage)) => {
                let node = Arc::new(node);
                let raft = node.raft().clone();
                let node_id = cfg.raft_node_config.node_id;

                // 单节点 Voter 初始化: 优先 MetaRaft RPC 地址 (Docker 网络可达).
                // 若磁盘上已有 Raft 状态, initialize 会拒绝 — 此时仍注册 group 供读写.
                if init_as_voter {
                    let addr = init_rpc_addr
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("127.0.0.1:{}", 10000 + node_id));
                    match node.initialize(vec![(node_id, addr)]).await {
                        Ok(()) => {
                            tracing::info!(
                                group_id,
                                node_id,
                                "group initialized with single voter"
                            );
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            if msg.contains("not allowed to initialize") {
                                tracing::info!(
                                    group_id,
                                    node_id,
                                    "group reuses existing raft state"
                                );
                            } else {
                                tracing::warn!(
                                  group_id,
                                  node_id,
                                  error = %e,
                                  "initialize failed, registering opened group"
                                );
                            }
                        }
                    }
                }

                dispatcher.register_group(group_id, raft);
                dispatcher.register_node(group_id, node.clone());
                groups.write().insert(group_id, node);
                storages.write().insert(group_id, storage);
                tracing::info!(group_id, init_as_voter, "group created and registered");
            }
            Err(e) => {
                tracing::error!(group_id, error = %e, "failed to create group");
            }
        }
    }

    async fn open_group(
        group_id: u64,
        cfg: &LifecycleConfig,
        net_factory: &Arc<RwLock<RaftNetworkClientFactory>>,
    ) -> Result<(OpenRaftNode, ShardedStorage)> {
        let storage = ShardedStorage::open(&cfg.data_dir, group_id, cfg.options.clone())?;
        storage.set_compaction_filter(cfg.compaction_filter.clone());
        if let Some(factory) = &cfg.compaction_removal_listener_factory {
            storage.set_compaction_removal_listener(Some(factory(storage.db().clone())));
        }
        let db = storage.db().clone();
        let mut config = cfg.raft_node_config.clone();
        config.group_id = group_id;
        let factory = net_factory.read().clone().with_group_id(group_id);
        let node = OpenRaftNode::new(config, db, factory).await?;
        Ok((node, storage))
    }

    async fn remove_group_inner(
        group_id: u64,
        groups: &Arc<RwLock<HashMap<u64, Arc<OpenRaftNode>>>>,
        storages: &Arc<RwLock<HashMap<u64, ShardedStorage>>>,
        dispatcher: &Arc<RaftServiceDispatcher>,
    ) {
        let node = groups.write().remove(&group_id);
        let storage = storages.write().remove(&group_id);
        if let Some(n) = node {
            let _ = n.shutdown().await;
        }
        if let Some(s) = storage {
            let _ = s.close();
        }
        dispatcher.unregister_group(group_id);
        tracing::info!(group_id, "group removed and unregistered");
    }

    /// 扫描本地 group, 检测是否有实例进入 openraft 的 `Fatal` 状态 (由
    /// `apply_to_state_machine` 遇到真实存储错误后 fail-fast 触发), 并对
    /// 命中的 group 尝试就地重开 (等价于"重启这一个 group 的进程", 但不影响
    /// 同一节点上其它 group 或 gRPC server).
    ///
    /// 重开时**不**传 `init_as_voter=true` —— 该 group 之前已经是集群里正常
    /// 运行的成员 (leader 或 follower), 磁盘上已经有它自己的 log/vote/
    /// membership 状态; 这里只是重新加载现有状态, 不是重新做单节点 bootstrap.
    pub(super) async fn supervise_groups(
        groups: &Arc<RwLock<HashMap<u64, Arc<OpenRaftNode>>>>,
        storages: &Arc<RwLock<HashMap<u64, ShardedStorage>>>,
        dispatcher: &Arc<RaftServiceDispatcher>,
        cfg: &LifecycleConfig,
        net_factory: &Arc<RwLock<RaftNetworkClientFactory>>,
        restart_state: &Arc<RwLock<HashMap<u64, GroupRestartState>>>,
    ) {
        let fatal_groups: Vec<(u64, String)> = {
            groups
                .read()
                .iter()
                .filter_map(|(gid, node)| {
                    match node.raft().metrics().borrow_watched().running_state {
                        Ok(()) => None,
                        Err(ref fatal) => Some((*gid, fatal.to_string())),
                    }
                })
                .collect()
        };

        // 已恢复健康的 group 清除退避计数, 下次真正故障时重新从最短退避开始.
        if !fatal_groups.is_empty() || !restart_state.read().is_empty() {
            let fatal_ids: HashSet<u64> = fatal_groups.iter().map(|(gid, _)| *gid).collect();
            restart_state
                .write()
                .retain(|gid, _| fatal_ids.contains(gid));
        }

        for (group_id, reason) in fatal_groups {
            if let Some(stats) = &cfg.options.statistics {
                stats
                    .raft_group_fatal
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }

            let should_attempt = {
                let mut state = restart_state.write();
                let entry = state.entry(group_id).or_insert(GroupRestartState {
                    last_attempt: None,
                    consecutive_failures: 0,
                });
                let backoff = group_restart_backoff(entry.consecutive_failures);
                let ready = entry.last_attempt.is_none_or(|t| t.elapsed() >= backoff);
                if ready {
                    entry.last_attempt = Some(std::time::Instant::now());
                    entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
                }
                ready
            };

            if !should_attempt {
                tracing::debug!(
                    group_id,
                    "raft group still in self-heal backoff window, skip this tick"
                );
                continue;
            }

            tracing::error!(
                group_id,
                error = %reason,
                "raft group entered fatal state, attempting in-process self-heal restart"
            );

            Self::remove_group_inner(group_id, groups, storages, dispatcher).await;
            Self::create_group_inner(
                group_id,
                false,
                None,
                groups,
                storages,
                dispatcher,
                cfg,
                net_factory,
            )
            .await;

            if groups.read().contains_key(&group_id) {
                tracing::warn!(group_id, "raft group self-heal restart succeeded");
                if let Some(stats) = &cfg.options.statistics {
                    stats.raft_group_restart
                        [crate::statistics::RaftRestartOutcome::Success as usize]
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            } else {
                tracing::error!(
                    group_id,
                    "raft group self-heal restart failed to reopen, will retry with backoff"
                );
                if let Some(stats) = &cfg.options.statistics {
                    stats.raft_group_restart
                        [crate::statistics::RaftRestartOutcome::Failure as usize]
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }

    /// 对账成员变更 — 关联函数, 与 `create_group_inner` / `remove_group_inner` 模式一致.
    ///
    /// 注意: 此方法**不**调用 `meta_raft.propose(ChangeGroupMembership)` —
    /// MetaRaft 元数据已经是期望状态, 此方法只执行 Raft 层面的操作.
    async fn apply_membership_change(
        group_id: u64,
        expected: &BTreeSet<NodeId>,
        to_add: &[NodeId],
        groups: &Arc<RwLock<HashMap<u64, Arc<OpenRaftNode>>>>,
        cluster_meta: &crate::cluster::meta_types::ClusterMeta,
    ) -> Result<()> {
        // Step 1: 添加缺失的副本为 Learner.
        let node = {
            let groups_read = groups.read();
            groups_read
                .get(&group_id)
                .cloned()
                .ok_or_else(|| ClusterError::Raft(format!("group {group_id} not found locally")))?
        };

        for node_id in to_add {
            // Raft gRPC 走 MetaRaft RPC 端口 (共享 dispatcher), 不能用 client_addr.
            let addr = cluster_meta
                .nodes
                .get(node_id)
                .map(|n| n.rpc_addr.clone())
                .ok_or_else(|| {
                    ClusterError::InvalidConfig(format!("node {node_id} rpc addr not found"))
                })?;
            node.add_learner_nonblocking(*node_id, addr).await?;
        }

        // Step 2: Joint Consensus 成员变更 (全量替换为期望成员集合).
        // Openraft 的 joint consensus 保证不会出现无 Voter 的中间状态.
        let new_members: Vec<NodeId> = expected.iter().copied().collect();
        node.change_membership(new_members).await?;

        Ok(())
    }

    /// 获取指定 group 的当前 Raft 成员集合 (从 OpenRaftNode 获取).
    #[allow(dead_code)]
    pub(crate) async fn get_group_raft_members(&self, group_id: u64) -> Option<BTreeSet<NodeId>> {
        let node = self.groups.read().get(&group_id).cloned();
        match node {
            Some(n) => Some(n.get_members().await),
            None => None,
        }
    }
}
