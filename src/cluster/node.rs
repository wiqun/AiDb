//! 通用 OpenRaft 节点封装 — 把 `openraft::Raft` 与 `OpenRaftStorage` 组合成
//! 单 Group 的读写入口. MetaRaft 控制面与数据 Group 都复用本类型
//! (`meta_raft_node.rs` 是它的 gid=0 特化).
//!
//! # 数据流
//!
//! ```text
//! propose(request)
//!   ├─ check_entry_size (超限 InvalidConfig)
//!   └─ raft.client_write (重试 ≤3)
//!        ├─ Ok -> 返回 Response.data; track_proposed_bytes (F-008 size snapshot)
//!        └─ ForwardToLeader -> 注册 leader 地址 -> gRPC remote_propose 转发到 leader
//!             └─ 仍失败 -> ClusterError::NotLeader (is_ask=false)
//!
//! change_membership(members)
//!   ├─ 屏障 1: wait_members_catch_up — 所有成员已连接且 matched 落后 ≤5 (30s 超时)
//!   ├─ raft.change_membership (joint consensus)
//!   └─ 屏障 2: confirm_replication — 至少一个其他 voter matched ≥ last_log,
//!        再等 3×heartbeat 让 commit_index 传播
//! ```
//!
//! 读路径 `get` / `get_migration_tip` / `get_migration_tombstone` 共用
//! `ensure_leader_for_linear_read`: `linearizable_read=true` 时走 LeaseRead
//! (lease 内本地读零 RTT, 失去多数派且无法续租时快速失败返回
//! `ForwardToLeader::empty()` → `NotLeader`), 否则本地 leader check
//! (FIX-0056-A1 合并读线性点依赖此语义).
//!
//! # Invariant
//!
//! - 成员变更双屏障: catch-up + replication confirm — 防 leader 在
//!   change_membership 提交后、commit_index 传播前崩溃导致的新成员缺日志.
//! - Raft 模式必须 `db.use_wal() == true`, 否则 `ClusterError::InvalidConfig`.
//! - ForwardToLeader 的转发地址来自 wire 上的 leader addr, 必须注册进 network
//!   factory 才能触达.
//! - `initialize` 仅用于首节点单 voter bootstrap; 已有 Raft 状态时 openraft 拒绝.

use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{ForwardToLeader, RaftError};
use openraft::type_config::async_runtime::WatchReceiver;
use openraft::{Config, Raft, RaftMetrics, RaftNetworkFactory, TryAsRef};
use tracing::instrument;

use crate::cluster::log_committer::spawn_committer;
use crate::cluster::network::{RaftNetworkClientFactory, RaftServiceDispatcher, RaftServiceImpl};
use crate::cluster::storage::OpenRaftStorage;
use crate::cluster::types::{
    ClusterError, NodeId, RaftNodeConfig, Request, Response, ThinWriteBatch, TypeConfig,
};
use crate::error::{Error, Result};
use crate::DB;

/// 复制屏障超时常量.
const CATCH_UP_TIMEOUT: Duration = Duration::from_secs(30);
const CATCH_UP_POLL: Duration = Duration::from_millis(50);
const CATCH_UP_THRESHOLD: u64 = 5;
const REPLICATION_CONFIRM_TIMEOUT: Duration = Duration::from_secs(5);
const REPLICATION_POLL: Duration = Duration::from_millis(50);
const REPLICATION_HEARTBEAT_MULTIPLIER: u32 = 3;

pub struct OpenRaftNode {
    node_id: NodeId,
    group_id: u64,
    raft: Arc<Raft<TypeConfig, OpenRaftStorage>>,
    storage: OpenRaftStorage,
    network_factory: Arc<RwLock<RaftNetworkClientFactory>>,
    max_entry_size: u64,
    heartbeat_interval_ms: u64,
    /// leader 上 propose 成功累积的写请求估算字节数 (从不重置, 用于 size-based snapshot).
    estimated_proposed_bytes_accumulated: AtomicU64,
    /// 达到此字节数后触发 snapshot (None = 禁用).
    snapshot_size_threshold: Option<u64>,
    /// 是否启用 linearizable read (ReadIndex).
    linearizable_read: bool,
}

impl OpenRaftNode {
    pub async fn new(
        config: RaftNodeConfig,
        db: Arc<DB>,
        network_factory: RaftNetworkClientFactory,
    ) -> Result<Self> {
        config.validate()?;

        if !db.use_wal() {
            return Err(Error::Cluster(ClusterError::InvalidConfig(
                "raft mode requires use_wal=true".into(),
            )));
        }

        // 可选: 创建 LogCommitter.
        let committer = config
            .log_committer_config
            .as_ref()
            .map(|cfg| spawn_committer(config.group_id, db.clone(), cfg.clone()));

        let storage =
            OpenRaftStorage::new_with_committer(db.clone(), config.group_id, None, committer)?;
        Self::new_with_storage(config, db, storage, network_factory).await
    }

    pub async fn new_with_storage(
        config: RaftNodeConfig,
        db: Arc<DB>,
        storage: OpenRaftStorage,
        network_factory: RaftNetworkClientFactory,
    ) -> Result<Self> {
        let heartbeat_interval_ms = config.heartbeat_interval;
        config.validate()?;

        if !db.use_wal() {
            return Err(Error::Cluster(ClusterError::InvalidConfig(
                "raft mode requires use_wal=true".into(),
            )));
        }

        let raft_config = Config {
            cluster_name: format!("aidb-raft-{}", config.group_id),
            election_timeout_min: config.election_timeout_min,
            election_timeout_max: config.election_timeout_max,
            heartbeat_interval: config.heartbeat_interval,
            max_payload_entries: config.max_payload_entries,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(
                config.snapshot_logs_since_last,
            ),
            ..Default::default()
        }
        .validate()
        .map_err(|e| Error::Cluster(ClusterError::InvalidConfig(e.to_string())))?;

        let network_factory_arc = Arc::new(RwLock::new(network_factory));
        let network_for_raft = network_factory_arc.read().clone();

        // v0.10: OpenRaftStorage implements both RaftLogStorage and RaftStateMachine
        let log_store = storage.clone();
        let state_machine = storage.clone();

        let raft = Raft::new(
            config.node_id,
            Arc::new(raft_config),
            network_for_raft,
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| Error::Cluster(ClusterError::Raft(e.to_string())))?;

        Ok(Self {
            node_id: config.node_id,
            group_id: config.group_id,
            raft: Arc::new(raft),
            storage,
            network_factory: network_factory_arc,
            max_entry_size: config.max_entry_size,
            heartbeat_interval_ms,
            estimated_proposed_bytes_accumulated: AtomicU64::new(0),
            snapshot_size_threshold: config.snapshot_size_threshold,
            linearizable_read: config.linearizable_read,
        })
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn group_id(&self) -> u64 {
        self.group_id
    }

    pub async fn initialize(&self, nodes: Vec<(NodeId, String)>) -> Result<()> {
        let mut members = std::collections::BTreeMap::new();
        for (node_id, addr) in nodes {
            members.insert(node_id, openraft::BasicNode { addr: addr.clone() });
            self.network_factory.write().add_node(node_id, addr);
        }
        self.raft
            .initialize(members)
            .await
            .map_err(|e| Error::Cluster(ClusterError::Raft(e.to_string())))?;
        Ok(())
    }

    pub async fn add_learner(&self, node_id: NodeId, address: String) -> Result<()> {
        self.add_learner_inner(node_id, address, true).await
    }

    /// Add a learner without blocking (non-blocking mode).
    ///
    /// Returns immediately after the membership change is committed —
    /// does NOT wait for the learner to catch up on log replication.
    pub async fn add_learner_nonblocking(&self, node_id: NodeId, address: String) -> Result<()> {
        self.add_learner_inner(node_id, address, false).await
    }

    async fn add_learner_inner(
        &self,
        node_id: NodeId,
        address: String,
        blocking: bool,
    ) -> Result<()> {
        tracing::debug!(
          node_id,
          address = %address,
          group_id = self.group_id,
          blocking,
          "add_learner: registering address and calling openraft",
        );
        self.network_factory
            .write()
            .add_node(node_id, address.clone());
        let result = self
            .raft
            .add_learner(node_id, openraft::BasicNode { addr: address }, blocking)
            .await;
        tracing::debug!(
            node_id,
            group_id = self.group_id,
            success = result.is_ok(),
            "add_learner: completed",
        );
        result.map_err(|e| Error::Cluster(ClusterError::Raft(e.to_string())))?;
        Ok(())
    }

    /// Change membership with replication barriers.
    ///
    /// Two barriers guard against the leader crashing between `change_membership`
    /// commit and follower `commit_index` propagation:
    /// 1. Wait for all new members to catch up on log replication
    /// 2. Confirm at least one other voter received the entry + heartbeat wait
    #[instrument(skip(self), fields(group_id = self.group_id, member_count = members.len()))]
    pub async fn change_membership(&self, members: Vec<NodeId>) -> Result<()> {
        let set: std::collections::BTreeSet<NodeId> = members.into_iter().collect();
        let total_start = std::time::Instant::now();

        // ── 屏障 1: 等所有成员追平 ──
        let t0 = std::time::Instant::now();
        self.wait_members_catch_up(&set).await?;
        tracing::info!(
            elapsed_ms = t0.elapsed().as_millis(),
            "change_membership: barrier 1 (catch_up) passed"
        );

        self.raft
            .change_membership(set.clone(), false)
            .await
            .map_err(|e| Error::Cluster(ClusterError::Raft(e.to_string())))?;

        // ── 屏障 2: 确认 entry 已传播 ──
        let t1 = std::time::Instant::now();
        let voter_ids: Vec<NodeId> = set.iter().copied().collect();
        self.confirm_replication(&voter_ids).await?;
        tracing::info!(
            elapsed_ms = t1.elapsed().as_millis(),
            "change_membership: barrier 2 (replication) passed"
        );

        tracing::info!(
            total_elapsed_ms = total_start.elapsed().as_millis(),
            "change_membership: complete"
        );
        Ok(())
    }

    /// 等待所有成员追平 leader 日志 (屏障 1).
    ///
    /// 每个成员必须同时满足:
    /// 1. 已出现在 replication metrics 中 (leader 已与其建立复制连接)
    /// 2. matched_log_index 与 leader last_log_index 差距不超过阈值
    async fn wait_members_catch_up(
        &self,
        members: &std::collections::BTreeSet<NodeId>,
    ) -> Result<()> {
        let start = tokio::time::Instant::now();
        let deadline = start + CATCH_UP_TIMEOUT;
        loop {
            let metrics = self.metrics().await;
            let leader_last = metrics.last_log_index.unwrap_or(0);
            let all_caught_up = members.iter().all(|id| {
                if *id == self.node_id {
                    return true; // 跳过自己
                }
                let matched = Self::matched_log_index(&metrics, *id);
                let connected = Self::is_connected(&metrics, *id);
                // 必须已连接且日志追平 (connected 保证 learner 已加入复制流,
                // 避免 last_log_index=0 时 barrier 立即通过)
                connected && leader_last.saturating_sub(matched) <= CATCH_UP_THRESHOLD
            });

            if all_caught_up {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(
                    ?members,
                    leader_last,
                    elapsed_ms = start.elapsed().as_millis(),
                    "members catch-up timed out"
                );
                return Err(Error::Cluster(ClusterError::Timeout(
                    "members failed to catch up within 30s".into(),
                )));
            }
            tokio::time::sleep(CATCH_UP_POLL).await;
        }
    }

    /// 确认 membership change entry 已传播到至少一个其他 voter (屏障 2).
    async fn confirm_replication(&self, voter_ids: &[NodeId]) -> Result<()> {
        // 快速路径: 只有本节点一个 voter, entry 已本地 committed
        if voter_ids.len() <= 1 {
            tracing::debug!("single voter, replication confirmation skipped");
            return Ok(());
        }

        // Step 1: 确认至少一个其他 voter 收到 entry
        let last_log = self.metrics().await.last_log_index.unwrap_or(0);
        let deadline = tokio::time::Instant::now() + REPLICATION_CONFIRM_TIMEOUT;

        loop {
            let metrics = self.metrics().await;
            let confirmed_voter = voter_ids.iter().find(|id| {
                **id != self.node_id() && Self::matched_log_index(&metrics, **id) >= last_log
            });
            if let Some(voter_id) = confirmed_voter {
                tracing::debug!(
                  confirmed_voter = %voter_id,
                  last_log_index = last_log,
                  "replication confirmed, waiting for commit propagation"
                );
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Cluster(ClusterError::Timeout(
                    "no voter confirmed replication within 5s".into(),
                )));
            }
            tokio::time::sleep(REPLICATION_POLL).await;
        }

        // Step 2: 等 3 个 heartbeat 让 commit_index 传播
        tokio::time::sleep(Duration::from_millis(
            REPLICATION_HEARTBEAT_MULTIPLIER as u64 * self.heartbeat_interval_ms,
        ))
        .await;
        Ok(())
    }

    /// 从 RaftMetrics 提取指定节点的 matched log index.
    fn matched_log_index(metrics: &RaftMetrics<TypeConfig>, node_id: NodeId) -> u64 {
        metrics
            .replication
            .as_ref()
            .and_then(|r| r.get(&node_id))
            .and_then(|v| v.as_ref())
            .map(|log_id| log_id.index)
            .unwrap_or(0)
    }

    /// 检查节点是否已出现在 replication metrics 中 (leader 已与其建立复制连接).
    fn is_connected(metrics: &RaftMetrics<TypeConfig>, node_id: NodeId) -> bool {
        metrics
            .replication
            .as_ref()
            .map(|r| r.contains_key(&node_id))
            .unwrap_or(false)
    }

    fn check_entry_size(&self, request: &Request) -> Result<()> {
        let size = request.estimated_serialized_size() as u64;
        if size > self.max_entry_size {
            return Err(Error::Cluster(ClusterError::InvalidConfig(format!(
                "entry size {size} exceeds max_entry_size {}",
                self.max_entry_size
            ))));
        }
        Ok(())
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn propose(&self, request: Request) -> Result<Response> {
        let t0 = std::time::Instant::now();
        self.check_entry_size(&request)?;
        let estimated_size = request.estimated_serialized_size() as u64;
        // Retry on ForwardToLeader — during leader re-election, the first
        // forward target may itself be in transition.  Up to 3 retries with
        // 200ms backoff.
        for attempt in 0u32..3 {
            let t1 = std::time::Instant::now();
            match self.raft.client_write(request.clone()).await {
                Ok(response) => {
                    let elapsed = t0.elapsed();
                    tracing::debug!(
                        target: "perf",
                        group_id = self.group_id,
                        total_us = elapsed.as_micros(),
                        client_write_us = t1.elapsed().as_micros(),
                        attempt,
                        "raft_propose_ok"
                    );
                    // size-based snapshot trigger (F-008)
                    self.track_proposed_bytes(estimated_size);
                    return Ok(response.data);
                }
                Err(e) => {
                    if let Some(ftl) = e.forward_to_leader() {
                        let leader_addr = ftl.leader_node.as_ref().map(|n| n.addr.clone());
                        // Register the leader address so the network factory can reach it.
                        if let (Some(leader_id), Some(ref addr)) = (ftl.leader_id, &leader_addr) {
                            self.network_factory
                                .write()
                                .add_node(leader_id, addr.clone());
                        }
                        // 本地不是 leader 时, 通过 gRPC remote_propose 把请求真正
                        // 转发到当前 leader 节点执行, 而不是只重试本地 client_write.
                        // 这使 MetaRaft 的 leader_watcher / 集群命令能在任意节点上
                        // 完成控制面更新, 数据面 follower 也获得同一兜底.
                        if let (Some(leader_id), Some(addr)) = (ftl.leader_id, leader_addr.clone())
                        {
                            match self
                                .forward_propose_to_leader(leader_id, &addr, &request)
                                .await
                            {
                                Ok(resp) => return Ok(resp),
                                Err(forward_err) => {
                                    tracing::warn!(
                                        group_id = self.group_id,
                                        leader_id,
                                        error = %forward_err,
                                        attempt,
                                        "forward propose to leader failed, will retry"
                                    );
                                    if attempt < 2 {
                                        tracing::warn!(
                                            group_id = self.group_id,
                                            attempt,
                                            "forward retry"
                                        );
                                        tokio::time::sleep(std::time::Duration::from_millis(200))
                                            .await;
                                        continue;
                                    }
                                    // 远端仍返回 ForwardToLeader Display 串时, 归一成 NotLeader.
                                    return Err(normalize_forward_like_error(
                                        forward_err,
                                        ftl.leader_id,
                                        leader_addr.clone(),
                                    ));
                                }
                            }
                        }
                        if attempt < 2 {
                            tracing::warn!(group_id = self.group_id, attempt, "forward retry");
                            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                            continue;
                        }
                        return Err(Error::Cluster(ClusterError::NotLeader {
                            leader: ftl.leader_id,
                            leader_addr,
                            is_ask: false,
                        }));
                    }
                    // 部分 openraft 错误 Display 含 "has to forward" 但 forward_to_leader()
                    // 抽不出结构体 — 仍按 NotLeader 处理, 避免 CLUSTER MEET 等直接致命失败.
                    let msg = e.to_string();
                    if msg.contains("has to forward request to") {
                        return Err(Error::Cluster(ClusterError::NotLeader {
                            leader: None,
                            leader_addr: None,
                            is_ask: false,
                        }));
                    }
                    return Err(Error::Cluster(ClusterError::Raft(msg)));
                }
            }
        }
        unreachable!()
    }

    /// 将 propose 请求经 gRPC `remote_propose` 转发到目标 leader 节点执行.
    ///
    /// 服务端 `remote_propose` 把请求路由到对应 group 的 `OpenRaftNode` 后
    /// 本地 propose; 若该节点也已不再是 leader, 它自己会继续转发/重试,
    /// 最终收敛到当前 leader. 客户端复用 network factory 的连接池.
    async fn forward_propose_to_leader(
        &self,
        leader_id: NodeId,
        addr: &str,
        request: &Request,
    ) -> Result<Response> {
        let mut factory = self
            .network_factory
            .read()
            .clone()
            .with_group_id(self.group_id);
        let mut client = factory
            .new_client(
                leader_id,
                &openraft::BasicNode {
                    addr: addr.to_string(),
                },
            )
            .await;
        client.remote_propose(self.group_id, request).await
    }

    /// 累加 propose 成功的字节预估数, 超阈值时异步触发 snapshot (F-008).
    fn track_proposed_bytes(&self, estimated_size: u64) {
        if estimated_size == 0 {
            return;
        }
        let old = self
            .estimated_proposed_bytes_accumulated
            .fetch_add(estimated_size, AtomicOrdering::Relaxed);
        let new = old + estimated_size;
        if let Some(t) = self.snapshot_size_threshold {
            if old / t != new / t {
                let raft = self.raft.clone();
                tracing::info!(
                    target: "snap",
                    group_id = self.group_id,
                    threshold_bytes = t,
                    proposed_bytes = new,
                    "size-based snapshot requested"
                );
                tokio::spawn(async move {
                    match raft.trigger().snapshot().await {
                        Ok(()) => {} // triggered or already in progress — both OK
                        Err(e) => tracing::warn!(
                            target: "snap",
                            threshold_bytes = t,
                            proposed_bytes = new,
                            error = %e,
                            "size-based snapshot failed, retry on next threshold crossing"
                        ),
                    }
                });
            }
        }
    }

    pub async fn write_batch(&self, batch: ThinWriteBatch) -> Result<()> {
        match self.propose(Request::WriteBatch(batch)).await? {
            Response::Ok => Ok(()),
            Response::Error(msg) => Err(Error::Cluster(ClusterError::Internal(msg))),
            other => Err(Error::Cluster(ClusterError::Internal(format!(
                "unexpected write_batch response: {other}"
            )))),
        }
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut batch = ThinWriteBatch::new();
        batch.put(key, value);
        self.write_batch(batch).await
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<()> {
        let mut batch = ThinWriteBatch::new();
        batch.delete(key);
        self.write_batch(batch).await
    }

    pub async fn is_leader(&self) -> bool {
        self.raft.metrics().borrow_watched().current_leader == Some(self.node_id)
    }

    pub async fn get_leader(&self) -> Option<NodeId> {
        self.raft.metrics().borrow_watched().current_leader
    }

    pub async fn metrics(&self) -> RaftMetrics<TypeConfig> {
        self.raft.metrics().borrow_watched().clone()
    }

    /// 获取当前 Raft group 的成员节点集合, 用于 LifecycleManager 对账.
    pub async fn get_members(&self) -> std::collections::BTreeSet<NodeId> {
        self.raft
            .metrics()
            .borrow_watched()
            .membership_config
            .nodes()
            .map(|(nid, _)| *nid)
            .collect()
    }

    pub fn raft(&self) -> Arc<Raft<TypeConfig, OpenRaftStorage>> {
        self.raft.clone()
    }

    /// 合并读线性点 (FIX-0056-A1 硬约束): 确保后续读发生在 leader 视角上, 或
    /// 通过 ReadIndex 等价路径确认线性. `get()` / `get_migration_tip()`
    /// 共用同一语义, 不允许落后 follower 冒充最新读.
    async fn ensure_leader_for_linear_read(&self) -> Result<()> {
        if self.linearizable_read {
            // Linearizable read: lease 有效期内本地读零 RTT, 过期 (失去多数派
            // 且无法续租) 时快速失败返回 ForwardToLeader::empty() → NotLeader.
            self.raft
                .ensure_linearizable(openraft::ReadPolicy::LeaseRead)
                .await
                .map_err(map_linearizable_error)?;
        } else {
            // 退化路径: 本地 leader check (保持 Redis Cluster 最终一致性)
            if !self.is_leader().await {
                let leader = self.get_leader().await;
                return Err(Error::Cluster(ClusterError::NotLeader {
                    leader,
                    leader_addr: None,
                    is_ask: false,
                }));
            }
        }
        Ok(())
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.ensure_leader_for_linear_read().await?;
        let storage = self.storage.clone();
        tokio::task::spawn_blocking(move || storage.get_state_machine_value(&key))
            .await
            .map_err(|e| Error::Cluster(ClusterError::Internal(e.to_string())))?
    }

    /// FIX-0056-A1: 读取本 group 在 `epoch` 下的迁移 oplog tip. 供
    /// `SlotMigrationManager::drain_oplog_tip_stable` 判断"tip 是否已稳定"
    /// (mark_ready 前置). tip 缺失视为 0.
    pub async fn get_migration_tip(&self, epoch: u64) -> Result<u64> {
        self.ensure_leader_for_linear_read().await?;
        let storage = self.storage.clone();
        tokio::task::spawn_blocking(move || storage.get_migration_tip(epoch))
            .await
            .map_err(|e| Error::Cluster(ClusterError::Internal(e.to_string())))?
    }

    /// FIX-0056-A1 合并读线性点第 1 步: 读取本 group 在 `epoch` 下 `key` 的
    /// 迁移 tombstone (Put/Del), 供 aikv 合并读判断 target miss 是"从未拷贝"
    /// 还是"已被客户端 Del" (`None` = 无 tombstone). 与 `get()` 共用同一
    /// leader / linearizable 语义.
    pub async fn get_migration_tombstone(
        &self,
        epoch: u64,
        key: Vec<u8>,
    ) -> Result<Option<crate::cluster::migration_oplog::MigOp>> {
        self.ensure_leader_for_linear_read().await?;
        let storage = self.storage.clone();
        tokio::task::spawn_blocking(move || storage.get_migration_tombstone(epoch, &key))
            .await
            .map_err(|e| Error::Cluster(ClusterError::Internal(e.to_string())))?
    }

    pub async fn start_server(
        &self,
        addr: std::net::SocketAddr,
        max_message_size: u64,
    ) -> Result<()> {
        let dispatcher = Arc::new(RaftServiceDispatcher::new());
        dispatcher.register_group(self.group_id, self.raft.clone());
        self.start_server_with_dispatcher(addr, max_message_size, dispatcher)
            .await
    }

    /// 启动 gRPC server, 使用外部共享的 dispatcher.
    ///
    /// 共享 dispatcher 允许同一个端口路由多个 group 的 Raft 消息.
    /// MetaRaft gRPC server 使用此方法共享 MultiRaft 的 dispatcher,
    /// 使得 `add_learner_to_group` 等操作即使发到 MetaRaft 端口也能正确路由.
    pub async fn start_server_with_dispatcher(
        &self,
        addr: std::net::SocketAddr,
        max_message_size: u64,
        dispatcher: Arc<RaftServiceDispatcher>,
    ) -> Result<()> {
        use raft_rpc::raft_service_server::RaftServiceServer;
        use tokio::net::TcpListener;

        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| Error::Cluster(ClusterError::Io(e)))?;
        // 注册本 group 到共享 dispatcher (如果尚未注册).
        dispatcher.register_group(self.group_id, self.raft.clone());
        let service = RaftServiceImpl::new(dispatcher);
        let server = RaftServiceServer::new(service)
            .max_decoding_message_size(max_message_size as usize)
            .max_encoding_message_size(max_message_size as usize);

        crate::cluster::network::raft_server()
            .add_service(server)
            .serve_with_incoming(crate::cluster::network::tcp_incoming(listener))
            .await
            .map_err(|e| Error::Cluster(ClusterError::Raft(e.to_string())))?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        // 先停 LogCommitter actor (确保其持有的 Arc<DB> 释放), 再停 raft.
        // 自愈重开路径依赖此顺序: 不先退出 committer, 底层 WAL LOCK 不会被
        // 释放, `create_group_inner` 重开会报 `Database already in use`.
        if let Some(ref committer) = self.storage.committer {
            committer.shutdown().await;
        }
        self.raft
            .shutdown()
            .await
            .map_err(|e| Error::Cluster(ClusterError::Raft(e.to_string())))?;
        Ok(())
    }

    pub fn storage(&self) -> &OpenRaftStorage {
        &self.storage
    }

    pub fn add_node_address(&self, node_id: NodeId, address: String) {
        self.network_factory.write().add_node(node_id, address);
    }
}

/// 转发失败时若仍是 ForwardToLeader 语义, 归一为 `NotLeader`.
fn normalize_forward_like_error(
    err: Error,
    leader: Option<NodeId>,
    leader_addr: Option<String>,
) -> Error {
    match err {
        Error::Cluster(ClusterError::NotLeader { .. }) => err,
        Error::Cluster(ClusterError::Raft(msg)) if msg.contains("has to forward request to") => {
            Error::Cluster(ClusterError::NotLeader {
                leader,
                leader_addr,
                is_ask: false,
            })
        }
        other => other,
    }
}

/// 将 OpenRaft `ensure_linearizable` 错误映射为 ClusterError.
/// `ForwardToLeader` → `NotLeader` (客户端可做 MOVED 重定向);
/// 其他错误 → `Internal`.
fn map_linearizable_error(
    e: RaftError<TypeConfig, openraft::error::LinearizableReadError<TypeConfig>>,
) -> Error {
    if let Some(leader_err) = e.try_as_ref() {
        match leader_err {
            ForwardToLeader {
                leader_id: Some(leader),
                leader_node: Some(node),
            } => {
                return Error::Cluster(ClusterError::NotLeader {
                    leader: Some(*leader),
                    leader_addr: Some(node.addr.clone()),
                    is_ask: false,
                });
            }
            ForwardToLeader {
                leader_id: Some(leader),
                ..
            } => {
                return Error::Cluster(ClusterError::NotLeader {
                    leader: Some(*leader),
                    leader_addr: None,
                    is_ask: false,
                });
            }
            // Lease 过期且无已知 leader 可转发 (分区隔离的旧 leader): openraft
            // 返回 ForwardToLeader::empty(). 映射为 NotLeader { leader: None },
            // 与 Redis CLUSTERDOWN 语义对齐 (客户端不再收到滞后值/内部错误).
            ForwardToLeader {
                leader_id: None, ..
            } => {
                return Error::Cluster(ClusterError::NotLeader {
                    leader: None,
                    leader_addr: None,
                    is_ask: false,
                });
            }
        }
    }
    Error::Cluster(ClusterError::Internal(format!(
        "linearizable read failed: {e}"
    )))
}

#[cfg(feature = "cluster")]
use crate::cluster::network::raft_rpc;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::storage::DEFAULT_GROUP_ID;
    use crate::config::Options;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_raft_node_creation() {
        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let factory = RaftNetworkClientFactory::new(1, DEFAULT_GROUP_ID, 30, 65 * 1024 * 1024);
        let config = RaftNodeConfig::default();
        assert!(OpenRaftNode::new(config, db, factory).await.is_ok());
    }

    #[tokio::test]
    async fn test_empty_cluster_proposal() {
        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let factory = RaftNetworkClientFactory::new(1, DEFAULT_GROUP_ID, 30, 65 * 1024 * 1024);
        let node = OpenRaftNode::new(RaftNodeConfig::default(), db, factory)
            .await
            .unwrap();
        let err = node.put(b"k".to_vec(), b"v".to_vec()).await.unwrap_err();
        // Empty cluster has no leader, so propose returns NotLeader error.
        assert!(matches!(
            err,
            Error::Cluster(ClusterError::NotLeader {
                leader: None,
                leader_addr: None,
                is_ask: false,
            })
        ));
    }

    #[tokio::test]
    async fn test_max_entry_size_rejection() {
        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let factory = RaftNetworkClientFactory::new(1, DEFAULT_GROUP_ID, 30, 65 * 1024 * 1024);
        let config = RaftNodeConfig {
            max_entry_size: 8,
            ..Default::default()
        };
        let node = OpenRaftNode::new(config, db, factory).await.unwrap();
        let err = node
            .put(b"longkey".to_vec(), b"longvalue".to_vec())
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            Error::Cluster(ClusterError::InvalidConfig(_))
        ));
    }

    #[tokio::test]
    async fn test_use_wal_required() {
        let dir = TempDir::new().unwrap();
        let mut opts = Options::for_testing();
        opts.use_wal = false;
        let db = DB::open(dir.path(), opts).unwrap();
        let factory = RaftNetworkClientFactory::new(1, DEFAULT_GROUP_ID, 30, 65 * 1024 * 1024);
        let result = OpenRaftNode::new(RaftNodeConfig::default(), db, factory).await;
        assert!(matches!(
            result,
            Err(Error::Cluster(ClusterError::InvalidConfig(_)))
        ));
    }
}
