//! MetaRaft 控制面节点 — 强制 `group_id = 0` 的 OpenRaft 节点, 挂载
//! `MetaStateMachine` 作为状态机, 负责集群元数据 (节点 / Group / SlotTable /
//! 迁移状态) 的共识写入与查询.
//!
//! # 数据流
//!
//! ```text
//! propose(MetaRequest)
//!   ├─ 本地 validate_meta_request (仅优化, 失败仅 warn)
//!   └─ OpenRaftNode::propose(Request::Meta) -> Raft 共识 -> MetaStateMachine apply
//!        └─ 成功后读 get_cluster_meta / get_slot_table / get_migration_state
//!
//! initialize / initialize_with_client
//!   ├─ 已初始化则 no-op (幂等)
//!   ├─ inner.initialize (Raft membership)
//!   └─ 逐节点 RegisterNode + ChangeNodeRole(Voter) — bootstrap 节点即 Voter
//! ```
//!
//! 运维入口: `add_learner` / `promote_learner_to_voter` (经 `change_membership`
//! 双屏障, 见 `node.rs`); `start_server_with_dispatcher` 把 MetaRaft 注册进共享
//! dispatcher, 使 MetaRaft 端口也能路由数据 Group RPC.
//!
//! # Invariant
//!
//! - 强制 `group_id = METARAFT_GROUP_ID (0)`; 数据 Group ≥ 1.
//! - 本地校验只是提前失败优化, leader 在 apply 时做权威校验 (非 leader 本地
//!   状态可能滞后).
//! - bootstrap 节点在 ClusterMeta 中标记为 Voter, 与 Raft 层 membership 一致.
//! - 复用 OpenRaftNode: `use_wal = true` 同样强制.

use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;

use crate::cluster::meta_state_machine::MetaStateMachine;
use crate::cluster::meta_types::{MetaRequest, METARAFT_GROUP_ID};
use crate::cluster::network::RaftNetworkClientFactory;
use crate::cluster::node::OpenRaftNode;
use crate::cluster::storage::OpenRaftStorage;
use crate::cluster::types::{ClusterError, NodeId, RaftNodeConfig, Request, Response};
use crate::error::{Error, Result};
use crate::DB;

/// 屏障超时常量 (仅 #[cfg(test)] 下 MetaRaft 屏障单测使用; 生产路径见 `OpenRaftNode`).
#[cfg(test)]
const CATCH_UP_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(test)]
const CATCH_UP_POLL: Duration = Duration::from_millis(50);
#[cfg(test)]
const CATCH_UP_THRESHOLD: u64 = 5;
#[cfg(test)]
const REPLICATION_CONFIRM_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(test)]
const REPLICATION_POLL: Duration = Duration::from_millis(50);
#[cfg(test)]
const REPLICATION_HEARTBEAT_MULTIPLIER: u32 = 3;

pub struct MetaRaftNode {
    inner: Arc<OpenRaftNode>,
    state_machine: Arc<MetaStateMachine>,
    #[cfg_attr(not(test), allow(dead_code))]
    heartbeat_interval_ms: u64,
}

impl MetaRaftNode {
    pub async fn new(
        mut config: RaftNodeConfig,
        db: Arc<DB>,
        network_factory: RaftNetworkClientFactory,
    ) -> Result<Self> {
        config.group_id = METARAFT_GROUP_ID;
        let heartbeat_interval_ms = config.heartbeat_interval;
        config.validate()?;

        if !db.use_wal() {
            return Err(Error::Cluster(ClusterError::InvalidConfig(
                "raft mode requires use_wal=true".into(),
            )));
        }

        let state_machine = Arc::new(MetaStateMachine::new(db.clone())?);
        let storage = OpenRaftStorage::new(
            db.clone(),
            METARAFT_GROUP_ID,
            Some(Arc::clone(&state_machine)),
        )?;
        let inner =
            Arc::new(OpenRaftNode::new_with_storage(config, db, storage, network_factory).await?);

        Ok(Self {
            inner,
            state_machine,
            heartbeat_interval_ms,
        })
    }

    pub fn node_id(&self) -> NodeId {
        self.inner.node_id()
    }

    /// Register a node's gRPC address in the network client factory.
    pub fn add_node_address(&self, node_id: NodeId, addr: String) {
        self.inner.add_node_address(node_id, addr);
    }

    pub async fn initialize(&self, nodes: Vec<(NodeId, String)>) -> Result<()> {
        if self
            .inner
            .raft()
            .is_initialized()
            .await
            .map_err(|e| Error::Cluster(ClusterError::Raft(e.to_string())))?
        {
            return Ok(());
        }
        self.inner.initialize(nodes.clone()).await?;

        // Register bootstrap nodes in the MetaStateMachine so CLUSTER NODES
        // and other cluster commands can discover them immediately.
        // This is safe because we are the only voter — propose goes through
        // Raft consensus which will succeed with a single-node quorum.
        for (node_id, rpc_addr) in &nodes {
            self.propose(MetaRequest::RegisterNode {
                node_id: *node_id,
                rpc_addr: rpc_addr.clone(),
                client_addr: None, // 不设置 client_addr, 由调用方通过后续 MEET 提供
                tags: std::collections::HashMap::new(),
            })
            .await?;
            // Bootstrap nodes are Voters from the start — they form the initial
            // Raft membership.  Marking them as Voter in ClusterMeta keeps the
            // persisted metadata consistent with the Raft layer.
            self.propose(MetaRequest::ChangeNodeRole {
                node_id: *node_id,
                role: crate::cluster::meta_types::NodeRole::Voter,
            })
            .await?;
        }
        Ok(())
    }

    /// Bootstrap 注册时同时设置 client_addr (用于正确的 MOVED 重定向端口).
    pub async fn initialize_with_client(
        &self,
        nodes: Vec<(NodeId, String, Option<String>)>,
    ) -> Result<()> {
        if self
            .inner
            .raft()
            .is_initialized()
            .await
            .map_err(|e| Error::Cluster(ClusterError::Raft(e.to_string())))?
        {
            return Ok(());
        }
        let rpc_only: Vec<(NodeId, String)> = nodes
            .iter()
            .map(|(id, addr, _)| (*id, addr.clone()))
            .collect();
        self.inner.initialize(rpc_only).await?;

        for (node_id, rpc_addr, client_addr) in &nodes {
            self.propose(MetaRequest::RegisterNode {
                node_id: *node_id,
                rpc_addr: rpc_addr.clone(),
                client_addr: client_addr.clone(),
                tags: std::collections::HashMap::new(),
            })
            .await?;
            self.propose(MetaRequest::ChangeNodeRole {
                node_id: *node_id,
                role: crate::cluster::meta_types::NodeRole::Voter,
            })
            .await?;
        }
        Ok(())
    }

    pub async fn propose(&self, request: MetaRequest) -> Result<Response> {
        // Validate locally as an early optimization (catches obvious errors
        // before Raft round-trip).  For non-leader nodes the local state may
        // lag behind committed state — the leader will perform authoritative
        // validation when applying the entry.
        if let Err(e) = self.state_machine.validate_meta_request(&request) {
            tracing::warn!(error = %e, "local validation failed (will be validated by leader)");
        }
        self.inner.propose(Request::Meta(request)).await
    }

    pub fn get_cluster_meta(&self) -> crate::cluster::meta_types::ClusterMeta {
        self.state_machine.get_cluster_meta()
    }

    pub fn get_slot_table(&self) -> crate::cluster::meta_types::SlotTable {
        self.state_machine.get_slot_table()
    }

    /// Directly set slot table (for testing).
    pub fn set_slot_table(&self, table: crate::cluster::meta_types::SlotTable) {
        self.state_machine.set_slot_table(table);
    }

    pub fn get_migration_state(&self) -> Option<crate::cluster::meta_types::SlotMigrationState> {
        self.state_machine.get_migration_state()
    }

    /// FIX-0056-A1: 当前活跃迁移的 oplog epoch; 无活跃迁移时为 `None`.
    pub fn get_migration_epoch(&self) -> Option<u64> {
        self.state_machine.get_migration_epoch()
    }

    /// Set migration state directly (for testing).
    /// Skips Raft consensus — only use in test scenarios.
    pub fn set_migration_state(
        &self,
        state: Option<crate::cluster::meta_types::SlotMigrationState>,
    ) {
        self.state_machine.set_migration_state(state);
    }

    pub async fn is_leader(&self) -> bool {
        self.inner.is_leader().await
    }

    pub async fn get_leader(&self) -> Option<NodeId> {
        self.inner.get_leader().await
    }

    pub async fn add_learner(&self, node_id: NodeId, address: String) -> Result<()> {
        self.inner.add_learner(node_id, address).await
    }

    /// Add a learner without blocking for replication catch-up.
    pub async fn add_learner_nonblocking(&self, node_id: NodeId, address: String) -> Result<()> {
        self.inner.add_learner_nonblocking(node_id, address).await
    }

    /// 等待 learner 追平 leader 日志 (屏障 1).
    ///
    /// 每 50ms 检查一次 replication metrics, 允许落后 5 条以内.
    /// 超过 30s 返回 `ClusterError::Timeout`.
    #[cfg(test)]
    async fn wait_learner_catch_up(&self, node_id: NodeId) -> Result<()> {
        let start = tokio::time::Instant::now();
        let deadline = start + CATCH_UP_TIMEOUT;
        loop {
            let metrics = self.inner.metrics().await;
            let leader_last = metrics.last_log_index.unwrap_or(0);
            let learner_matched = metrics
                .replication
                .as_ref()
                .and_then(|r| r.get(&node_id))
                .and_then(|v| v.as_ref())
                .map(|log_id| log_id.index)
                .unwrap_or(0);

            tracing::debug!(
                node_id,
                leader_last,
                learner_matched,
                behind = leader_last.saturating_sub(learner_matched),
                "waiting for learner to catch up"
            );

            if leader_last.saturating_sub(learner_matched) <= CATCH_UP_THRESHOLD
                && Self::is_learner_connected(&metrics, node_id)
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(
                    node_id,
                    leader_last,
                    learner_matched,
                    elapsed_ms = start.elapsed().as_millis(),
                    "learner catch-up timed out"
                );
                return Err(Error::Cluster(ClusterError::Timeout(format!(
                    "learner {node_id} failed to catch up within 30s"
                ))));
            }
            tokio::time::sleep(CATCH_UP_POLL).await;
        }
    }

    /// 确认 membership change entry 已传播到至少一个其他 voter (屏障 2).
    ///
    /// Step 1: 等待至少一个其他 voter 的 `matched >= last_log_index`.
    /// Step 2: sleep `3 * heartbeat_interval` 让 commit_index 传播.
    #[cfg(test)]
    async fn confirm_replication(&self, voter_ids: &[NodeId]) -> Result<()> {
        // 快速路径: 只有本节点一个 voter, entry 已本地 committed
        if voter_ids.len() <= 1 {
            tracing::debug!("single voter, replication confirmation skipped");
            return Ok(());
        }

        // Step 1: 确认至少一个其他 voter 收到 entry
        let last_log = self.inner.metrics().await.last_log_index.unwrap_or(0);
        let deadline = tokio::time::Instant::now() + REPLICATION_CONFIRM_TIMEOUT;

        loop {
            let metrics = self.inner.metrics().await;
            let confirmed_voter = voter_ids.iter().find(|id| {
                **id != self.node_id()
                    && metrics
                        .replication
                        .as_ref()
                        .and_then(|r| r.get(id))
                        .and_then(|v| v.as_ref())
                        .map(|log_id| log_id.index >= last_log)
                        .unwrap_or(false)
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

    /// Promote the given node from Learner to Voter in the MetaRaft group.
    ///
    /// Reads the current voter set from ClusterMeta and builds a new voter
    /// list that includes the target node.  Delegates to
    /// `OpenRaftNode::change_membership` which includes replication barriers
    /// (member catch-up + replication confirmation).
    ///
    /// After the membership change commits, updates ClusterMeta so subsequent
    /// promotions see the correct voter set.
    pub async fn promote_learner_to_voter(&self, node_id: NodeId) -> Result<()> {
        let meta = self.get_cluster_meta();
        let mut voter_ids: Vec<NodeId> = meta
            .nodes
            .iter()
            .filter(|(_, info)| matches!(info.role, crate::cluster::meta_types::NodeRole::Voter))
            .map(|(id, _)| *id)
            .collect();
        // Safety net: if no voters are recorded in ClusterMeta (possible with stale
        // persisted data from an earlier release), include the local bootstrap node
        // which is always the initial voter.
        if voter_ids.is_empty() {
            voter_ids.push(self.node_id());
        }
        if !voter_ids.contains(&node_id) {
            voter_ids.push(node_id);
        }
        // change_membership 内含复制屏障 (wait_members_catch_up + confirm_replication)
        self.inner.change_membership(voter_ids).await?;

        // Update ClusterMeta so the promoted node is recorded as Voter.
        // This keeps the persisted metadata consistent with the Raft layer
        // so that subsequent promotions see the correct voter set.
        self.propose(MetaRequest::ChangeNodeRole {
            node_id,
            role: crate::cluster::meta_types::NodeRole::Voter,
        })
        .await?;
        Ok(())
    }

    /// 检查 learner 是否已出现在 replication metrics 中.
    #[cfg(test)]
    fn is_learner_connected(
        metrics: &openraft::RaftMetrics<crate::cluster::types::TypeConfig>,
        node_id: NodeId,
    ) -> bool {
        metrics
            .replication
            .as_ref()
            .map(|r| r.contains_key(&node_id))
            .unwrap_or(false)
    }

    pub async fn change_membership(&self, members: Vec<NodeId>) -> Result<()> {
        self.inner.change_membership(members).await
    }

    pub async fn start_server(
        &self,
        addr: std::net::SocketAddr,
        max_message_size: u64,
    ) -> Result<()> {
        self.inner.start_server(addr, max_message_size).await
    }

    /// 启动 MetaRaft gRPC server, 使用外部共享的 dispatcher.
    ///
    /// 共享 dispatcher 使 MetaRaft 端口 (如 16379) 也能路由
    /// MultiRaft 数据 group 的 Raft 消息, 解决 `add_learner_to_group`
    /// 等操作使用 MetaRaft RPC 地址导致的端口错位问题.
    pub async fn start_server_with_dispatcher(
        &self,
        addr: std::net::SocketAddr,
        max_message_size: u64,
        dispatcher: std::sync::Arc<crate::cluster::network::RaftServiceDispatcher>,
    ) -> Result<()> {
        // 注册 MetaRaft 的 OpenRaftNode 句柄, 使 `remote_propose` 等 gRPC
        // 服务端能路由到控制面 (group 0). 跨节点 propose (例如 failover 后
        // 新 leader 向 MetaRaft leader 转发 is_leader 更新) 依赖此注册.
        // 注意: 必须在 `serve` 之前注册, 因为 `serve_with_incoming` 是阻塞的.
        dispatcher.register_node(METARAFT_GROUP_ID, self.inner.clone());
        self.inner
            .start_server_with_dispatcher(addr, max_message_size, dispatcher)
            .await
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.inner.shutdown().await
    }

    pub fn inner(&self) -> &OpenRaftNode {
        &self.inner
    }

    pub fn state_machine(&self) -> &Arc<MetaStateMachine> {
        &self.state_machine
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::types::RaftNodeConfig;
    use crate::cluster::SlotStatus;
    use crate::config::Options;
    use std::collections::HashMap;
    use std::time::Duration;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_single_node_propose_register() {
        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let factory = RaftNetworkClientFactory::new(
            1,
            METARAFT_GROUP_ID,
            RaftNodeConfig::default().rpc_timeout_ms,
            RaftNodeConfig::default().grpc_max_message_size,
        );
        let cfg = RaftNodeConfig {
            node_id: 1,
            group_id: METARAFT_GROUP_ID,
            election_timeout_min: 500,
            election_timeout_max: 1000,
            heartbeat_interval: 50,
            ..Default::default()
        };
        let node = MetaRaftNode::new(cfg, db, factory).await.unwrap();
        node.initialize(vec![(1, "http://127.0.0.1:1".into())])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(800)).await;
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            node.propose(MetaRequest::RegisterNode {
                node_id: 10,
                rpc_addr: "http://127.0.0.1:9010".into(),
                client_addr: None,
                tags: HashMap::new(),
            }),
        )
        .await
        .expect("propose timed out")
        .unwrap();
        assert!(matches!(result, Response::Ok));
        assert!(node.get_cluster_meta().nodes.contains_key(&10));
    }

    /// 屏障 1 超时: learner 不在 replication metrics 中 → 30s 后 Timeout.
    #[tokio::test]
    async fn test_learner_catch_up_timeout() {
        tokio::time::pause();

        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let factory = RaftNetworkClientFactory::new(
            1,
            METARAFT_GROUP_ID,
            RaftNodeConfig::default().rpc_timeout_ms,
            RaftNodeConfig::default().grpc_max_message_size,
        );
        let cfg = RaftNodeConfig {
            node_id: 1,
            group_id: METARAFT_GROUP_ID,
            election_timeout_min: 500,
            election_timeout_max: 1000,
            heartbeat_interval: 50,
            ..Default::default()
        };
        let node = MetaRaftNode::new(cfg, db, factory).await.unwrap();
        node.initialize(vec![(1, "http://127.0.0.1:1".into())])
            .await
            .unwrap();

        // Propose enough entries so leader_last > CATCH_UP_THRESHOLD (5)
        for i in 0..10 {
            node.propose(MetaRequest::RegisterNode {
                node_id: 100 + i,
                rpc_addr: format!("http://127.0.0.1:{}", 100 + i),
                client_addr: None,
                tags: HashMap::new(),
            })
            .await
            .unwrap();
        }
        tokio::time::advance(Duration::from_millis(500)).await;

        // node 999 不在 group 中, metrics.replication 不会有它
        // → learner_matched = 0, leader_last > 5
        // → 循环等待直到 30s 超时

        // 用 select! 并行推进时间, 使 catch_up 内部的 loop 能够运行
        tokio::select! {
          r = node.wait_learner_catch_up(999) => {
            assert!(r.is_err(), "expected Timeout error");
            let err_msg = format!("{}", r.unwrap_err());
            assert!(err_msg.contains("failed to catch up"), "unexpected: {err_msg}");
          }
          _ = async {
            // 逐步推进时间, 让 poll loop 能迭代到 30s deadline
            for _ in 0..35 {
              tokio::time::advance(Duration::from_secs(1)).await;
            }
          } => {
            panic!("time advancement completed before catch_up returned");
          }
        }
    }

    /// 屏障 2 超时: 虚假 voter 不在 replication metrics 中 → 5s 后 Timeout.
    #[tokio::test]
    async fn test_replication_confirm_timeout() {
        tokio::time::pause();

        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let factory = RaftNetworkClientFactory::new(
            1,
            METARAFT_GROUP_ID,
            RaftNodeConfig::default().rpc_timeout_ms,
            RaftNodeConfig::default().grpc_max_message_size,
        );
        let cfg = RaftNodeConfig {
            node_id: 1,
            group_id: METARAFT_GROUP_ID,
            election_timeout_min: 500,
            election_timeout_max: 1000,
            heartbeat_interval: 50,
            ..Default::default()
        };
        let node = MetaRaftNode::new(cfg, db, factory).await.unwrap();
        node.initialize(vec![(1, "http://127.0.0.1:1".into())])
            .await
            .unwrap();

        // Propose enough entries so last_log_index > 0 for confirm check
        for i in 0..5 {
            node.propose(MetaRequest::RegisterNode {
                node_id: 100 + i,
                rpc_addr: format!("http://127.0.0.1:{}", 100 + i),
                client_addr: None,
                tags: HashMap::new(),
            })
            .await
            .unwrap();
        }
        tokio::time::advance(Duration::from_millis(500)).await;

        // voter_ids 含虚假节点 999, 但 999 不在 replication 中
        // → 永远不会确认 → 5s 后 Timeout
        tokio::select! {
          r = node.confirm_replication(&[1, 999]) => {
            assert!(r.is_err(), "expected Timeout error");
            let err_msg = format!("{}", r.unwrap_err());
            assert!(err_msg.contains("no voter confirmed"), "unexpected: {err_msg}");
          }
          _ = async {
            for _ in 0..10 {
              tokio::time::advance(Duration::from_secs(1)).await;
            }
          } => {
            panic!("time advancement completed before confirm_replication returned");
          }
        }
    }

    /// 快速路径: self 作为 learner (immediate catch_up) + 单个 voter (skip confirm).
    #[tokio::test]
    async fn test_barrier_fast_path() {
        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let factory = RaftNetworkClientFactory::new(
            1,
            METARAFT_GROUP_ID,
            RaftNodeConfig::default().rpc_timeout_ms,
            RaftNodeConfig::default().grpc_max_message_size,
        );
        let cfg = RaftNodeConfig {
            node_id: 1,
            group_id: METARAFT_GROUP_ID,
            election_timeout_min: 500,
            election_timeout_max: 1000,
            heartbeat_interval: 50,
            ..Default::default()
        };
        let node = MetaRaftNode::new(cfg, db, factory).await.unwrap();
        node.initialize(vec![(1, "http://127.0.0.1:1".into())])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(800)).await;

        // 屏障 1: wait_learner_catch_up(1) — self 的 matched >= last_log, 立即返回
        let start = std::time::Instant::now();
        node.wait_learner_catch_up(1).await.unwrap();
        let elapsed1 = start.elapsed();

        // 屏障 2: confirm_replication(&[1]) — len <= 1 → 快速路径
        let start2 = std::time::Instant::now();
        node.confirm_replication(&[1]).await.unwrap();
        let elapsed2 = start2.elapsed();

        assert!(
            elapsed1 < Duration::from_millis(500),
            "catch_up took {elapsed1:?}, expected <500ms"
        );
        assert!(
            elapsed2 < Duration::from_millis(100),
            "confirm took {elapsed2:?}, expected <100ms"
        );
    }

    /// 并发验证: Slot 迁移 + Raft 成员变更并发场景 (F-055).
    /// 两个 MetaRequest 通过 tokio::join! 并发 propose, MetaRaft 串行 apply.
    #[tokio::test]
    async fn test_concurrent_register_assign_slots() {
        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let factory = RaftNetworkClientFactory::new(
            1,
            METARAFT_GROUP_ID,
            RaftNodeConfig::default().rpc_timeout_ms,
            RaftNodeConfig::default().grpc_max_message_size,
        );
        let cfg = RaftNodeConfig {
            node_id: 1,
            group_id: METARAFT_GROUP_ID,
            election_timeout_min: 500,
            election_timeout_max: 1000,
            heartbeat_interval: 50,
            ..Default::default()
        };
        let node = MetaRaftNode::new(cfg, db, factory).await.unwrap();
        node.initialize(vec![(1, "http://127.0.0.1:1".into())])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(800)).await;

        // 先创建 group 1, AssignSlots 需要 group 存在
        node.propose(MetaRequest::CreateGroup {
            group_id: 1,
            initial_replicas: vec![(1, true)],
        })
        .await
        .unwrap();

        // 并发 propose RegisterNode + AssignSlots
        let (res_a, res_b) = tokio::join!(
            tokio::time::timeout(
                Duration::from_secs(10),
                node.propose(MetaRequest::RegisterNode {
                    node_id: 10,
                    rpc_addr: "http://127.0.0.1:9010".into(),
                    client_addr: None,
                    tags: HashMap::new(),
                })
            ),
            tokio::time::timeout(
                Duration::from_secs(10),
                node.propose(MetaRequest::AssignSlots {
                    group_id: 1,
                    slots: vec![0],
                })
            ),
        );

        assert!(
            matches!(res_a, Ok(Ok(Response::Ok))),
            "RegisterNode failed: {res_a:?}"
        );
        assert!(
            matches!(res_b, Ok(Ok(Response::Ok))),
            "AssignSlots failed: {res_b:?}"
        );

        // 最终状态一致: 节点存在且 slot 已分配
        let meta = node.get_cluster_meta();
        assert!(meta.nodes.contains_key(&10), "node 10 not found");
        let slot_table = node.get_slot_table();
        assert!(
            matches!(slot_table[0], SlotStatus::Assigned(1)),
            "slot 0 not assigned to group 1: {:?}",
            slot_table[0]
        );
    }

    /// 并发验证: CreateGroup + RegisterNode (F-055).
    #[tokio::test]
    async fn test_concurrent_create_group_register_node() {
        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let factory = RaftNetworkClientFactory::new(
            1,
            METARAFT_GROUP_ID,
            RaftNodeConfig::default().rpc_timeout_ms,
            RaftNodeConfig::default().grpc_max_message_size,
        );
        let cfg = RaftNodeConfig {
            node_id: 1,
            group_id: METARAFT_GROUP_ID,
            election_timeout_min: 500,
            election_timeout_max: 1000,
            heartbeat_interval: 50,
            ..Default::default()
        };
        let node = MetaRaftNode::new(cfg, db, factory).await.unwrap();
        node.initialize(vec![(1, "http://127.0.0.1:1".into())])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(800)).await;

        // 并发 propose CreateGroup + RegisterNode
        let (res_a, res_b) = tokio::join!(
            tokio::time::timeout(
                Duration::from_secs(10),
                node.propose(MetaRequest::CreateGroup {
                    group_id: 2,
                    initial_replicas: vec![(1, true)],
                })
            ),
            tokio::time::timeout(
                Duration::from_secs(10),
                node.propose(MetaRequest::RegisterNode {
                    node_id: 20,
                    rpc_addr: "http://127.0.0.1:9020".into(),
                    client_addr: None,
                    tags: HashMap::new(),
                })
            ),
        );

        assert!(
            matches!(res_a, Ok(Ok(Response::Ok))),
            "CreateGroup failed: {res_a:?}"
        );
        assert!(
            matches!(res_b, Ok(Ok(Response::Ok))),
            "RegisterNode failed: {res_b:?}"
        );

        // 最终状态一致: group 和 node 共存
        let meta = node.get_cluster_meta();
        assert!(meta.groups.contains_key(&2), "group 2 not found");
        assert!(meta.nodes.contains_key(&20), "node 20 not found");
    }
}
