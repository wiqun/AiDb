use super::raft_rpc::raft_service_server::RaftService;
use super::{
    raft_rpc, vote_from_wire, AppendEntriesRequest, AppendEntriesResponse, Arc, ClusterError,
    Error, HashMap, NodeId, OpenRaftNode, OpenRaftStorage, Request, RwLock, TonicRequest,
    TypeConfig, VoteRequest,
};
use crate::statistics::{RaftRpcDirection, RaftRpcType, Statistics};

/// 多 Group Raft gRPC 服务调度器
/// 所有数据 Group 共享此实例, 通过 group_id 分发 RPC
pub struct RaftServiceDispatcher {
    #[allow(clippy::type_complexity)]
    groups: Arc<RwLock<HashMap<u64, Arc<openraft::Raft<TypeConfig, OpenRaftStorage>>>>>,
    /// FIX-0056-A1: `GetKey`/`GetMigrationTip` 需要 `OpenRaftNode` 级别的
    /// leader-check / linearizable 读语义 (`get()`/`get_migration_tip()`),
    /// 而不仅是裸 `openraft::Raft`. 与 `groups` 分开维护是为了不影响现有
    /// Vote/AppendEntries/InstallSnapshot 调用点 (`register_group` 签名不变);
    /// 仅 `MultiRaftNode` 组装 group 时额外调用 `register_node`.
    nodes: Arc<RwLock<HashMap<u64, Arc<OpenRaftNode>>>>,
    stats: Option<Arc<Statistics>>,
}

impl RaftServiceDispatcher {
    pub fn new() -> Self {
        Self::new_with_stats(None)
    }

    pub fn new_with_stats(stats: Option<Arc<Statistics>>) -> Self {
        Self {
            groups: Arc::new(RwLock::new(HashMap::new())),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            stats,
        }
    }

    pub fn with_stats(mut self, stats: Arc<Statistics>) -> Self {
        self.stats = Some(stats);
        self
    }

    pub fn statistics(&self) -> Option<Arc<Statistics>> {
        self.stats.clone()
    }

    pub fn register_group(
        &self,
        group_id: u64,
        raft: Arc<openraft::Raft<TypeConfig, OpenRaftStorage>>,
    ) {
        self.groups.write().insert(group_id, raft);
    }

    /// 注册 `OpenRaftNode` 句柄, 供 `GetKey`/`GetMigrationTip` 服务端处理
    /// 复用其 leader-check / linearizable 读逻辑 (与 `register_group` 分开
    /// 调用, 通常紧跟其后).
    pub fn register_node(&self, group_id: u64, node: Arc<OpenRaftNode>) {
        self.nodes.write().insert(group_id, node);
    }

    pub fn unregister_group(&self, group_id: u64) {
        self.groups.write().remove(&group_id);
        self.nodes.write().remove(&group_id);
    }

    pub fn get_raft(
        &self,
        group_id: u64,
    ) -> Option<Arc<openraft::Raft<TypeConfig, OpenRaftStorage>>> {
        self.groups.read().get(&group_id).cloned()
    }

    pub fn get_node(&self, group_id: u64) -> Option<Arc<OpenRaftNode>> {
        self.nodes.read().get(&group_id).cloned()
    }

    pub fn group_count(&self) -> usize {
        self.groups.read().len()
    }
}

impl Default for RaftServiceDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RaftServiceImpl {
    dispatcher: Arc<RaftServiceDispatcher>,
}

impl RaftServiceImpl {
    pub fn new(dispatcher: Arc<RaftServiceDispatcher>) -> Self {
        Self { dispatcher }
    }
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    async fn vote(
        &self,
        request: TonicRequest<raft_rpc::VoteRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::VoteResponse>, tonic::Status> {
        if let Some(stats) = &self.dispatcher.stats {
            stats.raft_rpc[RaftRpcType::Vote as usize][RaftRpcDirection::Incoming as usize]
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let req = request.into_inner();
        let last_log_id = if req.last_log_index > 0 {
            Some(openraft::LogId::new(
                openraft::vote::leader_id_std::CommittedLeaderId::new(req.last_log_term),
                req.last_log_index,
            ))
        } else {
            None
        };

        let vote_req = VoteRequest {
            vote: vote_from_wire(req.vote_term, req.vote_node_id, req.vote_committed),
            last_log_id,
            leadership_transfer: false,
        };

        let raft = self
            .dispatcher
            .get_raft(req.group_id)
            .ok_or_else(|| tonic::Status::not_found(format!("group {} not found", req.group_id)))?;
        let vote_resp = raft
            .vote(vote_req)
            .await
            .map_err(|e| tonic::Status::internal(format!("Vote failed: {e}")))?;

        Ok(tonic::Response::new(raft_rpc::VoteResponse {
            vote_term: vote_resp.vote.leader_id.term,
            vote_node_id: vote_resp.vote.leader_id.voted_for,
            vote_committed: vote_resp.vote.committed,
            vote_granted: vote_resp.vote_granted,
            is_in_membership: true,
        }))
    }

    async fn append_entries(
        &self,
        request: TonicRequest<raft_rpc::AppendEntriesRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::AppendEntriesResponse>, tonic::Status> {
        let req = request.into_inner();
        crate::cluster::contact::note_leader_append(req.group_id);
        if let Some(stats) = &self.dispatcher.stats {
            stats.raft_rpc[RaftRpcType::AppendEntries as usize]
                [RaftRpcDirection::Incoming as usize]
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            stats.raft_log_entries.fetch_add(
                req.entries.len() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }

        tracing::debug!(
            group_id = req.group_id,
            entry_count = req.entries.len(),
            leader_commit = req.leader_commit_index,
            prev_log_index = req.prev_log_index,
            "gRPC: received AppendEntries",
        );

        let mut entries = Vec::new();
        for entry in req.entries {
            let payload: openraft::EntryPayload<Request, NodeId, openraft::BasicNode> =
                rmp_serde::from_slice(&entry.payload)
                    .map_err(|e| tonic::Status::internal(format!("deserialize entry: {e}")))?;
            entries.push(crate::cluster::types::LogEntry {
                log_id: openraft::LogId::new(
                    openraft::vote::leader_id_std::CommittedLeaderId::new(entry.log_term),
                    entry.log_index,
                ),
                payload,
            });
        }

        let prev_log_id = match (
            req.prev_log_index,
            req.prev_log_term,
            req.prev_log_leader_id,
        ) {
            (Some(index), Some(term), Some(_leader_id)) => Some(openraft::LogId::new(
                openraft::vote::leader_id_std::CommittedLeaderId::new(term),
                index,
            )),
            _ => None,
        };

        let leader_commit = match (
            req.leader_commit_index,
            req.leader_commit_term,
            req.leader_commit_leader_id,
        ) {
            (Some(index), Some(term), Some(_leader_id)) => Some(openraft::LogId::new(
                openraft::vote::leader_id_std::CommittedLeaderId::new(term),
                index,
            )),
            _ => None,
        };

        let append_req = AppendEntriesRequest {
            vote: vote_from_wire(req.vote_term, req.vote_node_id, req.vote_committed),
            prev_log_id,
            entries,
            leader_commit,
        };

        let raft = self
            .dispatcher
            .get_raft(req.group_id)
            .ok_or_else(|| tonic::Status::not_found(format!("group {} not found", req.group_id)))?;
        let append_resp = raft
            .append_entries(append_req)
            .await
            .map_err(|e| tonic::Status::internal(format!("AppendEntries failed: {e}")))?;

        let response = match append_resp {
            AppendEntriesResponse::Success | AppendEntriesResponse::PartialSuccess(_) => {
                raft_rpc::AppendEntriesResponse {
                    vote_term: 0,
                    vote_node_id: 0,
                    vote_committed: false,
                    success: true,
                    conflict_index: None,
                    conflict_term: None,
                }
            }
            AppendEntriesResponse::Conflict => raft_rpc::AppendEntriesResponse {
                vote_term: 0,
                vote_node_id: 0,
                vote_committed: false,
                success: false,
                conflict_index: Some(0),
                conflict_term: Some(0),
            },
            AppendEntriesResponse::HigherVote(vote) => raft_rpc::AppendEntriesResponse {
                vote_term: vote.leader_id.term,
                vote_node_id: vote.leader_id.voted_for,
                vote_committed: vote.committed,
                success: false,
                conflict_index: None,
                conflict_term: None,
            },
        };

        Ok(tonic::Response::new(response))
    }

    async fn install_snapshot(
        &self,
        request: TonicRequest<raft_rpc::InstallSnapshotRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::InstallSnapshotResponse>, tonic::Status>
    {
        if let Some(stats) = &self.dispatcher.stats {
            stats.raft_rpc[RaftRpcType::InstallSnapshot as usize]
                [RaftRpcDirection::Incoming as usize]
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let req = request.into_inner();
        let meta = req
            .meta
            .ok_or_else(|| tonic::Status::invalid_argument("missing snapshot meta"))?;

        let last_log_id = match (
            meta.last_log_index,
            meta.last_log_term,
            meta.last_log_leader_id,
        ) {
            (Some(index), Some(term), Some(_leader_id)) => Some(openraft::LogId::new(
                openraft::vote::leader_id_std::CommittedLeaderId::new(term),
                index,
            )),
            _ => None,
        };

        let last_membership: openraft::StoredMembership<
            openraft::vote::leader_id_std::CommittedLeaderId<u64>,
            NodeId,
            openraft::BasicNode,
        > = postcard::from_bytes(&meta.last_membership)
            .map_err(|e| tonic::Status::internal(format!("membership decode: {e}")))?;

        let snapshot_meta = openraft::SnapshotMeta {
            last_log_id,
            last_membership,
        };

        let vote = vote_from_wire(req.vote_term, req.vote_node_id, req.vote_committed);
        let snapshot = openraft::Snapshot {
            meta: snapshot_meta,
            snapshot: std::io::Cursor::new(req.snapshot_data),
        };

        let raft = self
            .dispatcher
            .get_raft(req.group_id)
            .ok_or_else(|| tonic::Status::not_found(format!("group {} not found", req.group_id)))?;
        let install_resp = raft
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| tonic::Status::internal(format!("InstallSnapshot failed: {e}")))?;

        Ok(tonic::Response::new(raft_rpc::InstallSnapshotResponse {
            vote_term: install_resp.vote.leader_id.term,
            vote_node_id: install_resp.vote.leader_id.voted_for,
            vote_committed: install_resp.vote.committed,
        }))
    }

    async fn get_key(
        &self,
        request: TonicRequest<raft_rpc::GetKeyRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::GetKeyResponse>, tonic::Status> {
        let req = request.into_inner();
        let node = self
            .dispatcher
            .get_node(req.group_id)
            .ok_or_else(|| tonic::Status::not_found(format!("group {} not found", req.group_id)))?;

        match node.get(req.key).await {
            Ok(Some(value)) => Ok(tonic::Response::new(raft_rpc::GetKeyResponse {
                found: true,
                value,
            })),
            Ok(None) => Ok(tonic::Response::new(raft_rpc::GetKeyResponse {
                found: false,
                value: Vec::new(),
            })),
            Err(e) => Err(map_node_error_to_status(e)),
        }
    }

    async fn get_migration_tip(
        &self,
        request: TonicRequest<raft_rpc::GetMigrationTipRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::GetMigrationTipResponse>, tonic::Status>
    {
        let req = request.into_inner();
        let node = self
            .dispatcher
            .get_node(req.group_id)
            .ok_or_else(|| tonic::Status::not_found(format!("group {} not found", req.group_id)))?;

        match node.get_migration_tip(req.epoch).await {
            Ok(tip) => Ok(tonic::Response::new(raft_rpc::GetMigrationTipResponse {
                tip,
            })),
            Err(e) => Err(map_node_error_to_status(e)),
        }
    }

    async fn get_migration_tombstone(
        &self,
        request: TonicRequest<raft_rpc::GetMigrationTombstoneRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::GetMigrationTombstoneResponse>, tonic::Status>
    {
        let req = request.into_inner();
        let node = self
            .dispatcher
            .get_node(req.group_id)
            .ok_or_else(|| tonic::Status::not_found(format!("group {} not found", req.group_id)))?;

        match node.get_migration_tombstone(req.epoch, req.key).await {
            Ok(op) => Ok(tonic::Response::new(
                raft_rpc::GetMigrationTombstoneResponse {
                    op_tag: op.map(|o| o.tag()).unwrap_or(0) as u32,
                },
            )),
            Err(e) => Err(map_node_error_to_status(e)),
        }
    }

    async fn remote_propose(
        &self,
        request: TonicRequest<raft_rpc::RemoteProposeRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::RemoteProposeResponse>, tonic::Status> {
        let req = request.into_inner();
        let node = self
            .dispatcher
            .get_node(req.group_id)
            .ok_or_else(|| tonic::Status::not_found(format!("group {} not found", req.group_id)))?;

        let inner: Request = postcard::from_bytes(&req.request)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        match node.propose(inner).await {
            Ok(resp) => {
                let payload = postcard::to_allocvec(&resp)
                    .map_err(|e| tonic::Status::internal(e.to_string()))?;
                Ok(tonic::Response::new(raft_rpc::RemoteProposeResponse {
                    response: payload,
                }))
            }
            Err(e) => Err(map_node_error_to_status(e)),
        }
    }
}

/// 将 `OpenRaftNode::get`/`get_migration_tip` 的错误映射为 `tonic::Status`.
/// `NotLeader` (含 linearizable 检查失败时的 `ForwardToLeader`) 映射为
/// `failed_precondition`, 供客户端 (`RaftNetworkClient::get_key` 等) 区分
/// "对端不是 leader" 与其它内部错误.
fn map_node_error_to_status(e: Error) -> tonic::Status {
    match e {
        Error::Cluster(ClusterError::NotLeader { .. }) => {
            tonic::Status::failed_precondition(e.to_string())
        }
        _ => tonic::Status::internal(e.to_string()),
    }
}
