use super::raft_rpc::raft_service_server::RaftService;
use super::{
    raft_rpc, vote_from_wire, AppendEntriesRequest, Arc, ClusterError, Error, HashMap,
    OpenRaftNode, OpenRaftStorage, Request, RwLock, TonicRequest, TypeConfig, VoteRequest,
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

pub(super) fn decode_log_entries(
    raw_entries: Vec<raft_rpc::LogEntry>,
) -> std::result::Result<Vec<crate::cluster::types::LogEntry>, tonic::Status> {
    let mut entries = Vec::with_capacity(raw_entries.len());
    for entry in raw_entries {
        let payload: openraft::alias::EntryPayloadOf<TypeConfig> =
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
    Ok(entries)
}

pub struct RaftServiceImpl {
    dispatcher: Arc<RaftServiceDispatcher>,
    /// group_id -> 尚未拼完的快照片. 分片按顺序到达, 拼满后才交给 raft.
    inflight: Arc<std::sync::Mutex<HashMap<u64, Option<SnapshotAssembly>>>>,
}

impl RaftServiceImpl {
    pub fn new(dispatcher: Arc<RaftServiceDispatcher>) -> Self {
        Self {
            dispatcher,
            inflight: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }
}

pub(crate) struct SnapshotAssembly {
    snapshot_id: String,
    total: u64,
    received: u64,
    path: std::path::PathBuf,
    writer: Option<std::io::BufWriter<std::fs::File>>,
}

impl Drop for SnapshotAssembly {
    fn drop(&mut self) {
        self.writer.take();
        if !self.path.as_os_str().is_empty() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// 把一片快照流式写入临时文件. 返回 `Some` 表示整包已经齐, 可以安装.
pub(crate) fn push_snapshot_chunk(
    slot: &mut Option<SnapshotAssembly>,
    snapshot_id: &str,
    total: u64,
    offset: u64,
    data: Vec<u8>,
) -> std::result::Result<Option<crate::cluster::storage::SnapshotFileData>, String> {
    use std::io::Write;
    if total == 0 {
        return crate::cluster::storage::SnapshotFileData::from_bytes_temp(&data)
            .map(Some)
            .map_err(|e| e.to_string());
    }
    let chunk_len = data.len() as u64;
    if offset > total || chunk_len > total - offset {
        return Err(format!(
            "snapshot chunk out of range offset={offset} len={} total={total}",
            data.len()
        ));
    }
    if offset == 0 && chunk_len == total {
        *slot = None;
        return crate::cluster::storage::SnapshotFileData::from_bytes_temp(&data)
            .map(Some)
            .map_err(|e| e.to_string());
    }
    if offset == 0 {
        let seq = NEXT_RECV_SNAP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("aidb_snap_recv_{}_{}.bin", std::process::id(), seq));
        let file = std::fs::File::create(&path).map_err(|e| e.to_string())?;
        let mut writer = std::io::BufWriter::new(file);
        writer.write_all(&data).map_err(|e| e.to_string())?;
        *slot = Some(SnapshotAssembly {
            snapshot_id: snapshot_id.to_string(),
            total,
            received: chunk_len,
            path,
            writer: Some(writer),
        });
        return Ok(None);
    }
    let Some(cur) = slot.as_mut() else {
        return Err("snapshot chunk without a start".to_string());
    };
    if cur.snapshot_id != snapshot_id || cur.total != total || cur.received != offset {
        return Err("snapshot chunk does not continue the in-flight snapshot".to_string());
    }
    let writer = cur
        .writer
        .as_mut()
        .ok_or_else(|| "snapshot writer closed".to_string())?;
    writer.write_all(&data).map_err(|e| e.to_string())?;
    cur.received += chunk_len;
    if cur.received == cur.total {
        let mut done = slot.take().expect("slot 刚确认存在");
        if let Some(mut w) = done.writer.take() {
            w.flush().map_err(|e| e.to_string())?;
        }
        let path = std::mem::take(&mut done.path);
        return Ok(Some(
            crate::cluster::storage::SnapshotFileData::from_temp_path(
                path,
                cur_total_or(done.total),
            ),
        ));
    }
    Ok(None)
}

fn cur_total_or(total: u64) -> u64 {
    total
}

static NEXT_RECV_SNAP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn decode_vote_request(req: &raft_rpc::VoteRequest) -> VoteRequest<TypeConfig> {
    let last_log_id = if req.last_log_index > 0 {
        Some(openraft::LogId::new(
            openraft::vote::leader_id_std::CommittedLeaderId::new(req.last_log_term),
            req.last_log_index,
        ))
    } else {
        None
    };
    VoteRequest::new(
        vote_from_wire(req.vote_term, req.vote_node_id, req.vote_committed),
        last_log_id,
    )
}

fn encode_vote_response(
    vote_resp: openraft::raft::VoteResponse<TypeConfig>,
) -> raft_rpc::VoteResponse {
    raft_rpc::VoteResponse {
        vote_term: vote_resp.vote.leader_id.term,
        vote_node_id: vote_resp.vote.leader_id.voted_for,
        vote_committed: vote_resp.vote.committed,
        vote_granted: vote_resp.vote_granted,
        is_in_membership: true,
        last_log_index: vote_resp.last_log_id.map(|id| id.index),
        last_log_term: vote_resp.last_log_id.map(|id| id.leader_id.term),
    }
}

fn decode_append_request(
    req: raft_rpc::AppendEntriesRequest,
) -> std::result::Result<(u64, AppendEntriesRequest<TypeConfig>), tonic::Status> {
    let group_id = req.group_id;
    let entries = decode_log_entries(req.entries)?;
    let prev_log_id = match (
        req.prev_log_index,
        req.prev_log_term,
        req.prev_log_leader_id,
    ) {
        (Some(index), Some(term), Some(_)) => Some(openraft::LogId::new(
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
        (Some(index), Some(term), Some(_)) => Some(openraft::LogId::new(
            openraft::vote::leader_id_std::CommittedLeaderId::new(term),
            index,
        )),
        _ => None,
    };
    Ok((
        group_id,
        AppendEntriesRequest {
            vote: vote_from_wire(req.vote_term, req.vote_node_id, req.vote_committed),
            prev_log_id,
            entries,
            leader_commit,
        },
    ))
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    type StreamAppendEntriesStream = tokio_stream::wrappers::ReceiverStream<
        std::result::Result<raft_rpc::StreamAppendResponse, tonic::Status>,
    >;

    async fn vote(
        &self,
        request: TonicRequest<raft_rpc::VoteRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::VoteResponse>, tonic::Status> {
        if let Some(stats) = &self.dispatcher.stats {
            stats.raft_rpc[RaftRpcType::Vote as usize][RaftRpcDirection::Incoming as usize]
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let req = request.into_inner();
        let vote_req = decode_vote_request(&req);
        let raft = self
            .dispatcher
            .get_raft(req.group_id)
            .ok_or_else(|| tonic::Status::not_found(format!("group {} not found", req.group_id)))?;
        let vote_resp = raft
            .vote(vote_req)
            .await
            .map_err(|e| tonic::Status::internal(format!("Vote failed: {e}")))?;
        Ok(tonic::Response::new(encode_vote_response(vote_resp)))
    }

    async fn pre_vote(
        &self,
        request: TonicRequest<raft_rpc::VoteRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::VoteResponse>, tonic::Status> {
        if let Some(stats) = &self.dispatcher.stats {
            stats.raft_rpc[RaftRpcType::Vote as usize][RaftRpcDirection::Incoming as usize]
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let req = request.into_inner();
        let vote_req = decode_vote_request(&req);
        let raft = self
            .dispatcher
            .get_raft(req.group_id)
            .ok_or_else(|| tonic::Status::not_found(format!("group {} not found", req.group_id)))?;
        let vote_resp = raft
            .pre_vote(vote_req)
            .await
            .map_err(|e| tonic::Status::internal(format!("PreVote failed: {e}")))?;
        Ok(tonic::Response::new(encode_vote_response(vote_resp)))
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

        let entries = decode_log_entries(req.entries)?;

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
            Ok(_) => raft_rpc::AppendEntriesResponse {
                vote_term: 0,
                vote_node_id: 0,
                vote_committed: false,
                success: true,
                conflict_index: None,
                conflict_term: None,
            },
            Err(openraft::raft::AppendError::Conflict { log_id, hint }) => {
                let first_hint = hint.as_ref().and_then(|h| h.log_ids.first()).copied();
                let (conflict_index, conflict_term) = match first_hint {
                    Some(id) => (Some(id.index), Some(id.leader_id.term)),
                    None => (Some(log_id.index), Some(log_id.leader_id.term)),
                };
                raft_rpc::AppendEntriesResponse {
                    vote_term: 0,
                    vote_node_id: 0,
                    vote_committed: false,
                    success: false,
                    conflict_index,
                    conflict_term,
                }
            }
            Err(openraft::raft::AppendError::HigherVote(vote)) => raft_rpc::AppendEntriesResponse {
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

    async fn stream_append_entries(
        &self,
        request: TonicRequest<tonic::Streaming<raft_rpc::AppendEntriesRequest>>,
    ) -> std::result::Result<tonic::Response<Self::StreamAppendEntriesStream>, tonic::Status> {
        use futures_util::StreamExt;
        let mut inbound = request.into_inner();
        let first = match inbound.message().await? {
            Some(msg) => msg,
            None => {
                let (_tx, rx) = tokio::sync::mpsc::channel(1);
                return Ok(tonic::Response::new(
                    tokio_stream::wrappers::ReceiverStream::new(rx),
                ));
            }
        };
        let (group_id, first_req) = decode_append_request(first)?;
        let raft = self
            .dispatcher
            .get_raft(group_id)
            .ok_or_else(|| tonic::Status::not_found(format!("group {group_id} not found")))?;

        let (req_tx, req_rx) = tokio::sync::mpsc::channel(64);
        let (resp_tx, resp_rx) = tokio::sync::mpsc::channel(64);
        let stats = self.dispatcher.stats.clone();

        tokio::spawn(async move {
            let mut count = first_req.entries.len() as u64;
            if req_tx.send(first_req).await.is_err() {
                return;
            }
            crate::cluster::contact::note_leader_append(group_id);
            while let Ok(Some(msg)) = inbound.message().await {
                count += msg.entries.len() as u64;
                if let Ok((_, decoded)) = decode_append_request(msg) {
                    if req_tx.send(decoded).await.is_err() {
                        break;
                    }
                } else {
                    break;
                }
            }
            if let Some(s) = &stats {
                s.raft_log_entries
                    .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            }
        });

        tokio::spawn(async move {
            let mut out =
                Box::pin(raft.stream_append(tokio_stream::wrappers::ReceiverStream::new(req_rx)));
            while let Some(res) = out.next().await {
                let wire = match res {
                    Ok(Ok(ack)) => raft_rpc::StreamAppendResponse {
                        vote_term: 0,
                        vote_node_id: 0,
                        vote_committed: false,
                        success: true,
                        ack_index: ack.map(|id| id.index),
                        ack_term: ack.map(|id| id.leader_id.term),
                        conflict_index: None,
                        conflict_term: None,
                        hint_indices: Vec::new(),
                        hint_terms: Vec::new(),
                    },
                    Ok(Err(openraft::raft::AppendError::Conflict { log_id, hint })) => {
                        let (hint_indices, hint_terms) = match hint {
                            Some(h) => (
                                h.log_ids.iter().map(|id| id.index).collect(),
                                h.log_ids.iter().map(|id| id.leader_id.term).collect(),
                            ),
                            None => (Vec::new(), Vec::new()),
                        };
                        raft_rpc::StreamAppendResponse {
                            vote_term: 0,
                            vote_node_id: 0,
                            vote_committed: false,
                            success: false,
                            ack_index: None,
                            ack_term: None,
                            conflict_index: Some(log_id.index),
                            conflict_term: Some(log_id.leader_id.term),
                            hint_indices,
                            hint_terms,
                        }
                    }
                    Ok(Err(openraft::raft::AppendError::HigherVote(vote))) => {
                        raft_rpc::StreamAppendResponse {
                            vote_term: vote.leader_id.term,
                            vote_node_id: vote.leader_id.voted_for,
                            vote_committed: vote.committed,
                            success: false,
                            ack_index: None,
                            ack_term: None,
                            conflict_index: None,
                            conflict_term: None,
                            hint_indices: Vec::new(),
                            hint_terms: Vec::new(),
                        }
                    }
                    Err(e) => {
                        let _ = resp_tx
                            .send(Err(tonic::Status::internal(format!(
                                "StreamAppend failed: {e}"
                            ))))
                            .await;
                        break;
                    }
                };
                if resp_tx.send(Ok(wire)).await.is_err() {
                    break;
                }
            }
        });

        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(resp_rx),
        ))
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

        let last_membership: openraft::alias::StoredMembershipOf<TypeConfig> =
            postcard::from_bytes(&meta.last_membership)
                .map_err(|e| tonic::Status::internal(format!("membership decode: {e}")))?;

        let snapshot_meta = openraft::SnapshotMeta {
            last_log_id,
            last_membership,
        };

        let vote = vote_from_wire(req.vote_term, req.vote_node_id, req.vote_committed);
        let snapshot_data = {
            let mut guard = self.inflight.lock().unwrap_or_else(|e| e.into_inner());
            let outcome = {
                let slot = guard.entry(req.group_id).or_default();
                push_snapshot_chunk(
                    slot,
                    &meta.snapshot_id,
                    req.snapshot_total,
                    req.snapshot_offset,
                    req.snapshot_data,
                )
            };
            match outcome {
                Ok(Some(bytes)) => {
                    guard.remove(&req.group_id);
                    Ok(Some(bytes))
                }
                Ok(None) => Ok(None),
                Err(e) => {
                    guard.remove(&req.group_id);
                    Err(e)
                }
            }
        };
        let snapshot_data = match snapshot_data {
            Ok(bytes) => bytes,
            Err(e) => return Err(tonic::Status::invalid_argument(e)),
        };
        let Some(snapshot_data) = snapshot_data else {
            return Ok(tonic::Response::new(raft_rpc::InstallSnapshotResponse {
                vote_term: req.vote_term,
                vote_node_id: req.vote_node_id,
                vote_committed: req.vote_committed,
            }));
        };
        let snapshot = openraft::Snapshot {
            meta: snapshot_meta,
            snapshot: snapshot_data,
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

    async fn transfer_leader(
        &self,
        request: TonicRequest<raft_rpc::TransferLeaderRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::TransferLeaderResponse>, tonic::Status> {
        let req = request.into_inner();
        let raft = self
            .dispatcher
            .get_raft(req.group_id)
            .ok_or_else(|| tonic::Status::not_found(format!("group {} not found", req.group_id)))?;
        raft.trigger()
            .transfer_leader(req.target_node_id)
            .await
            .map_err(|e| tonic::Status::internal(format!("TransferLeader failed: {e}")))?;
        Ok(tonic::Response::new(raft_rpc::TransferLeaderResponse {}))
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

#[cfg(test)]
mod tests {
    use super::push_snapshot_chunk;

    #[test]
    fn snapshot_chunks_reassemble_in_order() {
        let mut slot = None;
        let first = push_snapshot_chunk(&mut slot, "snap-1", 5, 0, b"ab".to_vec()).unwrap();
        assert!(first.is_none());
        let second = push_snapshot_chunk(&mut slot, "snap-1", 5, 2, b"cde".to_vec()).unwrap();
        let snap = second.unwrap();
        assert_eq!(std::fs::read(snap.path().unwrap()).unwrap(), b"abcde");
        assert!(slot.is_none());
    }

    #[test]
    fn snapshot_chunk_gap_discards_partial() {
        let mut slot = None;
        push_snapshot_chunk(&mut slot, "snap-1", 5, 0, b"ab".to_vec()).unwrap();
        let err = push_snapshot_chunk(&mut slot, "snap-1", 5, 3, b"de".to_vec()).unwrap_err();
        assert!(err.contains("does not continue"));
    }

    #[test]
    fn whole_snapshot_does_not_need_assembly() {
        let mut slot = None;
        let got = push_snapshot_chunk(&mut slot, "snap-1", 0, 0, b"whole".to_vec()).unwrap();
        let snap = got.unwrap();
        assert_eq!(std::fs::read(snap.path().unwrap()).unwrap(), b"whole");
    }
}
