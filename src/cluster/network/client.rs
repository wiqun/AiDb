use super::{
    raft_rpc, timeout, vote_from_wire, AppendEntriesRequest, AppendEntriesResponse, Arc,
    ClusterError, DashMap, Duration, Error, NetworkError, NodeId, RPCError, RPCOption,
    RaftNetworkV2, RaftServiceClient, Request, Response, Result, SnapshotResponse, TonicRequest,
    TypeConfig, Unreachable, VoteRequest, VoteResponse,
};
use crate::statistics::{RaftRpcDirection, RaftRpcType, Statistics};

pub struct RaftNetworkClient {
    #[cfg_attr(not(feature = "cluster-test-util"), allow(dead_code))]
    node_id: NodeId,
    /// RPC 目标节点 ID — 连接池缓存的 key, 网络失败时据此失效重建.
    target: NodeId,
    target_addr: String,
    pub(super) client: Option<RaftServiceClient<tonic::transport::Channel>>,
    /// 共享 gRPC 连接池 (与 `RaftNetworkClientFactory` 同一实例).
    /// 网络失败时移除对应 entry, 使下个 RPC 重建 channel 并重新解析地址.
    channels: Option<Arc<DashMap<NodeId, RaftServiceClient<tonic::transport::Channel>>>>,
    request_timeout: Duration,
    group_id: u64,
    max_message_size: usize,
    stats: Option<Arc<Statistics>>,
}

impl RaftNetworkClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeId,
        target: NodeId,
        target_addr: String,
        group_id: u64,
        rpc_timeout_ms: u64,
        max_message_size: u64,
        channel: Option<RaftServiceClient<tonic::transport::Channel>>,
        channels: Option<Arc<DashMap<NodeId, RaftServiceClient<tonic::transport::Channel>>>>,
        stats: Option<Arc<Statistics>>,
    ) -> Self {
        Self {
            node_id,
            target,
            target_addr,
            client: channel,
            channels,
            request_timeout: Duration::from_millis(rpc_timeout_ms),
            group_id,
            max_message_size: max_message_size as usize,
            stats,
        }
    }

    /// 对端网络不可达时失效连接池缓存, 迫使下个 RPC 重新解析地址并建连.
    ///
    /// tonic/gRPC channel 内置指数退避 (默认 1s→120s), 长分区结束后若不重建
    /// channel, 节点恢复同步要等退避倒计时跑完 (实测 ~100s). 每次网络失败即
    /// 失效缓存, 自愈收敛到下一个心跳周期 (~500ms).
    fn invalidate_channel(&mut self) {
        if let Some(map) = &self.channels {
            map.remove(&self.target);
        }
        self.client = None;
    }

    /// 仅当连接级失败 (`tonic::Code::Unavailable`) 时失效连接池缓存.
    ///
    /// 分区断网后 tonic 会在连接关闭时返回 `Unavailable`, 失效缓存让下个 RPC
    /// 重建 channel, 绕开指数退避快速自愈; 而**超时**与**应用级错误** (Internal /
    /// FailedPrecondition 等) 通常表示连接完好、只是对端处理慢 (选主风暴 / DB
    /// 阻塞), 失效缓存反而触发 DNS 解析 + TCP/HTTP2 握手的重连风暴, 让健康节点
    /// 之间的 vote 心跳雪上加霜 (实测演化为 ~3min 选举活锁). 因此只有
    /// `Unavailable` 视为连接损坏并失效缓存, 其余一律保留.
    pub(super) fn invalidate_on_unavailable(&mut self, status: &tonic::Status) {
        if status.code() == tonic::Code::Unavailable {
            self.invalidate_channel();
        }
    }

    #[cfg(test)]
    pub fn target_addr(&self) -> &str {
        &self.target_addr
    }

    /// FIX-0056-A1: 跨节点合并读 — 向本 client 的目标节点 (预期为 group
    /// leader) 请求指定 key 的当前值. 与 `append_entries`/`vote` 共用同一
    /// gRPC 通道/连接池/超时配置.
    ///
    /// 超时映射为 `ClusterError::Timeout`, 调用方 (`MultiRaftNode`) 必须
    /// 将其视为不确定结果 (向客户端 TRYAGAIN), **不得** 静默当作 key 不存在.
    pub async fn get_key(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let req_timeout = self.request_timeout;
        let group_id = self.group_id;
        let client = self
            .get_client()
            .await
            .map_err(|e| Error::Cluster(ClusterError::Raft(e.to_string())))?;

        let request = raft_rpc::GetKeyRequest {
            group_id,
            key: key.to_vec(),
        };
        let response = match timeout(req_timeout, client.get_key(TonicRequest::new(request))).await
        {
            Ok(Ok(r)) => r,
            Ok(Err(status)) => {
                self.invalidate_on_unavailable(&status);
                return Err(map_rpc_status_error("GetKey", status));
            }
            Err(_) => {
                return Err(Error::Cluster(ClusterError::Timeout(format!(
                    "GetKey timeout after {}ms",
                    req_timeout.as_millis()
                ))));
            }
        };
        let resp = response.into_inner();
        Ok(resp.found.then_some(resp.value))
    }

    /// FIX-0056-A1: 跨节点读取 `mig/{group}/{epoch}/tip` — `read_migration_tip`
    /// 在 group 非本地时的远程 fallback, 语义与 `get_key` 一致.
    pub async fn get_migration_tip(&mut self, epoch: u64) -> Result<u64> {
        let req_timeout = self.request_timeout;
        let group_id = self.group_id;
        let client = self
            .get_client()
            .await
            .map_err(|e| Error::Cluster(ClusterError::Raft(e.to_string())))?;

        let request = raft_rpc::GetMigrationTipRequest { group_id, epoch };
        let response = match timeout(
            req_timeout,
            client.get_migration_tip(TonicRequest::new(request)),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(status)) => {
                self.invalidate_on_unavailable(&status);
                return Err(map_rpc_status_error("GetMigrationTip", status));
            }
            Err(_) => {
                return Err(Error::Cluster(ClusterError::Timeout(format!(
                    "GetMigrationTip timeout after {}ms",
                    req_timeout.as_millis()
                ))));
            }
        };
        Ok(response.into_inner().tip)
    }

    /// FIX-0056-A1 合并读线性点第 1 步: 跨节点读取 target group 上 `epoch`
    /// 内 `key` 的迁移 tombstone. 语义与 `get_key`/`get_migration_tip` 一致
    /// (同一 gRPC 通道, 超时映射为 `ClusterError::Timeout`).
    pub async fn get_migration_tombstone(
        &mut self,
        epoch: u64,
        key: &[u8],
    ) -> Result<Option<crate::cluster::migration_oplog::MigOp>> {
        let req_timeout = self.request_timeout;
        let group_id = self.group_id;
        let client = self
            .get_client()
            .await
            .map_err(|e| Error::Cluster(ClusterError::Raft(e.to_string())))?;

        let request = raft_rpc::GetMigrationTombstoneRequest {
            group_id,
            epoch,
            key: key.to_vec(),
        };
        let response = match timeout(
            req_timeout,
            client.get_migration_tombstone(TonicRequest::new(request)),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(status)) => {
                self.invalidate_on_unavailable(&status);
                return Err(map_rpc_status_error("GetMigrationTombstone", status));
            }
            Err(_) => {
                return Err(Error::Cluster(ClusterError::Timeout(format!(
                    "GetMigrationTombstone timeout after {}ms",
                    req_timeout.as_millis()
                ))));
            }
        };
        let tag = response.into_inner().op_tag;
        Ok(crate::cluster::migration_oplog::MigOp::from_tag(tag as u8))
    }

    /// 跨节点迁移写: 向目标 group leader 节点 propose 一条数据面请求.
    ///
    /// 用于 `propose_group` 在 group 非本地时的远程 fallback (在线 slot
    /// 迁移的 `PutConditional` 全量拷贝 / `MigrationBarrier` 写屏障等必须
    /// 落到持有 target group 的节点). 请求/响应以 postcard 序列化, 与
    /// `GetKey` 共用同一 gRPC 通道与超时语义; 对端 `NotLeader` 映射为
    /// `failed_precondition`, 由调用方决定转发/重试.
    pub async fn remote_propose(&mut self, group_id: u64, request: &Request) -> Result<Response> {
        let req_timeout = self.request_timeout;
        let client = self
            .get_client()
            .await
            .map_err(|e| Error::Cluster(ClusterError::Raft(e.to_string())))?;

        let payload = postcard::to_allocvec(request)
            .map_err(|e| Error::Cluster(ClusterError::Internal(e.to_string())))?;
        let request = raft_rpc::RemoteProposeRequest {
            group_id,
            request: payload,
        };
        let response = match timeout(
            req_timeout,
            client.remote_propose(TonicRequest::new(request)),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(status)) => {
                self.invalidate_on_unavailable(&status);
                return Err(map_rpc_status_error("RemotePropose", status));
            }
            Err(_) => {
                return Err(Error::Cluster(ClusterError::Timeout(format!(
                    "RemotePropose timeout after {}ms",
                    req_timeout.as_millis()
                ))));
            }
        };
        let payload = response.into_inner().response;
        postcard::from_bytes(&payload)
            .map_err(|e| Error::Cluster(ClusterError::Internal(e.to_string())))
    }

    async fn get_client(
        &mut self,
    ) -> std::result::Result<
        &mut RaftServiceClient<tonic::transport::Channel>,
        NetworkError<TypeConfig>,
    > {
        if self.client.is_none() {
            // Fallback: lazy connect (backward compat, 缓存未命中时)
            // connect 纳入 request_timeout 保护: 静默黑洞分区 (丢包无 RST) 下
            // tonic connect 依赖内核 SYN 重传, 可能挂起远超 RPC 超时, 破坏
            // 快速失败不变量. 超时后由调用方失效缓存, 下个 RPC 重试.
            tracing::debug!(
              target_addr = %self.target_addr,
              group_id = self.group_id,
              "gRPC: lazy connect (channel not cached)",
            );
            let req_timeout = self.request_timeout;
            let target_addr = self.target_addr.clone();
            let connect_fut = RaftServiceClient::connect(target_addr);
            let client = match tokio::time::timeout(req_timeout, connect_fut).await {
                Ok(Ok(c)) => c,
                Ok(Err(e)) => {
                    tracing::warn!(
                      target_addr = %self.target_addr,
                      error = %e,
                      "gRPC client connect failed",
                    );
                    return Err(NetworkError::<TypeConfig>::new(
                        &Unreachable::<TypeConfig>::new(&e),
                    ));
                }
                Err(_) => {
                    let io_err = std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("gRPC connect timeout after {}ms", req_timeout.as_millis()),
                    );
                    return Err(NetworkError::<TypeConfig>::new(
                        &Unreachable::<TypeConfig>::new(&io_err),
                    ));
                }
            }
            .max_decoding_message_size(self.max_message_size)
            .max_encoding_message_size(self.max_message_size);
            self.client = Some(client);
        }
        Ok(self.client.as_mut().unwrap())
    }
}

impl RaftNetworkV2<TypeConfig> for RaftNetworkClient {
    type SnapshotData = std::io::Cursor<Vec<u8>>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        #[cfg(feature = "cluster-test-util")]
        if crate::cluster::failpoint::blackhole_active(self.node_id, &self.target_addr) {
            return Err(RPCError::Network(NetworkError::<TypeConfig>::from_string(
                "blackhole",
            )));
        }
        if let Some(stats) = &self.stats {
            stats.raft_rpc[RaftRpcType::AppendEntries as usize]
                [RaftRpcDirection::Outgoing as usize]
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let req_timeout = self.request_timeout;
        let group_id = self.group_id;
        let client = match self.get_client().await {
            Ok(c) => c,
            Err(e) => {
                self.invalidate_channel();
                return Err(RPCError::Network(e));
            }
        };

        let mut entries = Vec::new();
        for entry in rpc.entries {
            let payload = rmp_serde::to_vec(&entry.payload).map_err(|e| {
                RPCError::Network(NetworkError::<TypeConfig>::new(
                    &Unreachable::<TypeConfig>::new(&e),
                ))
            })?;
            entries.push(raft_rpc::LogEntry {
                log_index: entry.log_id.index,
                log_term: entry.log_id.leader_id.term,
                log_leader_id: 0, // v0.10 std mode CommittedLeaderId only has term
                payload,
                is_blank: matches!(entry.payload, openraft::EntryPayload::Blank),
                is_membership: matches!(entry.payload, openraft::EntryPayload::Membership(_)),
            });
        }

        let request = raft_rpc::AppendEntriesRequest {
            group_id,
            vote_term: rpc.vote.leader_id.term,
            vote_node_id: rpc.vote.leader_id.voted_for,
            vote_committed: rpc.vote.committed,
            prev_log_index: rpc.prev_log_id.map(|id| id.index),
            prev_log_term: rpc.prev_log_id.map(|id| id.leader_id.term),
            prev_log_leader_id: rpc.prev_log_id.map(|_id| 0), // v0.10 CommittedLeaderId has no node_id
            entries,
            leader_commit_index: rpc.leader_commit.map(|id| id.index),
            leader_commit_term: rpc.leader_commit.map(|id| id.leader_id.term),
            leader_commit_leader_id: rpc.leader_commit.map(|_id| 0), // v0.10 CommittedLeaderId has no node_id
        };

        let rpc_future = client.append_entries(TonicRequest::new(request));
        let t0 = std::time::Instant::now();
        let response = match timeout(req_timeout, rpc_future).await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                self.invalidate_on_unavailable(&e);
                return Err(RPCError::Network(NetworkError::<TypeConfig>::new(
                    &Unreachable::<TypeConfig>::new(&e),
                )));
            }
            Err(_) => {
                let io_err = std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("AppendEntries timeout after {}ms", req_timeout.as_millis()),
                );
                return Err(RPCError::Network(NetworkError::<TypeConfig>::new(
                    &Unreachable::<TypeConfig>::new(&io_err),
                )));
            }
        };
        tracing::debug!(
            target: "perf",
            group_id,
            grpc_us = t0.elapsed().as_micros(),
            "raft_rpc_ae_done"
        );

        let resp = response.into_inner();
        if resp.success {
            Ok(AppendEntriesResponse::Success)
        } else if resp.vote_term > 0 || resp.vote_node_id > 0 {
            // HigherVote: the follower has seen a higher term. The leader must step down.
            tracing::info!(
                target = display(&self.target_addr),
                vote_term = resp.vote_term,
                vote_node_id = resp.vote_node_id,
                "received HigherVote from follower, leader must step down"
            );
            let vote = vote_from_wire(resp.vote_term, resp.vote_node_id, resp.vote_committed);
            Ok(AppendEntriesResponse::HigherVote(vote))
        } else {
            Ok(AppendEntriesResponse::Conflict)
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: openraft::alias::VoteOf<TypeConfig>,
        snapshot: openraft::alias::SnapshotOf<TypeConfig, Self::SnapshotData>,
        cancel: impl std::future::Future<Output = openraft::error::ReplicationClosed>
            + openraft::OptionalSend
            + 'static,
        _option: RPCOption,
    ) -> std::result::Result<
        SnapshotResponse<TypeConfig>,
        openraft::error::StreamingError<TypeConfig>,
    > {
        #[cfg(feature = "cluster-test-util")]
        if crate::cluster::failpoint::blackhole_active(self.node_id, &self.target_addr) {
            return Err(openraft::error::StreamingError::Network(NetworkError::<
                TypeConfig,
            >::from_string(
                "blackhole"
            )));
        }
        drop(cancel); // Not using cancellation in this implementation
        let req_timeout = self.request_timeout;
        let group_id = self.group_id;
        let client = match self.get_client().await {
            Ok(c) => c,
            Err(e) => {
                self.invalidate_channel();
                return Err(openraft::error::StreamingError::Network(e));
            }
        };

        let meta = raft_rpc::SnapshotMeta {
            last_log_index: snapshot.meta.last_log_id.map(|id| id.index),
            last_log_term: snapshot.meta.last_log_id.map(|id| id.leader_id.term),
            last_log_leader_id: snapshot.meta.last_log_id.map(|id| id.leader_id.term),
            last_membership: postcard::to_allocvec(&snapshot.meta.last_membership).map_err(
                |e| {
                    openraft::error::StreamingError::Network(NetworkError::<TypeConfig>::new(
                        &Unreachable::<TypeConfig>::new(&e),
                    ))
                },
            )?,
            snapshot_id: snapshot
                .meta
                .last_log_id
                .map(|id| format!("snap-{}", id.index))
                .unwrap_or_default(),
        };

        let data = snapshot.snapshot.into_inner();
        let request = raft_rpc::InstallSnapshotRequest {
            group_id,
            vote_term: vote.leader_id.term,
            vote_node_id: vote.leader_id.voted_for,
            vote_committed: vote.committed,
            meta: Some(meta),
            snapshot_data: data,
        };

        let rpc_future = client.install_snapshot(TonicRequest::new(request));
        let response = match timeout(req_timeout, rpc_future).await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                self.invalidate_on_unavailable(&e);
                if e.code() == tonic::Code::Unavailable {
                    return Err(openraft::error::StreamingError::Unreachable(Unreachable::<
                        TypeConfig,
                    >::new(
                        &e
                    )));
                }
                return Err(openraft::error::StreamingError::Network(NetworkError::<
                    TypeConfig,
                >::new(
                    &e
                )));
            }
            Err(_) => {
                let io_err = std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "InstallSnapshot timeout after {}ms",
                        req_timeout.as_millis()
                    ),
                );
                return Err(openraft::error::StreamingError::Unreachable(Unreachable::<
                    TypeConfig,
                >::new(
                    &io_err
                )));
            }
        };

        let resp = response.into_inner();
        Ok(SnapshotResponse {
            vote: vote_from_wire(resp.vote_term, resp.vote_node_id, resp.vote_committed),
        })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        #[cfg(feature = "cluster-test-util")]
        if crate::cluster::failpoint::blackhole_active(self.node_id, &self.target_addr) {
            return Err(RPCError::Network(NetworkError::<TypeConfig>::from_string(
                "blackhole",
            )));
        }
        if let Some(stats) = &self.stats {
            stats.raft_rpc[RaftRpcType::Vote as usize][RaftRpcDirection::Outgoing as usize]
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let req_timeout = self.request_timeout;
        let group_id = self.group_id;
        let client = match self.get_client().await {
            Ok(c) => c,
            Err(e) => {
                self.invalidate_channel();
                return Err(RPCError::Network(e));
            }
        };

        let request = raft_rpc::VoteRequest {
            group_id,
            vote_term: rpc.vote.leader_id.term,
            vote_node_id: rpc.vote.leader_id.voted_for,
            vote_committed: rpc.vote.committed,
            last_log_index: rpc.last_log_id.map(|id| id.index).unwrap_or(0),
            last_log_term: rpc.last_log_id.map(|id| id.leader_id.term).unwrap_or(0),
            last_log_leader_id: rpc.last_log_id.map(|_id| 0).unwrap_or(0),
        };

        let rpc_future = client.vote(TonicRequest::new(request));
        let response = match timeout(req_timeout, rpc_future).await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                self.invalidate_on_unavailable(&e);
                return Err(RPCError::Network(NetworkError::<TypeConfig>::new(&e)));
            }
            Err(_) => {
                let io_err = std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("Vote timeout after {}ms", req_timeout.as_millis()),
                );
                return Err(RPCError::Network(NetworkError::<TypeConfig>::new(
                    &Unreachable::<TypeConfig>::new(&io_err),
                )));
            }
        };

        let resp = response.into_inner();
        Ok(VoteResponse {
            vote: vote_from_wire(resp.vote_term, resp.vote_node_id, resp.vote_committed),
            vote_granted: resp.vote_granted,
            last_log_id: None,
        })
    }
}

/// 将 `GetKey`/`GetMigrationTip` 的 `tonic::Status` 映射为 `ClusterError`.
/// `FailedPrecondition` (对端非 leader / linearizable 检查失败) 映射为
/// `NotLeader`, 其余映射为 `Raft` (保留原始 gRPC 错误信息, 不伪装成"确定性"
/// 结果, 呼应 A1"超时/失败禁止静默 fallback"不变式).
fn map_rpc_status_error(rpc_name: &str, status: tonic::Status) -> Error {
    match status.code() {
        tonic::Code::FailedPrecondition => Error::Cluster(ClusterError::NotLeader {
            leader: None,
            leader_addr: None,
            is_ask: false,
        }),
        tonic::Code::NotFound => Error::Cluster(ClusterError::Raft(format!(
            "{rpc_name}: {}",
            status.message()
        ))),
        // 对端可能把 openraft ForwardToLeader 打成 Internal/其它码; 统一成 NotLeader
        // 以便上层 (CLUSTER MEET 等) 退避重试, 而不是把 Display 串当致命 Raft 错.
        other if status.message().contains("has to forward request to") => {
            tracing::debug!(
                rpc = rpc_name,
                code = ?other,
                msg = %status.message(),
                "map ForwardToLeader-like status to NotLeader"
            );
            Error::Cluster(ClusterError::NotLeader {
                leader: None,
                leader_addr: None,
                is_ask: false,
            })
        }
        _ => Error::Cluster(ClusterError::Raft(format!(
            "{rpc_name} failed: {}",
            status.message()
        ))),
    }
}
