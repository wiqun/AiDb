//! gRPC 网络层 — Raft 对等 RPC (vote / append_entries / full_snapshot) 与扩展
//! RPC (get_key / migration tip+tombstone / remote_propose) 的 client / server,
//! 以及按 `group_id` 分发的统一 dispatcher. 一个端口承载 MetaRaft (gid=0) 与
//! 全部数据 Group (gid≥1).
//!
//! # 数据流
//!
//! ```text
//! 本地 openraft::Raft core
//!   └─ RaftNetworkClientFactory::new_client(target, node)
//!        ├─ channel 连接池 (DashMap 按 node_id 缓存, 复用 TCP/HTTP2)
//!        └─ RaftNetworkClient (携带 group_id)
//!             └─ gRPC -> 对端 RaftServiceImpl (RaftServiceDispatcher)
//!                  ├─ dispatcher.get_raft(group_id)   -> vote/append/snapshot
//!                  └─ dispatcher.get_node(group_id)   -> get_key/migration/remote_propose
//! ```
//!
//! # Invariant
//!
//! - Raft RPC 一律发往 `rpc_addr` (BasicNode.address 字段); `client_addr` 仅供
//!   客户端 MOVED 重定向, 不参与 Raft 共识通信.
//! - 连接复用: channel 按 node_id 缓存, 避免 Docker DNS 解析的 50-100ms 开销;
//!   `add_node` 时清除旧 channel 以拾取地址变更.
//! - 超时 / Unavailable 归类为不确定错误 (RPCError), 不伪装成确定性结果.
//! - channel 失效策略: 仅 `Unavailable` (连接级失败) 失效缓存重建连接, 绕开
//!   tonic 指数退避; 超时与应用级错误 (Internal 等) 连接仍完好, 不失效, 避免
//!   选主风暴下重连风暴放大为选举活锁. 详见 `invalidate_on_unavailable`.
//! - 快速失败: 所有 RPC (含 lazy connect) 均受 `request_timeout` 约束, 静默黑洞
//!   (丢包无 RST) 下 connect 不会依赖内核 SYN 重传长时间挂起.

pub(super) use dashmap::DashMap;
pub(super) use parking_lot::RwLock;
pub(super) use std::collections::HashMap;
pub(super) use std::sync::Arc;
pub(super) use std::time::Duration;
pub(super) use tokio::time::timeout;
pub(super) use tonic::Request as TonicRequest;

pub(super) use openraft::{
    error::{NetworkError, RPCError, Unreachable},
    network::RPCOption,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    RaftNetworkFactory, RaftNetworkV2,
};

pub(super) use crate::cluster::node::OpenRaftNode;
pub(super) use crate::cluster::storage::OpenRaftStorage;
pub(super) use crate::cluster::types::{ClusterError, NodeId, Request, Response, TypeConfig};
pub(super) use crate::error::{Error, Result};

#[allow(clippy::all)]
pub mod raft_rpc {
    include!(concat!(env!("OUT_DIR"), "/aidb.raft.rs"));
}

pub(super) use raft_rpc::raft_service_client::RaftServiceClient;

/// 根据 RPC 中携带的 `committed` 标志构造 `Vote`.
///
/// 注意: 必须尊重对端传来的 committed 状态, 不能无条件 `new_committed`.
/// candidate 发起选举时其 vote 是 uncommitted, 若误标为 committed 会让
/// 其他节点进入 leader-lease 保护而拒绝投票, 导致选举死锁.
pub(super) fn vote_from_wire<LID>(
    term: LID::Term,
    node_id: LID::NodeId,
    committed: bool,
) -> openraft::Vote<LID>
where
    LID: openraft::vote::RaftLeaderId,
{
    if committed {
        openraft::Vote::new_committed(term, node_id)
    } else {
        openraft::Vote::new(term, node_id)
    }
}

mod client;
mod factory;
mod incoming;
mod server;
#[cfg(test)]
mod tests;

pub use client::RaftNetworkClient;
pub use factory::RaftNetworkClientFactory;
pub use incoming::{raft_server, tcp_incoming};
pub use server::{RaftServiceDispatcher, RaftServiceImpl};
