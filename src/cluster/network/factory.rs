use super::{
    Arc, DashMap, Duration, HashMap, NetworkError, NodeId, RaftNetworkClient, RaftNetworkFactory,
    RaftServiceClient, RwLock, TypeConfig, Unreachable,
};
use crate::statistics::Statistics;

#[derive(Clone)]
pub struct RaftNetworkClientFactory {
    node_id: NodeId,
    group_id: u64,
    rpc_timeout_ms: u64,
    max_message_size: u64,
    nodes: Arc<RwLock<HashMap<NodeId, String>>>,
    /// gRPC channel 连接池 — 按 node_id 缓存, 避免每次 RPC 都重新 TCP + HTTP/2 握手.
    /// Docker 容器 DNS 解析 (aikv-N) 每次 50-100ms, 复用 channel 可消除此开销.
    channels: Arc<DashMap<NodeId, RaftServiceClient<tonic::transport::Channel>>>,
    stats: Option<Arc<Statistics>>,
}

impl RaftNetworkClientFactory {
    pub fn new(node_id: NodeId, group_id: u64, rpc_timeout_ms: u64, max_message_size: u64) -> Self {
        Self::new_with_stats(node_id, group_id, rpc_timeout_ms, max_message_size, None)
    }

    pub fn new_with_stats(
        node_id: NodeId,
        group_id: u64,
        rpc_timeout_ms: u64,
        max_message_size: u64,
        stats: Option<Arc<Statistics>>,
    ) -> Self {
        Self {
            node_id,
            group_id,
            rpc_timeout_ms,
            max_message_size,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            channels: Arc::new(DashMap::new()),
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

    pub fn with_group_id(&self, group_id: u64) -> Self {
        Self {
            node_id: self.node_id,
            group_id,
            rpc_timeout_ms: self.rpc_timeout_ms,
            max_message_size: self.max_message_size,
            nodes: Arc::clone(&self.nodes),
            channels: Arc::clone(&self.channels),
            stats: self.stats.clone(),
        }
    }

    pub fn add_node(&self, node_id: NodeId, address: String) {
        self.nodes.write().insert(node_id, address);
        // 地址变更时清除旧 channel, 下次 RPC 自动重连
        self.channels.remove(&node_id);
    }

    pub fn remove_node(&self, node_id: NodeId) {
        self.nodes.write().remove(&node_id);
        self.channels.remove(&node_id);
    }

    pub fn list_nodes(&self) -> Vec<(NodeId, String)> {
        self.nodes
            .read()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    /// 获取或创建 gRPC channel — 缓存复用避免频繁 TCP 握手.
    async fn get_or_create_channel(
        &self,
        target: NodeId,
        addr: &str,
        max_message_size: usize,
    ) -> std::result::Result<RaftServiceClient<tonic::transport::Channel>, NetworkError<TypeConfig>>
    {
        // DashMap 内置分片锁, get/insert 无全局锁竞争
        if let Some(client) = self.channels.get(&target) {
            return Ok(client.clone());
        }
        // Slow path: connect + cache (多个并发 miss 可能重复 connect,
        // 后 insert 覆盖前 insert; tonic Channel 基于 Arc, 旧的 clone 不受影响)
        let normalized = normalize_grpc_addr(addr);
        tracing::debug!(%target, %normalized, "gRPC: establishing new channel for peer");
        let req_timeout = Duration::from_millis(self.rpc_timeout_ms);
        let connect_fut = RaftServiceClient::connect(normalized);
        let client = match tokio::time::timeout(req_timeout, connect_fut).await {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                return Err(NetworkError::<TypeConfig>::new(
                    &Unreachable::<TypeConfig>::new(&e),
                ))
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
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size);
        self.channels.insert(target, client.clone());
        Ok(client)
    }
}

/// Ensure a gRPC address has a proper URI scheme (http://) for tonic.
/// Tonic's `Channel::from_shared` requires a valid URI; bare addresses
/// like `127.0.0.1:20349` are rejected.
fn normalize_grpc_addr(addr: &str) -> String {
    if addr.is_empty() {
        return String::new();
    }
    if addr.contains("://") {
        addr.to_string()
    } else {
        format!("http://{}", addr)
    }
}

impl RaftNetworkFactory<TypeConfig> for RaftNetworkClientFactory {
    type Network = RaftNetworkClient;

    async fn new_client(&mut self, target: NodeId, node: &openraft::BasicNode) -> Self::Network {
        // 解析目标地址
        let raw_addr = if !node.addr.is_empty() {
            let addr = node.addr.clone();
            self.nodes.write().insert(target, addr.clone());
            addr
        } else {
            self.nodes.read().get(&target).cloned().unwrap_or_else(|| {
                tracing::error!(
                    target_node_id = target,
                    "gRPC address for node {} is not registered",
                    target,
                );
                String::new()
            })
        };

        let target_addr = normalize_grpc_addr(&raw_addr);

        // 从连接池获取或创建 channel, 避免每次 RPC 都重新 TCP + HTTP/2 握手
        let channel = match self
            .get_or_create_channel(target, &raw_addr, self.max_message_size as usize)
            .await
        {
            Ok(ch) => Some(ch),
            Err(e) => {
                tracing::warn!(%target, error = %e, "gRPC: channel creation failed, will lazy connect on RPC");
                None
            }
        };

        RaftNetworkClient::new(
            self.node_id,
            target,
            target_addr,
            self.group_id,
            self.rpc_timeout_ms,
            self.max_message_size,
            channel,
            Some(self.channels.clone()),
            self.stats.clone(),
        )
    }
}
