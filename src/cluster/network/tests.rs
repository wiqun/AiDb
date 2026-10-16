use super::*;
use tokio;

#[tokio::test]
async fn test_network_factory_unknown_node_returns_empty_addr() {
    let mut factory = RaftNetworkClientFactory::new(1, 0, 100, 1024);
    let node = openraft::BasicNode { addr: "".into() };
    let client = factory.new_client(99, &node).await;
    assert!(
        client.target_addr().is_empty(),
        "expected empty addr for unknown node without cached address, but got: {}",
        client.target_addr(),
    );
}

/// 连接池失效策略: 仅 `Unavailable` (连接级失败) 失效缓存重建连接;
/// 超时与应用级错误 (Internal) 视为连接仍完好, 不失效, 避免选主风暴下
/// 重连风暴放大为选举活锁 (FIX-0065-A1 连接池策略).
///
/// 通过 `channels` 池 + `client` 两个观察点验证:
/// - `Unavailable` → 池 entry 被移除, 本地 client 置 None
/// - `DeadlineExceeded` (超时) → 池 entry 保留, 本地 client 保留
/// - `Internal` (应用级) → 池 entry 保留, 本地 client 保留
#[tokio::test]
async fn invalidate_on_unavailable_only_for_connection_failures() {
    let channels: Arc<DashMap<NodeId, RaftServiceClient<tonic::transport::Channel>>> =
        Arc::new(DashMap::new());
    // 占位 channel (lazy, 不建立真实连接): 仅用于观察池 entry 生命周期,
    // 验证失效判定逻辑, 不发起任何网络 I/O.
    let placeholder = RaftServiceClient::new(
        tonic::transport::Channel::from_static("http://127.0.0.1:1").connect_lazy(),
    );
    channels.insert(2, placeholder);

    let mut client = RaftNetworkClient::new(
        1,
        2,
        "http://127.0.0.1:1".to_string(),
        0,
        100,
        1024,
        None,
        Some(Arc::clone(&channels)),
        None,
    );

    // 基线: 池与本地 client 状态
    assert!(channels.contains_key(&2));

    // Unavailable → 失效
    client.invalidate_on_unavailable(&tonic::Status::unavailable("conn reset"));
    assert!(!channels.contains_key(&2), "Unavailable 应移除连接池 entry");
    assert!(client.client.is_none(), "Unavailable 应置本地 client None");

    // DeadlineExceeded (超时) → 不失效
    channels.insert(
        2,
        RaftServiceClient::new(
            tonic::transport::Channel::from_static("http://127.0.0.1:1").connect_lazy(),
        ),
    );
    client.invalidate_on_unavailable(&tonic::Status::deadline_exceeded("slow peer"));
    assert!(channels.contains_key(&2), "超时不应移除连接池 entry");

    // Internal (应用级) → 不失效
    client.invalidate_on_unavailable(&tonic::Status::internal("internal error"));
    assert!(channels.contains_key(&2), "应用级错误不应移除连接池 entry");
}
