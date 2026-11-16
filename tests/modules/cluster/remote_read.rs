//! FIX-0056-A1 "读导向" 点 3 (`get_key_from_group_remote`) / "tip 跨节点读取"
//! @component aidb-cluster
//! (`read_migration_tip` 远程 fallback) 端到端测试.
//!
//! 两个真实 `MultiRaftNode`, 真实数据面 gRPC (`GetKey`/`GetMigrationTip`),
//! 不使用 mock:
//! - node A (leader): 单节点 MetaRaft 驱动, 本地托管 `TARGET_GROUP`, 启动统一
//!   数据面 gRPC server (Vote/AppendEntries/GetKey/GetMigrationTip 共用同一端口).
//! - node B (follower): 不托管 `TARGET_GROUP`, Router 手动配置 leader 指向 A,
//!   经 `RaftNetworkClient::get_key`/`get_migration_tip` 跨节点读取.

use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use aidb::cluster::lifecycle_manager::{LifecycleManager, MetaRaftProvider};
use aidb::cluster::meta_types::{default_slot_table, ClusterMeta, SlotMigrationState, SlotTable};
use aidb::cluster::multi_raft_node::LifecycleConfig;
use aidb::cluster::network::raft_rpc;
use aidb::cluster::types::{Request, ThinWriteBatch};
use aidb::cluster::{
    ClusterError, MetaRaftNode, MetaRequest, MultiRaftNode, RaftNetworkClientFactory,
    RaftNodeConfig, RaftServiceDispatcher, Router,
};
use aidb::config::Options;
use aidb::error::Error;
use aidb::DB;
use tempfile::TempDir;

const TARGET_GROUP: u64 = 2;

struct MetaRaftProv(Arc<MetaRaftNode>);

impl MetaRaftProvider for MetaRaftProv {
    fn get_cluster_meta(&self) -> ClusterMeta {
        self.0.get_cluster_meta()
    }
    fn get_slot_table(&self) -> SlotTable {
        self.0.get_slot_table()
    }
    fn get_migration_state(&self) -> Option<SlotMigrationState> {
        self.0.get_migration_state()
    }
}

fn pick_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

/// 搭建 node A: 单节点 MetaRaft 驱动 `MultiRaftNode` 本地托管 `TARGET_GROUP`
/// (leader), 并启动统一数据面 gRPC server. 返回 (multi_raft, "http://addr", _dirs).
async fn setup_leader_node() -> (Arc<MultiRaftNode>, String, TempDir, TempDir) {
    let meta_dir = TempDir::new().unwrap();
    let meta_db = DB::open(meta_dir.path().join("meta"), Options::for_testing()).unwrap();
    let meta_factory =
        RaftNetworkClientFactory::new(1, aidb::cluster::METARAFT_GROUP_ID, 30, 65 * 1024 * 1024);
    let meta_cfg = RaftNodeConfig {
        node_id: 1,
        group_id: aidb::cluster::METARAFT_GROUP_ID,
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 30,
        rpc_timeout_ms: 30,
        snapshot_logs_since_last: 200,
        ..Default::default()
    };
    let meta_raft = Arc::new(
        MetaRaftNode::new(meta_cfg, meta_db, meta_factory)
            .await
            .unwrap(),
    );
    meta_raft
        .initialize(vec![(1, "http://127.0.0.1:1".into())])
        .await
        .unwrap();
    for _ in 0..50 {
        if meta_raft.is_leader().await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        meta_raft.is_leader().await,
        "meta raft should elect itself as leader"
    );

    meta_raft
        .propose(MetaRequest::RegisterNode {
            node_id: 1,
            rpc_addr: "http://127.0.0.1:19300".into(),
            client_addr: None,
            tags: HashMap::new(),
        })
        .await
        .unwrap();
    meta_raft
        .propose(MetaRequest::CreateGroup {
            group_id: TARGET_GROUP,
            initial_replicas: vec![(1, true)],
        })
        .await
        .unwrap();

    let router = Arc::new(Router::new(
        default_slot_table(),
        HashMap::new(),
        HashMap::new(),
    ));
    let dispatcher = Arc::new(RaftServiceDispatcher::new());
    let lifecycle =
        LifecycleManager::new(1, router.clone(), Arc::new(MetaRaftProv(meta_raft.clone())))
            .with_tick_interval(Duration::from_millis(30));
    let multi_raft = Arc::new(MultiRaftNode::new_with_lifecycle(
        1, router, dispatcher, lifecycle,
    ));

    let data_dir = TempDir::new().unwrap();
    let _shutdown_rx = multi_raft.start_lifecycle_with_data(LifecycleConfig {
        data_dir: data_dir.path().to_path_buf(),
        raft_node_config: RaftNodeConfig {
            node_id: 1,
            election_timeout_min: 150,
            election_timeout_max: 300,
            heartbeat_interval: 30,
            rpc_timeout_ms: 30,
            snapshot_logs_since_last: 200,
            ..Default::default()
        },
        options: Options::for_testing(),
        compaction_filter: None,
        compaction_removal_listener_factory: None,
    });

    for _ in 0..100 {
        if multi_raft.local_group_ids().contains(&TARGET_GROUP)
            && multi_raft.is_elected_leader_sync(TARGET_GROUP)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        multi_raft.is_elected_leader_sync(TARGET_GROUP),
        "target group must have a locally elected leader"
    );

    let addr = pick_addr();
    multi_raft
        .start(addr, RaftNodeConfig::default().grpc_max_message_size)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    (multi_raft, format!("http://{addr}"), meta_dir, data_dir)
}

/// 搭建 node B: 不托管任何 group, Router 手动指向 `leader_addr` 作为
/// `TARGET_GROUP` 的 leader.
fn setup_follower_node(leader_node_id: u64, leader_addr: &str) -> Arc<MultiRaftNode> {
    let router = Arc::new(Router::new(
        default_slot_table(),
        HashMap::new(),
        HashMap::new(),
    ));
    let dispatcher = Arc::new(RaftServiceDispatcher::new());
    let multi_raft = Arc::new(MultiRaftNode::new(99, router.clone(), dispatcher));

    let mut group_leaders = HashMap::new();
    group_leaders.insert(TARGET_GROUP, leader_node_id);
    let mut node_addrs = HashMap::new();
    node_addrs.insert(leader_node_id, leader_addr.to_string());
    router.refresh_from_data(
        default_slot_table(),
        HashMap::new(),
        node_addrs,
        group_leaders,
    );

    // Follower 无 lifecycle, `remote_leader_client` 的 MetaRaft 元数据查找
    // 必然落空; 需把 leader 地址注册进 network factory 缓存, 供其回退解析
    // (否则 `new_client` 拿到空 addr → 空 URI, 见 A-003).
    multi_raft.register_peer_addr(leader_node_id, leader_addr.to_string());

    multi_raft
}

#[tokio::test]
async fn get_key_from_group_remote_reads_leader_value_from_non_local_node() {
    let (leader, addr, _meta_dir, _data_dir) = setup_leader_node().await;
    let follower = setup_follower_node(1, &addr);
    assert!(!follower.is_group_local(TARGET_GROUP));

    leader
        .propose_group(
            TARGET_GROUP,
            Request::Put {
                key: b"k1".to_vec(),
                value: bytes::Bytes::from(b"v1".to_vec()),
            },
        )
        .await
        .unwrap();

    let got = follower
        .get_key_from_group_remote(TARGET_GROUP, b"k1")
        .await
        .expect("remote GetKey RPC should succeed");
    assert_eq!(got, Some(b"v1".to_vec()));
}

#[tokio::test]
async fn get_key_from_group_remote_returns_none_for_absent_key_not_error() {
    let (leader, addr, _meta_dir, _data_dir) = setup_leader_node().await;
    let follower = setup_follower_node(1, &addr);
    let _ = &leader;

    let got = follower
        .get_key_from_group_remote(TARGET_GROUP, b"never-written")
        .await
        .expect("remote GetKey RPC should succeed even when key is absent");
    assert_eq!(got, None, "absent key must resolve to Ok(None), not an Err");
}

#[tokio::test]
async fn get_key_from_group_local_delegates_without_remote_rpc() {
    let (leader, addr, _meta_dir, _data_dir) = setup_leader_node().await;
    let _ = &addr;

    leader
        .propose_group(
            TARGET_GROUP,
            Request::Put {
                key: b"k1".to_vec(),
                value: bytes::Bytes::from(b"v1".to_vec()),
            },
        )
        .await
        .unwrap();

    // leader 本地托管该 group, get_key_from_group_remote 必须走本地路径
    // (is_group_local 为 true), 不经 RPC.
    assert!(leader.is_group_local(TARGET_GROUP));
    let got = leader
        .get_key_from_group_remote(TARGET_GROUP, b"k1")
        .await
        .unwrap();
    assert_eq!(got, Some(b"v1".to_vec()));
}

#[tokio::test]
async fn read_migration_tip_remote_reads_leader_tip_across_nodes() {
    let (leader, addr, _meta_dir, _data_dir) = setup_leader_node().await;
    let follower = setup_follower_node(1, &addr);

    let mut ops = ThinWriteBatch::new();
    ops.put(b"k1".to_vec(), b"v1".to_vec());
    leader
        .propose_group(TARGET_GROUP, Request::MigrationWrite { epoch: 7, ops })
        .await
        .unwrap();

    let tip = follower
        .read_migration_tip(TARGET_GROUP, 7)
        .await
        .expect("remote GetMigrationTip RPC should succeed");
    assert_eq!(tip, 1);

    // group 存在, 但这个 epoch 从未写过 —— tip 缺失即 0, 不是 Err.
    let tip_absent_epoch = follower
        .read_migration_tip(TARGET_GROUP, 999)
        .await
        .expect("absent epoch tip must resolve to Ok(0), not Err");
    assert_eq!(tip_absent_epoch, 0);
}

#[tokio::test]
async fn get_migration_tombstone_remote_reads_leader_tombstone_across_nodes() {
    use aidb::cluster::MigOp;

    let (leader, addr, _meta_dir, _data_dir) = setup_leader_node().await;
    let follower = setup_follower_node(1, &addr);

    let mut del_ops = ThinWriteBatch::new();
    del_ops.delete(b"k1".to_vec());
    leader
        .propose_group(
            TARGET_GROUP,
            Request::MigrationWrite {
                epoch: 5,
                ops: del_ops,
            },
        )
        .await
        .unwrap();

    let tombstone = follower
        .get_migration_tombstone_remote(TARGET_GROUP, 5, b"k1")
        .await
        .expect("remote GetMigrationTombstone RPC should succeed");
    assert_eq!(tombstone, Some(MigOp::Del));
}

#[tokio::test]
async fn get_migration_tombstone_remote_returns_none_when_absent() {
    let (leader, addr, _meta_dir, _data_dir) = setup_leader_node().await;
    let follower = setup_follower_node(1, &addr);
    let _ = &leader;

    let tombstone = follower
        .get_migration_tombstone_remote(TARGET_GROUP, 5, b"never-touched")
        .await
        .expect("remote GetMigrationTombstone RPC should succeed even when absent");
    assert_eq!(tombstone, None, "无 tombstone 必须是 Ok(None), 不是 Err");
}

#[tokio::test]
async fn get_migration_tombstone_local_delegates_without_remote_rpc() {
    use aidb::cluster::MigOp;

    let (leader, addr, _meta_dir, _data_dir) = setup_leader_node().await;
    let _ = &addr;

    let mut put_ops = ThinWriteBatch::new();
    put_ops.put(b"k1".to_vec(), b"v1".to_vec());
    leader
        .propose_group(
            TARGET_GROUP,
            Request::MigrationWrite {
                epoch: 5,
                ops: put_ops,
            },
        )
        .await
        .unwrap();

    assert!(leader.is_group_local(TARGET_GROUP));
    let tombstone = leader
        .get_migration_tombstone_remote(TARGET_GROUP, 5, b"k1")
        .await
        .unwrap();
    assert_eq!(tombstone, Some(MigOp::Put));
}

#[tokio::test]
async fn remote_read_for_unknown_group_is_err_not_a_false_negative() {
    let (leader, addr, _meta_dir, _data_dir) = setup_leader_node().await;
    let follower = setup_follower_node(1, &addr);

    // group 999 在 follower 的 router 里没有已知 leader —— 必须是 Err,
    // 不能被解释成"key 不存在"/"tip=0" (禁止把"不确定"伪装成"确定性结果").
    let key_err = follower.get_key_from_group_remote(999, b"k").await;
    assert!(
        key_err.is_err(),
        "unknown group leader must surface as Err, not Ok(None)"
    );
    let tip_err = follower.read_migration_tip(999, 1).await;
    assert!(
        tip_err.is_err(),
        "unknown group leader must surface as Err, not Ok(0)"
    );

    let _ = leader; // 保持 leader (及其 gRPC server) 存活到测试结束
}

/// 模拟对端 leader 的 `GetKey` RPC 长时间不响应 (真实 gRPC server, handler
/// 内部 sleep 超过 rpc_timeout) —— 必须映射为 `ClusterError::Timeout`, 不能
/// 被静默解释为 key 不存在, 呼应 A1"超时禁止静默 fallback"不变式.
struct SlowGetKeyService;

#[tonic::async_trait]
impl raft_rpc::raft_service_server::RaftService for SlowGetKeyService {
    async fn vote(
        &self,
        _request: tonic::Request<raft_rpc::VoteRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::VoteResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used in this test"))
    }

    async fn append_entries(
        &self,
        _request: tonic::Request<raft_rpc::AppendEntriesRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::AppendEntriesResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used in this test"))
    }

    async fn install_snapshot(
        &self,
        _request: tonic::Request<raft_rpc::InstallSnapshotRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::InstallSnapshotResponse>, tonic::Status>
    {
        Err(tonic::Status::unimplemented("not used in this test"))
    }

    async fn get_key(
        &self,
        _request: tonic::Request<raft_rpc::GetKeyRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::GetKeyResponse>, tonic::Status> {
        tokio::time::sleep(Duration::from_secs(10)).await;
        Ok(tonic::Response::new(raft_rpc::GetKeyResponse {
            found: false,
            value: Vec::new(),
        }))
    }

    async fn remote_propose(
        &self,
        _request: tonic::Request<raft_rpc::RemoteProposeRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::RemoteProposeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used in this test"))
    }

    async fn get_migration_tip(
        &self,
        _request: tonic::Request<raft_rpc::GetMigrationTipRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::GetMigrationTipResponse>, tonic::Status>
    {
        Err(tonic::Status::unimplemented("not used in this test"))
    }

    async fn get_migration_tombstone(
        &self,
        _request: tonic::Request<raft_rpc::GetMigrationTombstoneRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::GetMigrationTombstoneResponse>, tonic::Status>
    {
        Err(tonic::Status::unimplemented("not used in this test"))
    }
}

#[tokio::test]
async fn get_key_from_group_remote_timeout_is_not_silently_treated_as_absent() {
    use raft_rpc::raft_service_server::RaftServiceServer;
    use tokio::net::TcpListener as AsyncTcpListener;

    let addr = pick_addr();
    let listener = AsyncTcpListener::bind(addr).await.unwrap();
    tokio::spawn(async move {
        let _ = aidb::cluster::network::raft_server()
            .add_service(RaftServiceServer::new(SlowGetKeyService))
            .serve_with_incoming(aidb::cluster::network::tcp_incoming(listener))
            .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let follower = setup_follower_node(1, &format!("http://{addr}"));

    let result = follower.get_key_from_group_remote(TARGET_GROUP, b"k").await;
    match result {
        Err(Error::Cluster(ClusterError::Timeout(_))) => {}
        other => panic!("expected Timeout error for a hung leader RPC, got {other:?}"),
    }
}
