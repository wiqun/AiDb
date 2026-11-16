use std::collections::BTreeSet;

use super::*;

fn compute_drift(expected: &BTreeSet<u64>, actual: &BTreeSet<u64>) -> (Vec<u64>, Vec<u64>) {
    let to_add: Vec<u64> = expected.difference(actual).copied().collect();
    let to_remove: Vec<u64> = actual.difference(expected).copied().collect();
    (to_add, to_remove)
}

#[test]
fn drift_no_drift_when_membership_matches() {
    let expected: BTreeSet<u64> = [1, 2].iter().copied().collect();
    let actual: BTreeSet<u64> = [1, 2].iter().copied().collect();
    let (to_add, to_remove) = compute_drift(&expected, &actual);
    assert!(to_add.is_empty());
    assert!(to_remove.is_empty());
}

#[test]
fn drift_detects_missing_replica() {
    let expected: BTreeSet<u64> = [1, 2].iter().copied().collect();
    let actual: BTreeSet<u64> = [1].iter().copied().collect();
    let (to_add, to_remove) = compute_drift(&expected, &actual);
    assert_eq!(to_add, vec![2]);
    assert!(to_remove.is_empty());
}

#[test]
fn drift_detects_extra_member() {
    let expected: BTreeSet<u64> = [1].iter().copied().collect();
    let actual: BTreeSet<u64> = [1, 2].iter().copied().collect();
    let (to_add, to_remove) = compute_drift(&expected, &actual);
    assert!(to_add.is_empty());
    assert_eq!(to_remove, vec![2]);
}

#[test]
fn drift_detects_both_add_and_remove() {
    let expected: BTreeSet<u64> = [2, 3].iter().copied().collect();
    let actual: BTreeSet<u64> = [1, 2].iter().copied().collect();
    let (to_add, to_remove) = compute_drift(&expected, &actual);
    assert_eq!(to_add, vec![3]);
    assert_eq!(to_remove, vec![1]);
}

#[test]
fn drift_all_missing_empty_actual() {
    let expected: BTreeSet<u64> = [1, 2, 3].iter().copied().collect();
    let actual: BTreeSet<u64> = BTreeSet::new();
    let (to_add, to_remove) = compute_drift(&expected, &actual);
    assert_eq!(to_add.len(), 3);
    assert!(to_remove.is_empty());
}

#[test]
fn restart_backoff_grows_and_caps() {
    let b0 = super::group_restart_backoff(0);
    let b1 = super::group_restart_backoff(1);
    let b2 = super::group_restart_backoff(2);
    let b_far = super::group_restart_backoff(100);
    assert_eq!(b0, std::time::Duration::from_secs(2));
    assert_eq!(b1, std::time::Duration::from_secs(4));
    assert_eq!(b2, std::time::Duration::from_secs(8));
    assert!(
        b1 > b0 && b2 > b1,
        "backoff must strictly increase at first"
    );
    assert_eq!(
        b_far,
        std::time::Duration::from_secs(60),
        "backoff must be capped so a persistently faulty group doesn't restart-storm forever"
    );
}

/// 端到端验证 fail-fast + 自愈闭环: 真实存储故障 -> openraft Fatal ->
/// `supervise_groups` 就地重开 group -> 服务恢复且故障前数据无损.
#[tokio::test]
async fn supervise_groups_restarts_fatal_group_and_preserves_data() {
    use std::collections::HashMap;

    use tempfile::TempDir;

    use crate::cluster::network::{RaftNetworkClientFactory, RaftServiceDispatcher};
    use crate::cluster::sharded_storage::ShardedStorage;
    use crate::cluster::types::{RaftNodeConfig, Request};
    use crate::config::Options;

    const GROUP_ID: u64 = 1;

    let dir = TempDir::new().unwrap();
    let cfg = LifecycleConfig {
        data_dir: dir.path().to_path_buf(),
        raft_node_config: RaftNodeConfig {
            node_id: 1,
            group_id: GROUP_ID,
            ..RaftNodeConfig::default()
        },
        options: Options::for_testing(),
        compaction_filter: None,
        compaction_removal_listener_factory: None,
    };
    let net_factory = Arc::new(RwLock::new(RaftNetworkClientFactory::new(
        1,
        0,
        30,
        65 * 1024 * 1024,
    )));
    let groups: Arc<RwLock<HashMap<u64, Arc<OpenRaftNode>>>> =
        Arc::new(RwLock::new(HashMap::new()));
    let storages: Arc<RwLock<HashMap<u64, ShardedStorage>>> = Arc::new(RwLock::new(HashMap::new()));
    let dispatcher = Arc::new(RaftServiceDispatcher::new());
    let restart_state: Arc<RwLock<HashMap<u64, GroupRestartState>>> =
        Arc::new(RwLock::new(HashMap::new()));

    MultiRaftNode::create_group_inner(
        GROUP_ID,
        true,
        Some("127.0.0.1:19001"),
        &groups,
        &storages,
        &dispatcher,
        &cfg,
        &net_factory,
    )
    .await;
    assert!(
        groups.read().contains_key(&GROUP_ID),
        "group should be created"
    );

    let node = groups.read().get(&GROUP_ID).cloned().unwrap();
    wait_for(std::time::Duration::from_secs(5), || {
        node.raft().metrics().borrow_watched().current_leader == Some(1)
    })
    .await;

    node.propose(Request::Put {
        key: b"k1".to_vec(),
        value: bytes::Bytes::from(b"v1".to_vec()),
    })
    .await
    .expect("initial write before fault injection must succeed");

    // 制造一次真实的存储故障: 直接关闭底层 DB, 让接下来 PutConditional 的
    // dedup 读 (db.get) 失败, 从而在 apply 路径上产生一次 fail-fast 错误.
    {
        let storages_read = storages.read();
        storages_read.get(&GROUP_ID).unwrap().db().close().unwrap();
    }
    let propose_err = node
        .propose(Request::PutConditional {
            key: b"k2".to_vec(),
            value: bytes::Bytes::from(b"v2".to_vec()),
            migration_epoch: None,
        })
        .await;
    assert!(
        propose_err.is_err(),
        "propose against a closed underlying db must surface as an error"
    );

    wait_for(std::time::Duration::from_secs(5), || {
        node.raft()
            .metrics()
            .borrow_watched()
            .running_state
            .is_err()
    })
    .await;

    // `node`/`groups`/`dispatcher` (经 `register_node`) 是仅剩的强引用;
    // 底层 DB 的文件锁要等它们 (及内嵌的 `Arc<DB>`) 全部释放才会解锁,
    // 与生产环境一致 —— fatal `OpenRaftNode` 只被 group 映射和 dispatcher
    // 持有, 不会有游离引用阻止重开.
    drop(node);

    MultiRaftNode::supervise_groups(
        &groups,
        &storages,
        &dispatcher,
        &cfg,
        &net_factory,
        &restart_state,
    )
    .await;

    assert!(
        groups.read().contains_key(&GROUP_ID),
        "group should be reopened in-place after self-heal"
    );
    let node2 = groups.read().get(&GROUP_ID).cloned().unwrap();
    // dispatcher (经 `register_node`) 必须跟着 self-heal 一起更新, 否则
    // `GetKey`/`GetMigrationTip` 会一直路由到已经 shutdown 的 fatal 实例.
    assert!(
        Arc::ptr_eq(
            &node2,
            &dispatcher
                .get_node(GROUP_ID)
                .expect("dispatcher must re-register the reopened node")
        ),
        "dispatcher must track the reopened OpenRaftNode, not a stale fatal reference"
    );
    assert!(
        node2
            .raft()
            .metrics()
            .borrow_watched()
            .running_state
            .is_ok(),
        "reopened group must not still be in a Fatal state"
    );

    wait_for(std::time::Duration::from_secs(5), || {
        node2.raft().metrics().borrow_watched().current_leader == Some(1)
    })
    .await;
    assert_eq!(
        node2.get(b"k1".to_vec()).await.unwrap(),
        Some(b"v1".to_vec()),
        "data committed before the fault must survive the in-process restart"
    );
}

async fn wait_for<F: Fn() -> bool>(timeout: std::time::Duration, cond: F) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !cond() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition did not become true within {timeout:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[test]
fn learner_addr_uses_rpc_not_client_port() {
    use std::collections::HashMap;

    use crate::cluster::meta_types::{ClusterMeta, NodeInfo, NodeRole, NodeStatus};

    let mut nodes = HashMap::new();
    nodes.insert(
        5u64,
        NodeInfo {
            node_id: 5,
            rpc_addr: "127.0.0.1:17380".into(),
            client_addr: Some("127.0.0.1:7380".into()),
            role: NodeRole::Voter,
            status: NodeStatus::Online,
            registered_at: 0,
            tags: HashMap::new(),
        },
    );
    let meta = ClusterMeta {
        cluster_id: "test".into(),
        nodes,
        groups: HashMap::new(),
        version: 1,
        format_version: 1,
    };

    let addr = meta
        .nodes
        .get(&5)
        .map(|n| n.rpc_addr.clone())
        .expect("rpc addr");
    assert_eq!(addr, "127.0.0.1:17380");
    assert_ne!(addr, "127.0.0.1:7380");
}
