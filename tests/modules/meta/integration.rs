//! @component aidb-cluster
//! Slot 迁移行为的集成回归见 `tests/modules/cluster/slot_migration.rs` (`--test raft`),
//! 本文件不重复占位测.

use std::collections::HashMap;
use std::time::Duration;

use aidb::cluster::{MetaRequest, Response};

use super::harness::MetaClusterTestHarness;

#[tokio::test]
async fn test_meta_raft_initialize() {
    let h = MetaClusterTestHarness::new_3node().await;
    h.bootstrap().await;
    assert!(h.wait_leader().await.is_some());
    h.shutdown_all().await;
}

#[tokio::test]
async fn test_meta_raft_register_node_across_cluster() {
    let h = MetaClusterTestHarness::new_3node().await;
    h.bootstrap().await;
    let leader = h.leader().await;
    match leader
        .propose(MetaRequest::RegisterNode {
            node_id: 10,
            rpc_addr: "http://127.0.0.1:9010".into(),
            client_addr: None,
            tags: HashMap::new(),
        })
        .await
        .unwrap()
    {
        Response::Ok => {}
        other => panic!("unexpected response: {other:?}"),
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    for node in &h.nodes {
        assert!(node.get_cluster_meta().nodes.contains_key(&10));
    }
    h.shutdown_all().await;
}

#[tokio::test]
async fn test_meta_raft_initialize_twice() {
    let h = MetaClusterTestHarness::new_3node().await;
    h.bootstrap().await;
    h.nodes[0]
        .initialize(vec![(1, "http://127.0.0.1:1".into())])
        .await
        .unwrap();
    h.shutdown_all().await;
}

#[tokio::test]
async fn test_meta_raft_leader_failover() {
    let h = MetaClusterTestHarness::new_3node().await;
    h.bootstrap().await;
    let leader = h.leader().await;
    leader
        .propose(MetaRequest::RegisterNode {
            node_id: 5,
            rpc_addr: "http://127.0.0.1:9005".into(),
            client_addr: None,
            tags: HashMap::new(),
        })
        .await
        .unwrap();
    let leader_id = leader.node_id();
    leader.shutdown().await.unwrap();

    let mut new_leader_id = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        for node in &h.nodes {
            if node.node_id() != leader_id
                && node.is_leader().await
                && node.get_cluster_meta().nodes.contains_key(&5)
            {
                new_leader_id = Some(node.node_id());
                break;
            }
        }
        if new_leader_id.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        new_leader_id.is_some(),
        "expected a new leader among remaining nodes within 10s"
    );
    h.shutdown_all().await;
}

#[tokio::test]
async fn test_meta_raft_recovery() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("meta_raft_recovery_test");
    let _db_path = path.clone();

    // Phase 1: bootstrap and propose
    {
        use aidb::cluster::{
            MetaRaftNode, MetaRequest, RaftNetworkClientFactory, RaftNodeConfig, METARAFT_GROUP_ID,
        };
        use aidb::config::Options;
        use aidb::DB;

        let db = DB::open(&path, Options::for_testing()).unwrap();
        let factory = RaftNetworkClientFactory::new(1, METARAFT_GROUP_ID, 30, 65 * 1024 * 1024);
        let cfg = RaftNodeConfig {
            node_id: 1,
            group_id: METARAFT_GROUP_ID,
            election_timeout_min: 200,
            election_timeout_max: 400,
            heartbeat_interval: 50,
            rpc_timeout_ms: 30,
            snapshot_logs_since_last: 50,
            ..Default::default()
        };
        let node = MetaRaftNode::new(cfg, db, factory).await.unwrap();
        node.initialize(vec![(1, "http://127.0.0.1:1".into())])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;

        node.propose(MetaRequest::RegisterNode {
            node_id: 42,
            rpc_addr: "http://127.0.0.1:9042".into(),
            client_addr: None,
            tags: HashMap::new(),
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(800)).await;
        assert!(node.get_cluster_meta().nodes.contains_key(&42));
        node.shutdown().await.unwrap();
    }

    // Phase 2: reopen same DB and verify state recovered
    {
        use aidb::cluster::{
            MetaRaftNode, RaftNetworkClientFactory, RaftNodeConfig, METARAFT_GROUP_ID,
        };
        use aidb::config::Options;
        use aidb::DB;

        let db = DB::open(&path, Options::for_testing()).unwrap();
        let factory = RaftNetworkClientFactory::new(1, METARAFT_GROUP_ID, 30, 65 * 1024 * 1024);
        let cfg = RaftNodeConfig {
            node_id: 1,
            group_id: METARAFT_GROUP_ID,
            election_timeout_min: 200,
            election_timeout_max: 400,
            heartbeat_interval: 50,
            rpc_timeout_ms: 30,
            ..Default::default()
        };
        let node = MetaRaftNode::new(cfg, db, factory).await.unwrap();
        // After restart, ClusterMeta should be reloaded from DB
        assert!(node.get_cluster_meta().nodes.contains_key(&42));
        assert_eq!(node.get_cluster_meta().cluster_id, "uninitialized");
        node.shutdown().await.unwrap();
    }
}
