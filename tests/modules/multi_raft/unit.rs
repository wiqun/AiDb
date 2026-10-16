//! @component aidb-cluster

use std::collections::HashMap;
use std::sync::Arc;

use aidb::cluster::lifecycle_manager::{LifecycleManager, MetaRaftProvider};
use aidb::cluster::meta_types::{
    default_slot_table, ClusterMeta, GroupMeta, ReplicaInfo, SlotMigrationState, SlotStatus,
    SlotTable, SLOT_COUNT,
};
use aidb::cluster::network::RaftServiceDispatcher;
use aidb::cluster::router::{crc16, extract_hash_tag, key_to_slot, Router};

// ===== CRC16 测试 =====

#[test]
fn test_crc16_computation() {
    let result = crc16(b"123456789");
    assert_eq!(result, 0x31C3, "CRC16-IBM standard vector");
}

#[test]
fn test_crc16_empty() {
    let result = crc16(b"");
    assert_eq!(result, 0x0000, "CRC16 of empty input");
}

#[test]
fn test_crc16_redis_test_vector() {
    let slot = key_to_slot(b"key:test:0000001");
    assert!(slot < 16384);
}

// ===== hash_tag 测试 =====

#[test]
fn test_extract_hash_tag_basic() {
    assert_eq!(extract_hash_tag(b"{user}.name"), b"user");
}

#[test]
fn test_extract_hash_tag_no_braces() {
    assert_eq!(extract_hash_tag(b"username"), b"username");
}

#[test]
fn test_extract_hash_tag_empty_braces() {
    assert_eq!(extract_hash_tag(b"{}.name"), b"{}.name");
}

#[test]
fn test_extract_hash_tag_nested() {
    // first { at index 1, first } after that at index 5 -> result = key[2..5] = "b{c"
    assert_eq!(extract_hash_tag(b"a{b{c}d}e"), b"b{c");
}

#[test]
fn test_extract_hash_tag_only_open() {
    assert_eq!(extract_hash_tag(b"key{missing"), b"key{missing");
}

#[test]
fn test_extract_hash_tag_empty_key() {
    assert_eq!(extract_hash_tag(b""), b"");
}

// ===== key_to_slot 测试 =====

#[test]
fn test_key_to_slot_consistency() {
    let key = b"some-test-key";
    let slot1 = key_to_slot(key);
    let slot2 = key_to_slot(key);
    assert_eq!(slot1, slot2);
}

#[test]
fn test_key_to_slot_hash_tag_effect() {
    let slot1 = key_to_slot(b"{tag}.key1");
    let slot2 = key_to_slot(b"{tag}.key2");
    assert_eq!(slot1, slot2);
}

#[test]
fn test_key_to_slot_different_tags_different_slots() {
    let slot1 = key_to_slot(b"{aaa}");
    let slot2 = key_to_slot(b"{bbb}");
    assert_ne!(slot1, slot2);
}

#[test]
fn test_key_to_slot_empty_key() {
    let slot = key_to_slot(b"");
    assert!(slot < SLOT_COUNT as u16);
}

// ===== Router 辅助 =====
fn test_router() -> Router {
    let mut table = default_slot_table();
    table[0] = SlotStatus::Assigned(1);
    table[1] = SlotStatus::Assigned(2);
    table[100] = SlotStatus::Unallocated;
    table[200] = SlotStatus::Migrating(1);
    let mut group_nodes = HashMap::new();
    group_nodes.insert(1, vec![10, 11]);
    group_nodes.insert(2, vec![20]);
    let mut node_addrs = HashMap::new();
    node_addrs.insert(10, "10.0.0.1:9001".into());
    node_addrs.insert(11, "10.0.0.2:9001".into());
    node_addrs.insert(20, "10.0.0.3:9001".into());
    Router::new(table, group_nodes, node_addrs)
}

#[test]
fn test_router_route_key_assigned() {
    let router = test_router();
    let result = router.route_key(b"");
    assert!(result.is_ok());
    let (gid, status) = result.unwrap();
    assert_eq!(gid, 1);
    assert_eq!(status, SlotStatus::Assigned(1));
}

#[test]
fn test_router_route_slot_unallocated() {
    let router = test_router();
    let result = router.route_slot(100);
    assert!(result.is_err());
}

#[test]
fn test_router_route_slot_migrating() {
    let router = test_router();
    let result = router.route_slot(200);
    assert!(result.is_ok());
    let (gid, status) = result.unwrap();
    assert_eq!(gid, 1);
    assert_eq!(status, SlotStatus::Migrating(1));
}

#[test]
fn test_router_group_slots() {
    let router = test_router();
    let slots = vec![0u16, 1, 0];
    let groups = router.group_slots(slots);
    assert_eq!(groups.len(), 2);
    assert_eq!(groups.get(&1).unwrap().len(), 2);
    assert_eq!(groups.get(&2).unwrap().len(), 1);
}

#[test]
fn test_router_get_group_leader() {
    let router = test_router();
    assert_eq!(router.get_group_leader(1), Some(10));
    assert_eq!(router.get_group_leader(2), Some(20));
    assert_eq!(router.get_group_leader(99), None);
}

#[test]
fn test_router_get_group_nodes() {
    let router = test_router();
    assert_eq!(router.get_group_nodes(1), Some(vec![10, 11]));
    assert_eq!(router.get_group_nodes(99), None);
}

#[test]
fn test_route_slot_out_of_range() {
    let router = test_router();
    let result = router.route_slot(65535);
    assert!(result.is_err());
}

#[test]
fn test_router_get_node_addr() {
    let router = test_router();
    assert_eq!(router.get_node_addr(10), Some("10.0.0.1:9001".to_string()));
    assert_eq!(router.get_node_addr(99), None);
}

// ===== RaftServiceDispatcher 测试 =====

#[test]
fn test_raft_service_dispatcher_empty() {
    let dispatcher = RaftServiceDispatcher::new();
    assert!(dispatcher.get_raft(1).is_none());
    assert_eq!(dispatcher.group_count(), 0);
}

#[test]
fn test_router_refresh() {
    let router = test_router();
    let new_table = {
        let mut t = default_slot_table();
        t[0] = SlotStatus::Assigned(2);
        t
    };
    let mut new_groups = HashMap::new();
    new_groups.insert(2, vec![20, 21]);
    let mut new_addrs = HashMap::new();
    new_addrs.insert(21, "10.0.0.4:9001".into());
    router.refresh_from_data(new_table, new_groups, new_addrs, HashMap::new());
    assert_eq!(router.get_group_leader(1), None);
    assert_eq!(router.get_group_leader(2), Some(20));
    assert_eq!(router.get_node_addr(21), Some("10.0.0.4:9001".to_string()));
}

// ===== ShardedStorage 测试 =====

#[test]
fn test_sharded_storage_open() {
    use aidb::cluster::sharded_storage::ShardedStorage;
    use aidb::config::Options;
    use tempfile::TempDir;

    let dir = TempDir::new().unwrap();
    let storage = ShardedStorage::open(dir.path(), 1, Options::for_testing()).unwrap();
    assert_eq!(storage.group_id(), 1);
    assert!(storage.path().exists());
    storage.close().unwrap();
}

#[test]
fn test_sharded_storage_open_existing() {
    use aidb::cluster::sharded_storage::ShardedStorage;
    use aidb::config::Options;
    use tempfile::TempDir;

    let dir = TempDir::new().unwrap();
    {
        let storage = ShardedStorage::open(dir.path(), 1, Options::for_testing()).unwrap();
        storage.db().put(b"k", b"v").unwrap();
        storage.close().unwrap();
    }
    let storage = ShardedStorage::open(dir.path(), 1, Options::for_testing()).unwrap();
    assert_eq!(storage.db().get(b"k").unwrap(), Some(b"v".to_vec()));
    storage.close().unwrap();
}

#[test]
fn test_sharded_storage_close() {
    use aidb::cluster::sharded_storage::ShardedStorage;
    use aidb::config::Options;
    use tempfile::TempDir;

    let dir = TempDir::new().unwrap();
    let storage = ShardedStorage::open(dir.path(), 1, Options::for_testing()).unwrap();
    storage.close().unwrap();
}

// ===== LifecycleManager tests =====

struct MockMetaRaft {
    cluster_meta: std::sync::Mutex<ClusterMeta>,
    slot_table: std::sync::Mutex<SlotTable>,
}

impl MetaRaftProvider for MockMetaRaft {
    fn get_cluster_meta(&self) -> ClusterMeta {
        self.cluster_meta.lock().unwrap().clone()
    }
    fn get_slot_table(&self) -> SlotTable {
        self.slot_table.lock().unwrap().clone()
    }
    fn get_migration_state(&self) -> Option<SlotMigrationState> {
        None
    }
}

#[test]
fn test_lifecycle_manager_initial_state() {
    let router = Router::new(default_slot_table(), HashMap::new(), HashMap::new());
    let mock = MockMetaRaft {
        cluster_meta: std::sync::Mutex::new(ClusterMeta::default()),
        slot_table: std::sync::Mutex::new(default_slot_table()),
    };
    let manager = LifecycleManager::new(
        1,
        Arc::new(router),
        Arc::new(mock) as Arc<dyn MetaRaftProvider>,
    );
    assert!(manager.local_groups().read().is_empty());
    assert_eq!(manager.node_id(), 1);
}

#[test]
fn test_lifecycle_manager_detect_new_group() {
    let router = Router::new(default_slot_table(), HashMap::new(), HashMap::new());
    let mut cluster_meta = ClusterMeta::default();
    cluster_meta.groups.insert(
        1,
        GroupMeta {
            group_id: 1,
            replicas: vec![ReplicaInfo {
                node_id: 1,
                is_leader: true,
            }],
            slot_ranges: vec![],
            config_version: 0,
        },
    );
    let mock = MockMetaRaft {
        cluster_meta: std::sync::Mutex::new(cluster_meta),
        slot_table: std::sync::Mutex::new(default_slot_table()),
    };
    let manager = LifecycleManager::new(
        1,
        Arc::new(router),
        Arc::new(mock) as Arc<dyn MetaRaftProvider>,
    );
    assert!(manager.local_groups().read().is_empty());
    let tick_result = manager.tick();
    assert!(tick_result.groups_to_create.contains(&1));
    assert!(tick_result.groups_to_remove.is_empty());
    assert!(manager.local_groups().read().contains(&1));
}

#[test]
fn test_lifecycle_manager_remove_group() {
    let router = Router::new(default_slot_table(), HashMap::new(), HashMap::new());
    let mut cluster_meta = ClusterMeta::default();
    cluster_meta.groups.insert(
        1,
        GroupMeta {
            group_id: 1,
            replicas: vec![ReplicaInfo {
                node_id: 1,
                is_leader: true,
            }],
            slot_ranges: vec![],
            config_version: 0,
        },
    );
    let mock = Arc::new(MockMetaRaft {
        cluster_meta: std::sync::Mutex::new(cluster_meta),
        slot_table: std::sync::Mutex::new(default_slot_table()),
    });
    let manager = LifecycleManager::new(
        1,
        Arc::new(router),
        Arc::clone(&mock) as Arc<dyn MetaRaftProvider>,
    );

    // First tick: detect group 1
    let tick_result = manager.tick();
    assert!(tick_result.groups_to_create.contains(&1));
    assert!(tick_result.groups_to_remove.is_empty());
    assert!(manager.local_groups().read().contains(&1));

    // Remove group from MetaRaft
    mock.cluster_meta.lock().unwrap().groups.clear();

    // Second tick: detect removal
    let tick_result = manager.tick();
    assert!(tick_result.groups_to_create.is_empty());
    assert!(tick_result.groups_to_remove.contains(&1));
    assert!(!manager.local_groups().read().contains(&1));
}

#[test]
fn test_lifecycle_manager_no_change() {
    let router = Router::new(default_slot_table(), HashMap::new(), HashMap::new());
    let mut cluster_meta = ClusterMeta::default();
    cluster_meta.groups.insert(
        1,
        GroupMeta {
            group_id: 1,
            replicas: vec![ReplicaInfo {
                node_id: 1,
                is_leader: true,
            }],
            slot_ranges: vec![],
            config_version: 0,
        },
    );
    let mock = MockMetaRaft {
        cluster_meta: std::sync::Mutex::new(cluster_meta),
        slot_table: std::sync::Mutex::new(default_slot_table()),
    };
    let manager = LifecycleManager::new(
        1,
        Arc::new(router),
        Arc::new(mock) as Arc<dyn MetaRaftProvider>,
    );

    manager.tick(); // first tick
    let tick_result = manager.tick(); // second tick — no change
    assert!(tick_result.groups_to_create.is_empty());
    assert!(tick_result.groups_to_remove.is_empty());
}

#[test]
fn test_lifecycle_manager_not_affected_by_other_nodes() {
    let router = Router::new(default_slot_table(), HashMap::new(), HashMap::new());
    let mut cluster_meta = ClusterMeta::default();
    // Group 1 on our node (1), Group 2 on other node (2)
    cluster_meta.groups.insert(
        1,
        GroupMeta {
            group_id: 1,
            replicas: vec![ReplicaInfo {
                node_id: 1,
                is_leader: true,
            }],
            slot_ranges: vec![],
            config_version: 0,
        },
    );
    cluster_meta.groups.insert(
        2,
        GroupMeta {
            group_id: 2,
            replicas: vec![ReplicaInfo {
                node_id: 2,
                is_leader: true,
            }],
            slot_ranges: vec![],
            config_version: 0,
        },
    );
    let mock = MockMetaRaft {
        cluster_meta: std::sync::Mutex::new(cluster_meta),
        slot_table: std::sync::Mutex::new(default_slot_table()),
    };
    let manager = LifecycleManager::new(
        1,
        Arc::new(router),
        Arc::new(mock) as Arc<dyn MetaRaftProvider>,
    );

    let tick_result = manager.tick();
    assert_eq!(tick_result.groups_to_create.len(), 1);
    assert!(tick_result.groups_to_create.contains(&1));
    assert!(tick_result.groups_to_remove.is_empty());
    assert_eq!(manager.local_groups().read().len(), 1);
    assert!(manager.local_groups().read().contains(&1));
}

// ===== MultiRaftNode tests =====

use aidb::cluster::multi_raft_node::MultiRaftNode;
use aidb::cluster::types::{ClusterError, Request};
use aidb::error::Error;

#[test]
fn test_multi_raft_node_init() {
    let router = Arc::new(Router::new(
        default_slot_table(),
        HashMap::new(),
        HashMap::new(),
    ));
    let dispatcher = Arc::new(RaftServiceDispatcher::new());
    let node = MultiRaftNode::new(1, router, dispatcher);
    assert!(!node.is_group_local(1));
    assert_eq!(node.node_id(), 1);
}

#[test]
fn last_local_user_pair_sync_missing_group() {
    let router = Arc::new(Router::new(
        default_slot_table(),
        HashMap::new(),
        HashMap::new(),
    ));
    let dispatcher = Arc::new(RaftServiceDispatcher::new());
    let node = MultiRaftNode::new(1, router, dispatcher);
    let err = node
        .last_local_user_pair_sync(1, b"", b"")
        .expect_err("missing group");
    assert!(
        err.to_string().contains("not found locally"),
        "{err}"
    );
    let err = node
        .scan_local_pairs_in_range_sync(1, b"", None)
        .expect_err("missing group");
    assert!(
        err.to_string().contains("not found locally"),
        "{err}"
    );
}

#[test]
fn test_multi_raft_node_new_with_lifecycle() {
    let router = Arc::new(Router::new(
        default_slot_table(),
        HashMap::new(),
        HashMap::new(),
    ));
    let dispatcher = Arc::new(RaftServiceDispatcher::new());
    let mock = MockMetaRaft {
        cluster_meta: std::sync::Mutex::new(ClusterMeta::default()),
        slot_table: std::sync::Mutex::new(default_slot_table()),
    };
    let manager = LifecycleManager::new(
        1,
        Arc::clone(&router),
        Arc::new(mock) as Arc<dyn MetaRaftProvider>,
    );
    let node = MultiRaftNode::new_with_lifecycle(1, router, dispatcher, manager);
    assert_eq!(node.node_id(), 1);
    assert!(!node.is_group_local(1));
}

#[tokio::test]
async fn test_multi_raft_node_propose_group_not_found() {
    let router = Arc::new(Router::new(
        default_slot_table(),
        HashMap::new(),
        HashMap::new(),
    ));
    let dispatcher = Arc::new(RaftServiceDispatcher::new());
    let node = MultiRaftNode::new(1, router, dispatcher);
    let result = node
        .propose_group(
            99,
            Request::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
        )
        .await;
    assert!(result.is_err());
    assert!(matches!(result, Err(Error::Cluster(ClusterError::Raft(_)))));
}

#[tokio::test]
async fn test_multi_raft_node_propose_key_not_found() {
    let mut table = default_slot_table();
    for status in table.iter_mut() {
        *status = SlotStatus::Assigned(1);
    }
    let router = Arc::new(Router::new(table, HashMap::new(), HashMap::new()));
    let dispatcher = Arc::new(RaftServiceDispatcher::new());
    let node = MultiRaftNode::new(1, router, dispatcher);
    let result = node
        .propose_key(b"somekey".to_vec(), Some(b"value".to_vec()))
        .await;
    assert!(result.is_err());
    assert!(matches!(result, Err(Error::Cluster(ClusterError::Raft(_)))));
}

#[tokio::test]
async fn test_multi_raft_node_get_key_not_found() {
    let mut table = default_slot_table();
    for status in table.iter_mut() {
        *status = SlotStatus::Assigned(1);
    }
    let router = Arc::new(Router::new(table, HashMap::new(), HashMap::new()));
    let dispatcher = Arc::new(RaftServiceDispatcher::new());
    let node = MultiRaftNode::new(1, router, dispatcher);
    let result = node.get_key(b"somekey".to_vec()).await;
    assert!(result.is_err());
    assert!(matches!(result, Err(Error::Cluster(ClusterError::Raft(_)))));
}

#[test]
fn test_multi_raft_node_router_and_dispatcher() {
    let router = Arc::new(Router::new(
        default_slot_table(),
        HashMap::new(),
        HashMap::new(),
    ));
    let dispatcher = Arc::new(RaftServiceDispatcher::new());
    let node = MultiRaftNode::new(1, Arc::clone(&router), Arc::clone(&dispatcher));
    assert!(Arc::ptr_eq(&router, node.router()));
    assert!(Arc::ptr_eq(&dispatcher, node.grpc_dispatcher()));
}

#[test]
fn test_multi_raft_node_get_groups_and_storages_empty() {
    let router = Arc::new(Router::new(
        default_slot_table(),
        HashMap::new(),
        HashMap::new(),
    ));
    let dispatcher = Arc::new(RaftServiceDispatcher::new());
    let node = MultiRaftNode::new(1, router, dispatcher);
    assert!(node.get_groups().read().is_empty());
    assert!(node.get_storages().read().is_empty());
}
