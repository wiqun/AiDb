//! 集群 Raft 模块无锁原子指标 (Atomic-First Statistics) 集成测试.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use openraft::network::v2::RaftNetworkV2;
use openraft::network::RPCOption;
use openraft::raft::{AppendEntriesRequest, VoteRequest};
use openraft::RaftNetworkFactory;

use aidb::cluster::meta_types::default_slot_table;
use aidb::cluster::network::raft_rpc::raft_service_server::RaftService;
use aidb::cluster::network::{
    raft_rpc, RaftNetworkClientFactory, RaftServiceDispatcher, RaftServiceImpl,
};
use aidb::cluster::types::RaftNodeConfig;
use aidb::cluster::{LifecycleConfig, MultiRaftNode, Router};
use aidb::config::Options;
use aidb::statistics::{RaftRpcDirection, RaftRpcType, Statistics};

#[tokio::test]
async fn test_network_client_atomic_stats() {
    let stats = Arc::new(Statistics::default());
    let mut factory = RaftNetworkClientFactory::new(1, 1, 100, 1024).with_stats(Arc::clone(&stats));

    let node = openraft::BasicNode {
        addr: "127.0.0.1:1".into(),
    };
    let mut client = factory.new_client(2, &node).await;

    let ae_req = AppendEntriesRequest {
        vote: openraft::Vote::new_committed(1, 1),
        prev_log_id: None,
        entries: Vec::new(),
        leader_commit: None,
    };

    // 尝试发送 RPC (目标不可达预期失败)
    let _ = client
        .append_entries(ae_req, RPCOption::new(Duration::from_millis(50)))
        .await;

    // 验证 Outgoing RPC 计数在发送尝试前已自增 (尝试数语义)
    assert_eq!(
        stats.raft_rpc[RaftRpcType::AppendEntries as usize][RaftRpcDirection::Outgoing as usize]
            .load(Ordering::Relaxed),
        1,
        "append_entries outgoing RPC attempt should be recorded"
    );

    let vote_req = VoteRequest {
        vote: openraft::Vote::new(1, 1),
        last_log_id: None,
        leadership_transfer: false,
    };

    let _ = client
        .vote(vote_req, RPCOption::new(Duration::from_millis(50)))
        .await;

    assert_eq!(
        stats.raft_rpc[RaftRpcType::Vote as usize][RaftRpcDirection::Outgoing as usize]
            .load(Ordering::Relaxed),
        1,
        "vote outgoing RPC attempt should be recorded"
    );
}

#[tokio::test]
async fn test_service_dispatcher_and_server_atomic_stats() {
    let stats = Arc::new(Statistics::default());
    let dispatcher = Arc::new(RaftServiceDispatcher::new().with_stats(Arc::clone(&stats)));
    let service = RaftServiceImpl::new(dispatcher);

    // 1. Vote incoming
    let vote_req = tonic::Request::new(raft_rpc::VoteRequest {
        group_id: 1,
        vote_term: 1,
        vote_node_id: 2,
        vote_committed: false,
        last_log_index: 0,
        last_log_term: 0,
        last_log_leader_id: 0,
    });
    let _ = service.vote(vote_req).await;
    assert_eq!(
        stats.raft_rpc[RaftRpcType::Vote as usize][RaftRpcDirection::Incoming as usize]
            .load(Ordering::Relaxed),
        1,
        "vote incoming RPC should be recorded"
    );

    // 2. AppendEntries incoming (3 entries)
    let dummy_entries = vec![
        raft_rpc::LogEntry {
            log_index: 1,
            log_term: 1,
            log_leader_id: 1,
            payload: vec![],
            is_blank: true,
            is_membership: false,
        },
        raft_rpc::LogEntry {
            log_index: 2,
            log_term: 1,
            log_leader_id: 1,
            payload: vec![],
            is_blank: true,
            is_membership: false,
        },
        raft_rpc::LogEntry {
            log_index: 3,
            log_term: 1,
            log_leader_id: 1,
            payload: vec![],
            is_blank: true,
            is_membership: false,
        },
    ];
    let ae_req = tonic::Request::new(raft_rpc::AppendEntriesRequest {
        group_id: 1,
        vote_term: 1,
        vote_node_id: 1,
        vote_committed: true,
        prev_log_index: Some(0),
        prev_log_term: Some(0),
        prev_log_leader_id: Some(0),
        entries: dummy_entries,
        leader_commit_index: Some(0),
        leader_commit_term: Some(0),
        leader_commit_leader_id: Some(0),
    });
    let _ = service.append_entries(ae_req).await;
    assert_eq!(
        stats.raft_rpc[RaftRpcType::AppendEntries as usize][RaftRpcDirection::Incoming as usize]
            .load(Ordering::Relaxed),
        1,
        "append_entries incoming RPC should be recorded"
    );
    assert_eq!(
        stats.raft_log_entries.load(Ordering::Relaxed),
        3,
        "raft_log_entries should be incremented by entries count"
    );

    // 3. InstallSnapshot incoming
    let snap_req = tonic::Request::new(raft_rpc::InstallSnapshotRequest {
        group_id: 1,
        vote_term: 1,
        vote_node_id: 1,
        vote_committed: true,
        meta: Some(raft_rpc::SnapshotMeta {
            last_log_index: Some(10),
            last_log_term: Some(1),
            last_log_leader_id: Some(1),
            last_membership: vec![],
            snapshot_id: "snap-1".into(),
        }),
        snapshot_data: vec![],
    });
    let _ = service.install_snapshot(snap_req).await;
    assert_eq!(
        stats.raft_rpc[RaftRpcType::InstallSnapshot as usize][RaftRpcDirection::Incoming as usize]
            .load(Ordering::Relaxed),
        1,
        "install_snapshot incoming RPC should be recorded"
    );
}

#[test]
fn test_multi_raft_node_shared_stats_propagation() {
    let stats = Arc::new(Statistics::default());
    let dispatcher = Arc::new(RaftServiceDispatcher::new().with_stats(Arc::clone(&stats)));
    let router = Arc::new(Router::new(
        default_slot_table(),
        HashMap::new(),
        HashMap::new(),
    ));
    let node = MultiRaftNode::new(1, router, dispatcher);

    assert!(
        Arc::ptr_eq(&node.statistics(), &stats),
        "MultiRaftNode must inherit and share the dispatcher's Statistics instance"
    );

    let dir = tempfile::tempdir().unwrap();
    let mut cfg = LifecycleConfig {
        data_dir: dir.path().to_path_buf(),
        raft_node_config: RaftNodeConfig::default(),
        options: Options::default(),
        compaction_filter: None,
        compaction_removal_listener_factory: None,
    };
    assert!(cfg.options.statistics.is_none());

    // 验证 options 注入逻辑保证 cfg 挂载共享 stats
    node.inject_shared_statistics(&mut cfg);
    assert!(cfg.options.statistics.is_some());
    assert!(Arc::ptr_eq(
        cfg.options.statistics.as_ref().unwrap(),
        &stats
    ));
}
