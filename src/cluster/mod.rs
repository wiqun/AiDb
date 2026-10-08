//! AiDb 分布式集群 (Phase 12: 单 Raft Group; Phase 13: MetaRaft).

pub mod contact;
#[cfg(feature = "cluster-test-util")]
pub mod failpoint;
pub mod leader_watcher;
pub mod lifecycle_manager;
pub mod log_committer;
pub mod membership_coordinator;
pub mod meta_raft_node;
pub mod meta_state_machine;
pub mod meta_types;
pub mod migration_oplog;
pub mod multi_raft_node;
pub mod network;
pub mod node;
pub mod pending_log;
pub mod replica_allocator;
pub mod router;
pub mod sharded_storage;
pub mod slot_migration;
pub mod storage;
pub mod types;

#[cfg(feature = "cluster-test-util")]
pub use failpoint::{registry as failpoint_registry, FailPoint};
pub use leader_watcher::LeaderChangeWatcher;
pub use lifecycle_manager::{LifecycleManager, MembershipDrift, TickResult};
pub use membership_coordinator::MembershipCoordinator;
pub use meta_raft_node::MetaRaftNode;
pub use meta_state_machine::{ApplyOutput, MetaStateMachine};
pub use meta_types::{
    default_slot_table, ClusterMeta, GroupMeta, MetaRequest, NodeInfo, NodeRole, NodeStatus,
    ReplicaInfo, SlotMigrationState, SlotStatus, SlotTable, METARAFT_GROUP_ID, SLOT_COUNT,
};
pub use migration_oplog::{decode_tip, decode_tombstone, encode_tip, encode_tombstone, MigOp};
pub use multi_raft_node::{LifecycleConfig, MultiRaftNode};
pub use network::{
    RaftNetworkClient, RaftNetworkClientFactory, RaftServiceDispatcher, RaftServiceImpl,
};
pub use node::OpenRaftNode;
pub use replica_allocator::ReplicaAllocator;
pub use router::{crc16, extract_hash_tag, key_to_slot, Router};
pub use sharded_storage::{AggregateStats, ShardedStorage, StorageStats};
pub use slot_migration::{
    MigrationPhase, MigrationProgress, SlotMigrationExecutor, SlotMigrationManager,
};
pub use storage::{OpenRaftStorage, DEFAULT_GROUP_ID};
pub use types::{
    ClusterError, LogEntry, NodeId, RaftNodeConfig, Request, Response, ThinWriteBatch, ThinWriteOp,
    TypeConfig,
};
