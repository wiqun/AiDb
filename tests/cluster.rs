//! 集群 Raft / MultiRaft / MetaRaft / 副本对账集成测试统一入口
//! @component aidb-cluster
#![cfg(feature = "cluster")]

#[path = "modules/cluster/mod.rs"]
mod cluster;

#[path = "modules/cluster_ops/mod.rs"]
mod cluster_ops;

#[path = "modules/cluster/replica_reconcile.rs"]
mod replica_reconcile;

#[path = "modules/meta/mod.rs"]
mod meta;

#[path = "modules/multi_raft/mod.rs"]
mod multi_raft;
