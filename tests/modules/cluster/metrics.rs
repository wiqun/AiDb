//! @component aidb-cluster

use std::sync::atomic::Ordering;

use aidb::metrics::{self, testutil};
use aidb::statistics::{RaftRpcDirection, RaftRpcType, Statistics};

#[test]
fn test_raft_metrics_otel_record() {
    let exporter = testutil::init_in_memory();
    let rpc_before = testutil::counter_sum(&exporter, "aidb_raft_rpc_total");
    let logs_before = testutil::counter_sum(&exporter, "aidb_raft_log_entries_total");

    let stats = Statistics::default();
    stats.raft_rpc[RaftRpcType::Vote as usize][RaftRpcDirection::Incoming as usize]
        .fetch_add(1, Ordering::Relaxed);
    stats.raft_log_entries.fetch_add(3, Ordering::Relaxed);

    metrics::sync_to_otel(&stats);

    assert_eq!(
        testutil::counter_sum(&exporter, "aidb_raft_rpc_total") - rpc_before,
        1,
        "vote incoming"
    );
    assert_eq!(
        testutil::counter_sum(&exporter, "aidb_raft_log_entries_total") - logs_before,
        3
    );
}
