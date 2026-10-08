use std::sync::atomic::Ordering;
use tempfile::TempDir;

use aidb::config::Options;
use aidb::statistics::{DbOp, WriteStallKind};
use aidb::DB;

#[test]
fn test_write_stall_kind_all_and_metadata() {
    assert_eq!(WriteStallKind::ALL.len(), 6);
    for kind in &WriteStallKind::ALL {
        let (cause, stall_type) = kind.cause_and_type();
        assert!(!cause.is_empty());
        assert!(stall_type == "slowdown" || stall_type == "stop");
    }
}

#[test]
fn test_write_stall_l0_metrics() {
    let dir = TempDir::new().unwrap();
    let opts = Options {
        background_compaction: true,
        memtable_size: 256,
        level0_compaction_trigger: 3,
        level0_slowdown_writes_trigger: 1,
        level0_stop_writes_trigger: 3,
        write_stall_poll_ms: 5,
        write_stall_slowdown_max_ms: 10,
        ..Default::default()
    };
    let db = DB::open(dir.path(), opts).unwrap();
    let stats = db.statistics();

    for batch in 0..15u64 {
        for i in 0..10u64 {
            db.put(format!("k{batch}_{i}").as_bytes(), b"val").unwrap();
        }
        db.flush().unwrap();
    }
    db.drain_compactions().unwrap();
    let slowdown_req = stats.write_stall_requests[WriteStallKind::L0FilesSlowdown as usize]
        .load(Ordering::Relaxed);
    let stop_req =
        stats.write_stall_requests[WriteStallKind::L0FilesStop as usize].load(Ordering::Relaxed);
    assert!(
        slowdown_req > 0 || stop_req > 0,
        "Expected L0 slowdown or stop requests > 0, got slowdown={slowdown_req}, stop={stop_req}"
    );

    let max_dur = stats.write_stall_max_duration_us.load(Ordering::Relaxed);
    assert!(max_dur > 0, "write_stall_max_duration_us should be > 0");

    if slowdown_req > 0 {
        assert_eq!(
            stats.operations[DbOp::StallSlowdown as usize].load(Ordering::Relaxed),
            0,
            "DbOp stall counters stay at zero"
        );
        let hist_count = stats.write_stall_durations[WriteStallKind::L0FilesSlowdown as usize]
            .count
            .load(Ordering::Relaxed);
        assert_eq!(hist_count, slowdown_req);
    }
    if stop_req > 0 {
        assert_eq!(
            stats.operations[DbOp::StallStop as usize].load(Ordering::Relaxed),
            0,
            "DbOp stall counters stay at zero"
        );
        let hist_count = stats.write_stall_durations[WriteStallKind::L0FilesStop as usize]
            .count
            .load(Ordering::Relaxed);
        assert_eq!(hist_count, stop_req);
    }
}
