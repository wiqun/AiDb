//! ISSUE-001 / ISSUE-002 — WriteBatch 与 WAL rotate 边界 (B1.2 模板)
//! @component aidb-wal
//!

use aidb::config::Options;
use aidb::engine::wal::manager::WALManager;
use aidb::engine::wal::record::OpType;
use aidb::{WriteBatch, DB};
use std::path::Path;
use tempfile::tempdir;

const BATCH_SIZE: usize = 8;

fn wal_opts(max_wal_size: u64) -> Options {
    let mut o = Options::for_testing();
    o.max_wal_size = max_wal_size;
    o.sync_wal = true;
    o
}

fn count_wal_log_files(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .filter(|e| {
            let name = e.file_name();
            let s = name.to_string_lossy();
            s.starts_with("wal_") && s.ends_with(".log")
        })
        .count()
}

fn write_test_batch(db: &DB, batch_size: usize, value: &[u8]) {
    let mut batch = WriteBatch::new();
    for i in 0..batch_size {
        batch.put(format!("k{i}").as_bytes(), value);
    }
    db.write(&batch).unwrap();
}

fn count_recovered_puts(result: &aidb::engine::wal::manager::RecoveryResult) -> usize {
    result
        .entries
        .iter()
        .filter(|e| e.op_type == OpType::TypePut)
        .count()
}

/// 标定: 单条 put batch 落盘后 wal_1.log 大小, 用于选取 max_wal_size.
fn probe_one_put_on_disk_size() -> u64 {
    let d = tempdir().unwrap();
    let db = DB::open(d.path(), wal_opts(0)).unwrap();
    write_test_batch(&db, 1, b"v");
    drop(db);
    std::fs::metadata(d.path().join("wal_1.log")).unwrap().len()
}

fn max_wal_size_for_mid_batch_rotate() -> u64 {
    probe_one_put_on_disk_size() + 20
}

/// ISSUE-001: batch 不应跨 wal_*.log.
#[test]
fn test_write_batch_stays_in_single_wal_file() {
    let max_wal_size = max_wal_size_for_mid_batch_rotate();
    let d = tempdir().unwrap();
    {
        let db = DB::open(d.path(), wal_opts(max_wal_size)).unwrap();
        write_test_batch(&db, BATCH_SIZE, b"v");
        drop(db);
    }

    let wal_count = count_wal_log_files(d.path());
    assert_eq!(
        wal_count, 1,
        "WriteBatch must not span WAL files (inventory), got {wal_count} files"
    );
}

/// ISSUE-001: 完整 batch recover 应全部 replay.
#[test]
fn test_recover_write_batch_all_or_nothing() {
    let max_wal_size = max_wal_size_for_mid_batch_rotate();
    let d = tempdir().unwrap();
    {
        let db = DB::open(d.path(), wal_opts(max_wal_size)).unwrap();
        write_test_batch(&db, BATCH_SIZE, b"v");
        drop(db);
    }

    assert_eq!(count_wal_log_files(d.path()), 1);

    let recovered =
        WALManager::recover(d.path(), std::sync::Arc::new(wal_opts(max_wal_size))).unwrap();
    assert_eq!(count_recovered_puts(&recovered), BATCH_SIZE);
}

/// ISSUE-002: batch 大于 max_wal_size 时单文件临时超限, 不 mid-batch rotate.
#[test]
fn test_large_batch_exceeds_max_wal_size_no_mid_batch_rotate() {
    let value = vec![b'x'; 120];
    let max_wal_size = 400;

    let d = tempdir().unwrap();
    {
        let db = DB::open(d.path(), wal_opts(max_wal_size)).unwrap();
        write_test_batch(&db, 5, &value);
        drop(db);
    }

    assert_eq!(
        count_wal_log_files(d.path()),
        1,
        "inventory expects no mid-batch rotate (single WAL file)"
    );
    let wal1_size = std::fs::metadata(d.path().join("wal_1.log")).unwrap().len();
    assert!(
        wal1_size > max_wal_size,
        "batch total should exceed max_wal_size={max_wal_size}, wal_1.log={wal1_size}"
    );
}
