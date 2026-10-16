//! ISSUE-001 — DB::write + WriteBatch 在 WAL rotate 下的崩溃恢复原子性 (L2).
//! @component aidb-engine

use aidb::config::Options;
use aidb::{WriteBatch, DB};
use std::path::Path;
use tempfile::tempdir;

const BATCH_SIZE: usize = 8;

fn wal_rotate_opts(max_wal_size: u64) -> Options {
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

/// 与 L1 `write_batch_boundary` 同思路: 小 max_wal_size 迫使 mid-batch rotate.
fn max_wal_size_for_mid_batch_rotate() -> u64 {
    use aidb::engine::wal::manager::WALManager;
    use aidb::engine::wal::record::{OpType, WalEntry};
    use std::sync::Arc;

    let d = tempdir().unwrap();
    let mut o = Options::for_testing();
    o.max_wal_size = 0;
    o.sync_wal = true;
    let opts = Arc::new(o);

    let batch_start = WalEntry {
        sequence: 0,
        op_type: OpType::BatchStart,
        has_value: true,
        key: vec![],
        value: Some(1u32.to_le_bytes().to_vec()),
    };
    let put = WalEntry {
        sequence: 100,
        op_type: OpType::TypePut,
        has_value: true,
        key: b"k0".to_vec(),
        value: Some(b"v".to_vec()),
    };

    let mut mgr = WALManager::open(d.path(), 1, 100, opts).unwrap();
    mgr.append(&batch_start.encode()).unwrap();
    mgr.append(&put.encode()).unwrap();
    mgr.note_appended_sequence(100);
    mgr.close().unwrap();

    std::fs::metadata(d.path().join("wal_1.log")).unwrap().len() + 20
}

#[test]
fn test_db_write_batch_crash_recovery_with_wal_rotate() {
    let max_wal_size = max_wal_size_for_mid_batch_rotate();
    let dir = tempdir().unwrap();

    let keys: Vec<Vec<u8>> = (0..BATCH_SIZE)
        .map(|i| format!("batch_key_{i}").into_bytes())
        .collect();

    {
        let db = DB::open(dir.path(), wal_rotate_opts(max_wal_size)).unwrap();
        let mut batch = WriteBatch::new();
        for key in &keys {
            batch.put(key.as_slice(), b"val");
        }
        db.write(&batch).unwrap();
        // 故意不 close — 模拟崩溃; Drop 会 sync WAL.
    }

    assert!(
        count_wal_log_files(dir.path()) == 1,
        "WriteBatch should stay in a single WAL file"
    );

    let db = DB::open(dir.path(), wal_rotate_opts(max_wal_size)).unwrap();
    let mut present = 0usize;
    for key in &keys {
        if db.get(key).unwrap().is_some() {
            present += 1;
        }
    }

    assert_eq!(
        present, BATCH_SIZE,
        "WriteBatch crash recovery must restore all keys, got {present}/{BATCH_SIZE}"
    );
}
