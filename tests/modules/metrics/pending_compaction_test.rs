use std::sync::atomic::Ordering;
use tempfile::TempDir;

use aidb::config::Options;
use aidb::engine::compaction::{CompactionPicker, VersionEdit, VersionSet};
use aidb::DB;

#[test]
fn test_version_set_pending_compaction_bytes_l0_oldest_first() {
    let dir = TempDir::new().unwrap();
    let mut vs = VersionSet::open_new(dir.path(), 7, 1024 * 1024).unwrap();

    let opts = Options {
        level0_compaction_trigger: 4,
        ..Default::default()
    };
    let picker = CompactionPicker::from_options(&opts);

    // 尚未超额: 3 个文件 < trigger 4
    for i in 1..=3u64 {
        vs.apply_edit(&VersionEdit::AddFile {
            level: 0,
            file_number: i,
            file_size: 100 * i,
            smallest_key: vec![],
            largest_key: vec![],
        })
        .unwrap();
    }
    assert_eq!(vs.pending_compaction_bytes(&picker), 0);

    // 增加至 6 个文件: N=6, trigger=4, excess=2
    // 最老的两个文件为 file 1 (100) 与 file 2 (200) -> 300
    for i in 4..=6u64 {
        vs.apply_edit(&VersionEdit::AddFile {
            level: 0,
            file_number: i,
            file_size: 100 * i,
            smallest_key: vec![],
            largest_key: vec![],
        })
        .unwrap();
    }
    assert_eq!(vs.pending_compaction_bytes(&picker), 300);
}

#[test]
fn test_version_set_pending_compaction_bytes_l1_plus_levels() {
    let dir = TempDir::new().unwrap();
    let mut vs = VersionSet::open_new(dir.path(), 7, 1024 * 1024).unwrap();

    let opts = Options {
        level0_compaction_trigger: 4,
        max_bytes_for_level_base: 500,
        max_bytes_for_level_multiplier: 10,
        max_levels: 7,
        ..Default::default()
    };
    let picker = CompactionPicker::from_options(&opts);

    // L0 没有超额 (2 个文件)
    for i in 1..=2u64 {
        vs.apply_edit(&VersionEdit::AddFile {
            level: 0,
            file_number: i,
            file_size: 100,
            smallest_key: vec![],
            largest_key: vec![],
        })
        .unwrap();
    }

    // L1 target 是 500. 增加两个 400 字节文件, actual = 800. excess = 300.
    vs.apply_edit(&VersionEdit::AddFile {
        level: 1,
        file_number: 10,
        file_size: 400,
        smallest_key: vec![],
        largest_key: vec![],
    })
    .unwrap();
    vs.apply_edit(&VersionEdit::AddFile {
        level: 1,
        file_number: 11,
        file_size: 400,
        smallest_key: vec![],
        largest_key: vec![],
    })
    .unwrap();

    // L6 (最后一层: index 6 = max_levels - 1) 即使有大量数据也不计入 compaction pending bytes
    vs.apply_edit(&VersionEdit::AddFile {
        level: 6,
        file_number: 20,
        file_size: 1_000_000,
        smallest_key: vec![],
        largest_key: vec![],
    })
    .unwrap();

    assert_eq!(vs.pending_compaction_bytes(&picker), 300);
}

#[test]
fn test_db_pending_compaction_bytes_integration() {
    let dir = TempDir::new().unwrap();
    let opts = Options {
        background_compaction: false, // 禁用后台 compaction 保持积压
        level0_compaction_trigger: 2,
        ..Default::default()
    };
    let db = DB::open(dir.path(), opts).unwrap();
    let stats = db.statistics();

    // 写入 3 个 SSTable
    for i in 0..3u64 {
        db.put(format!("k{i}").as_bytes(), b"val").unwrap();
        db.flush().unwrap();
    }

    let pending = db.pending_compaction_bytes();
    assert!(
        pending > 0,
        "pending_compaction_bytes should be > 0 when L0 has 3 files and trigger is 2"
    );

    let atomic_pending = stats.compaction_pending_bytes.load(Ordering::Relaxed);
    assert_eq!(
        pending, atomic_pending,
        "db.pending_compaction_bytes() must match stats.compaction_pending_bytes"
    );

    let snapshot_pending = stats.snapshot().compaction_pending_bytes;
    assert_eq!(
        pending, snapshot_pending,
        "snapshot.compaction_pending_bytes must match pending_compaction_bytes"
    );
}
