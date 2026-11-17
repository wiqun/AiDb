//! WAL 恢复数据完整性 proptest.
//! @component aidb-engine
//!
//! 验证 crash (非正常 close) 后重启, 已 flush 的数据不丢.

use aidb::config::Options;
use aidb::DB;
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use tempfile::tempdir;

fn recovery_test_opts() -> Options {
    let mut o = Options::for_testing();
    o.memtable_size = 4096;
    o.sync_wal = true; // 确保 WAL 同步
    o
}

/// 模拟 crash (`drop` 未调 `close`) 后重新打开, 等待后台线程释放文件锁.
fn reopen_after_drop(path: &std::path::Path) -> Arc<DB> {
    for _ in 0..100 {
        match DB::open(path, recovery_test_opts()) {
            Ok(db) => return db,
            Err(aidb::Error::Busy(_)) => std::thread::sleep(std::time::Duration::from_millis(10)),
            Err(e) => panic!("reopen failed: {e}"),
        }
    }
    DB::open(path, recovery_test_opts()).unwrap()
}

proptest! {
#![proptest_config(ProptestConfig::with_cases(50))]

/// 随机 puts → flush → 模拟 crash (drop without close) → reopen → 验证.
#[test]
fn prop_recovery_after_flush(
    puts in prop::collection::vec((0u8..=15u8, 0u8..=255u8), 10..60),
) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // 写入 + flush
    let mut model = BTreeMap::new();
    {
        let db = Arc::new(DB::open(&db_path, recovery_test_opts()).unwrap());
        for &(k, v) in &puts {
            db.put(&[k], &[v]).unwrap();
            model.insert(k, v);
        }
        db.flush().unwrap();
        // 模拟 crash: 不调用 close(), 只是 drop
    }

    // 恢复
    {
        let db = reopen_after_drop(&db_path);
        for k in 0u8..=15u8 {
            let expected = model.get(&k).map(|v| vec![*v]);
            let actual = db.get(&[k]).unwrap();
            prop_assert_eq!(actual, expected, "key {} after crash recovery", k);
        }
        db.close().unwrap();
    }
}

/// puts + overwrites → flush → crash → reopen → 验证最后写入的值.
#[test]
fn prop_recovery_with_overwrites(
    rounds in prop::collection::vec(
        (0u8..=10u8, 0u8..=255u8),
        1..30,
    ),
) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();
    let mut model = BTreeMap::new();

    {
        let db = Arc::new(DB::open(&db_path, recovery_test_opts()).unwrap());
        for &(k, v) in &rounds {
            db.put(&[k], &[v]).unwrap();
            model.insert(k, v);
        }
        db.flush().unwrap();
    }

    {
        let db = reopen_after_drop(&db_path);
        for k in 0u8..=10u8 {
            let expected = model.get(&k).map(|v| vec![*v]);
            let actual = db.get(&[k]).unwrap();
            prop_assert_eq!(actual, expected, "overwrite key {}", k);
        }
        db.close().unwrap();
    }
}

/// puts + deletes → flush → crash → reopen.
#[test]
fn prop_recovery_with_deletes(
    puts in prop::collection::vec((0u8..=10u8, 0u8..=255u8), 5..20),
    deletes in prop::collection::vec(0u8..=10u8, 1..10),
) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();
    let mut model: BTreeMap<u8, Option<u8>> = BTreeMap::new();

    {
        let db = Arc::new(DB::open(&db_path, recovery_test_opts()).unwrap());
        for &(k, v) in &puts {
            db.put(&[k], &[v]).unwrap();
            model.insert(k, Some(v));
        }
        for k in &deletes {
            db.delete(&[*k]).unwrap();
            model.insert(*k, None);
        }
        db.flush().unwrap();
    }

    {
        let db = reopen_after_drop(&db_path);
        for k in 0u8..=10u8 {
            let actual = db.get(&[k]).unwrap();
            match model.get(&k) {
                Some(None) => {
                    prop_assert_eq!(actual, None,
                        "deleted key {} should be absent after recovery", k);
                }
                Some(&Some(v)) => {
                    prop_assert_eq!(actual, Some(vec![v]),
                        "key {} value mismatch after recovery", k);
                }
                None => {
                    prop_assert_eq!(actual, None,
                        "never-written key {} should be absent", k);
                }
            }
        }
        db.close().unwrap();
    }
}

}
