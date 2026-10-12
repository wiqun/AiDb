//! CompactionFilter property-based 测试:
//! @component aidb-engine
//! 随机 put/delete/flush/compact 操作序列 + 对照模型, 验证 filter 语义.

use aidb::config::Options;
use aidb::engine::compaction::{CompactionFilter, CompactionPointReader, FilterDecision};
use aidb::DB;
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use tempfile::tempdir;

fn test_opts() -> Options {
    let mut o = Options::for_testing();
    o.memtable_size = 512;
    o.level0_compaction_trigger = 2;
    o.sync_wal = false;
    o
}

/// 过滤掉 value 最后一位 == 0 的 entry (偶数).
struct EvenFilter;

impl CompactionFilter for EvenFilter {
    fn filter(&self, _level: usize, _key: &[u8], value: &[u8]) -> FilterDecision {
        if value.last().is_some_and(|b| b % 2 == 0) {
            FilterDecision::Remove
        } else {
            FilterDecision::Keep
        }
    }
}

proptest! {
#![proptest_config(ProptestConfig::with_cases(100))]

/// 随机 put+flush+compact, 验证 CompactionFilter 正确移除了偶数 value 的 entry.
#[test]
fn prop_filter_removes_entries(
    puts in prop::collection::vec((0u8..=12u8, 0u8..=255u8), 1..60),
) {
    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open(dir.path(), test_opts()).unwrap());
    db.set_compaction_filter(Some(Arc::new(EvenFilter)));

    let mut model = BTreeMap::new();
    for &(k, v) in &puts {
        db.put(&[k], &[v]).unwrap();
        model.insert(k, v);
    }
    db.flush().unwrap();
    // 写入 key 直到触发 L0 compaction
    for i in 0..5u8 {
        db.put(&[b'p', i], &[i]).unwrap();
        db.flush().unwrap();
    }
    db.drain_compactions().unwrap();

    for k in 0u8..=12u8 {
        let expected = model.get(&k).and_then(|v| {
            if v % 2 == 0 { None }
            else { Some(vec![*v]) }
        });
        let actual = db.get(&[k]).unwrap();
        prop_assert_eq!(&actual, &expected,
            "key {}: expected={:?}, got={:?}", k, expected, actual);
    }
    db.close().unwrap();
}

/// put+delete交替, filter 不影响 tombstone.
#[test]
fn prop_filter_with_deletes(
    ops in prop::collection::vec(
        prop_oneof![
            (0u8..=10u8, 0u8..=255u8).prop_map(|(k,v)| (false, k, v)),
            (0u8..=10u8).prop_map(|k| (true, k, 0u8)),
        ],
        1..50,
    ),
) {
    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open(dir.path(), test_opts()).unwrap());
    db.set_compaction_filter(Some(Arc::new(EvenFilter)));

    let mut model = BTreeMap::new();
    for &(is_del, k, v) in &ops {
        if is_del {
            db.delete(&[k]).unwrap();
            model.insert(k, None);
        } else {
            db.put(&[k], &[v]).unwrap();
            model.insert(k, Some(v));
        }
    }
    db.flush().unwrap();
    for i in 0..5u8 {
        db.put(&[b'q', i], &[i]).unwrap();
        db.flush().unwrap();
    }
    db.drain_compactions().unwrap();

    for k in 0u8..=10u8 {
        let actual = db.get(&[k]).unwrap();
        match model.get(&k) {
            Some(None) => {
                prop_assert_eq!(&actual, &None,
                    "deleted key {}: got {:?}", k, actual);
            }
            Some(&Some(v)) if v % 2 == 0 => {
                prop_assert_eq!(&actual, &None,
                    "filtered key {}: got {:?}", k, actual);
            }
            Some(&Some(v)) => {
                prop_assert_eq!(&actual, &Some(vec![v]),
                    "unfiltered key {}", k);
            }
            None => {
                prop_assert_eq!(&actual, &None,
                    "never-written key {}", k);
            }
        }
    }
    db.close().unwrap();
}

/// 没有 filter 时所有 entry 保留.
#[test]
fn prop_no_filter_keeps_all(
    puts in prop::collection::vec((0u8..=12u8, 0u8..=255u8), 1..40),
) {
    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open(dir.path(), test_opts()).unwrap());

    let mut model = BTreeMap::new();
    for &(k, v) in &puts {
        db.put(&[k], &[v]).unwrap();
        model.insert(k, v);
    }
    db.flush().unwrap();
    for i in 0..5u8 {
        db.put(&[b'x', i], &[i]).unwrap();
        db.flush().unwrap();
    }
    db.drain_compactions().unwrap();

    for k in 0u8..=12u8 {
        let expected = model.get(&k).map(|v| vec![*v]);
        let actual = db.get(&[k]).unwrap();
        prop_assert_eq!(&actual, &expected, "key {}", k);
    }
    db.close().unwrap();
}

}

/// key 形如 `s{n}/{i}` 的是子项, 父 header 为 `h{n}`; 父不存在则子项是孤儿.
/// 只有带点查句柄时才能判断父是否存在, 因此 `filter` 保守保留.
struct OrphanFilter;

impl CompactionFilter for OrphanFilter {
    fn filter(&self, _level: usize, _key: &[u8], _value: &[u8]) -> FilterDecision {
        FilterDecision::Keep
    }

    fn filter_with_reader(
        &self,
        _level: usize,
        key: &[u8],
        _value: &[u8],
        reader: &dyn CompactionPointReader,
    ) -> FilterDecision {
        let user_key = aidb::engine::memtable::extract_user_key(key);
        let Some(rest) = user_key.strip_prefix(b"s") else {
            return FilterDecision::Keep;
        };
        let n = rest.split(|b| *b == b'/').next().unwrap_or_default();
        let header = [b"h".as_slice(), n].concat();
        match reader.get(&header) {
            Ok(None) => FilterDecision::Remove,
            _ => FilterDecision::Keep,
        }
    }
}

/// 期望: compaction 时 filter 能经 reader 点查父 header, 删除父已不存在的子项, 保留有父的子项.
/// 这是 aikv 版本化 subkey GC 的依赖点, 必须在真实 DB compaction 路径上生效.
#[test]
fn filter_with_reader_drops_orphans_during_db_compaction() {
    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open(dir.path(), test_opts()).unwrap());
    db.set_compaction_filter(Some(Arc::new(OrphanFilter)));

    db.put(b"h1", b"x").unwrap();
    for i in 0..20u8 {
        db.put(&[b"s1/".as_slice(), &[i]].concat(), b"v").unwrap();
        db.put(&[b"s2/".as_slice(), &[i]].concat(), b"v").unwrap();
    }
    db.flush().unwrap();
    for i in 0..5u8 {
        db.put(&[b'p', i], &[i]).unwrap();
        db.flush().unwrap();
    }
    db.drain_compactions().unwrap();

    for i in 0..20u8 {
        assert!(db
            .get(&[b"s1/".as_slice(), &[i]].concat())
            .unwrap()
            .is_some());
        assert!(db
            .get(&[b"s2/".as_slice(), &[i]].concat())
            .unwrap()
            .is_none());
    }
    db.close().unwrap();
}
