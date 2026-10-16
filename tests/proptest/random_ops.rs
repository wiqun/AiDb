//! 随机 put/delete/flush/compact/writebatch 与线性模型一致.
//! @component aidb-engine

use aidb::config::Options;
use aidb::WriteBatch;
use aidb::DB;
use proptest::prelude::*;
use std::sync::Arc;
use tempfile::tempdir;

fn test_opts() -> Options {
    let mut o = Options::for_testing();
    o.memtable_size = 512;
    o.level0_compaction_trigger = 2;
    o.sync_wal = false;
    o
}

#[derive(Debug, Clone)]
enum Op {
    Put(u8, u8),
    PutBatch(Vec<(u8, u8)>),
    Delete(u8),
    Flush,
    Compact,
}

fn apply_model(model: &mut std::collections::BTreeMap<u8, Option<Vec<u8>>>, op: &Op) {
    match op {
        Op::Put(k, v) => {
            model.insert(*k, Some(vec![*v]));
        }
        Op::PutBatch(kvs) => {
            for (k, v) in kvs {
                model.insert(*k, Some(vec![*v]));
            }
        }
        Op::Delete(k) => {
            model.insert(*k, None);
        }
        Op::Flush | Op::Compact => {}
    }
}

fn model_get(model: &std::collections::BTreeMap<u8, Option<Vec<u8>>>, k: u8) -> Option<Vec<u8>> {
    match model.get(&k) {
        Some(Some(v)) => Some(v.clone()),
        _ => None,
    }
}

proptest! {
  #![proptest_config(ProptestConfig::with_cases(100))]

  /// 随机 put/delete/flush/compact 序列, 验证 DB 与线性模型一致.
  #[test]
  fn prop_random_ops_match_model(ops in prop::collection::vec(
    prop_oneof![
      (0u8..=20u8, 0u8..=255u8).prop_map(|(k,v)| Op::Put(k,v)),
      (0u8..=20u8).prop_map(Op::Delete),
      Just(Op::Flush),
      Just(Op::Compact),
    ],
    1..80
  )) {
    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open(dir.path(), test_opts()).unwrap());
    let mut model = std::collections::BTreeMap::new();

    for op in &ops {
      match op {
        Op::Put(k, v) => { db.put(&[*k], &[*v]).unwrap(); }
        Op::Delete(k) => { db.delete(&[*k]).unwrap(); }
        Op::Flush => { db.flush().unwrap(); }
        Op::Compact => { db.drain_compactions().unwrap(); }
        Op::PutBatch(_) => unreachable!(),
      }
      apply_model(&mut model, op);
    }

    for k in 0u8..=20u8 {
      let expected = model_get(&model, k);
      let actual = db.get(&[k]).unwrap();
      prop_assert_eq!(actual, expected, "key {}", k);
    }
    db.close().unwrap();
  }

  /// 随机 WriteBatch 写入, 验证批量操作结果与线性模型一致.
  #[test]
  fn prop_writebatch_ops_match_model(batches in prop::collection::vec(
    prop::collection::vec(
      (0u8..=20u8, 0u8..=255u8).prop_map(|(k,v)| (k,v)),
      1..10,
    ).prop_map(Op::PutBatch),
    1..20
  )) {
    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open(dir.path(), test_opts()).unwrap());
    let mut model = std::collections::BTreeMap::new();

    for batch_kvs in &batches {
      let Op::PutBatch(ref kvs) = batch_kvs else { continue };
      let mut wb = WriteBatch::new();
      for (k, v) in kvs {
        wb.put([*k], [*v]);
      }
      db.write(&wb).unwrap();
      apply_model(&mut model, batch_kvs);
    }

    for k in 0u8..=20u8 {
      let expected = model_get(&model, k);
      let actual = db.get(&[k]).unwrap();
      prop_assert_eq!(actual, expected, "key {}", k);
    }
    db.close().unwrap();
  }

  /// 交替 put/delete (含重叠 key), 验证最终一致性.
  #[test]
  fn prop_interleaved_put_delete(
    puts in prop::collection::vec((0u8..=15u8, 0u8..=255u8), 1..30),
    deletes in prop::collection::vec(0u8..=15u8, 1..20),
  ) {
    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open(dir.path(), test_opts()).unwrap());
    let mut model = std::collections::BTreeMap::new();

    for (k, v) in &puts { db.put(&[*k], &[*v]).unwrap(); model.insert(*k, Some(vec![*v])); }
    for k in &deletes { db.delete(&[*k]).unwrap(); model.insert(*k, None); }
    db.flush().unwrap();

    for k in 0u8..=15u8 {
      let expected = model_get(&model, k);
      let actual = db.get(&[k]).unwrap();
      prop_assert_eq!(actual, expected, "key {}", k);
    }
    db.close().unwrap();
  }

  /// 空值写入 + 读取不变式.
  #[test]
  fn prop_empty_value(key in 0u8..=10u8) {
    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open(dir.path(), test_opts()).unwrap());
    db.put(&[key], b"").unwrap();
    let val = db.get(&[key]).unwrap();
    prop_assert_eq!(val, Some(vec![]));
    db.close().unwrap();
  }

  /// 大 value 写入 + 读取不变式.
  #[test]
  fn prop_large_value(val_len in 100usize..5000usize) {
    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open(dir.path(), test_opts()).unwrap());
    let val = (0..val_len).map(|i| (i % 256) as u8).collect::<Vec<_>>();
    db.put(b"large_key", &val).unwrap();
    let got = db.get(b"large_key").unwrap();
    prop_assert_eq!(got, Some(val));
    db.close().unwrap();
  }

}
