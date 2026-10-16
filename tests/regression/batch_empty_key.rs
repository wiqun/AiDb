//! WriteBatch 含空 key 时整批必须失败且不留下任何前缀写入.
//! @component aidb-engine
//!
//! ```bash
//! cargo test --test regression batch_empty_key -- --test-threads=1
//! ```

use aidb::config::Options;
use aidb::{WriteBatch, DB};
use tempfile::tempdir;

/// bug: `write()` 先把整批写入 WAL, 再在 MemTable 循环中逐条校验 user key; 批次中间出现空 key 时,
/// 前缀已写入 MemTable 但 `committed_sequence` 不推进, 之后任一写入推进边界都会把前缀暴露出来,
/// 且重开库后前缀仍可能从 WAL 回放.
/// 期望: 空 key 批次返回错误, 前缀 key 不可见 (包括 `get`, `scan`, 重开库后), 后续写入不暴露前缀.
/// Issue: 待补 (未提供编号)
#[test]
fn batch_with_empty_key_leaves_no_prefix_effect() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), Options::for_testing()).unwrap();

    let mut batch = WriteBatch::new();
    batch.put(b"a".to_vec(), b"1".to_vec());
    batch.put(Vec::new(), b"2".to_vec());
    batch.put(b"c".to_vec(), b"3".to_vec());
    assert!(db.write(&batch).is_err(), "含空 key 的批次必须返回错误");

    assert_eq!(db.get(b"a").unwrap(), None, "失败批次的前缀 key 不可见");

    db.put(b"z", b"9").unwrap();
    let keys: Vec<Vec<u8>> = db.scan(None, None).unwrap().map(|r| r.unwrap().0).collect();
    assert_eq!(keys, vec![b"z".to_vec()], "后续写入不得暴露失败批次的前缀");
    db.close().unwrap();
    drop(db);

    let db = DB::open(dir.path(), Options::for_testing()).unwrap();
    assert_eq!(db.get(b"a").unwrap(), None, "重开库后失败批次不得回放");
    assert_eq!(db.get(b"c").unwrap(), None, "重开库后失败批次不得回放");
    db.close().unwrap();
}
