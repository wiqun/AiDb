//! MemTable 功能测试 — InternalKey / CRUD / Iterator / freeze
//! @component aidb-memtable

use aidb::engine::memtable::{
    compare_internal_key, encode_internal_key, extract_sequence, MemTable, ValueType,
    K_MAX_SEQUENCE,
};

/// 验证 InternalKey 编码解码的 Roundtrip 完整性
#[test]
fn test_internal_key_encode_decode_roundtrip() {
    let enc = encode_internal_key(b"hello", 42, ValueType::TypeDelete);
    let dec = aidb::engine::memtable::decode_internal_key(&enc).unwrap();
    assert_eq!(dec.0, b"hello");
    assert_eq!(dec.1, 42);
    assert_eq!(dec.2, ValueType::TypeDelete);
}

/// 验证相同 UserKey 下 Sequence 倒序排列
#[test]
fn test_internal_key_ordering_sequence_desc() {
    let old = encode_internal_key(b"k", 1, ValueType::TypePut);
    let new = encode_internal_key(b"k", 2, ValueType::TypePut);
    assert_eq!(compare_internal_key(&new, &old), std::cmp::Ordering::Less);
}

/// 验证不同 UserKey 之间的字典序比较
#[test]
fn test_compare_internal_key() {
    let a = encode_internal_key(b"x", 10, ValueType::TypePut);
    let b = encode_internal_key(b"y", 1, ValueType::TypePut);
    assert_eq!(compare_internal_key(&a, &b), std::cmp::Ordering::Less);
}

/// 每条成功写入的内部记录都计入 entry_count, 包括覆盖, 点删除和范围删除.
#[test]
fn entry_count_counts_each_inserted_record() {
    let mt = MemTable::new();
    assert_eq!(mt.entry_count(), 0);
    mt.put(b"a", b"1", 1).unwrap();
    mt.put(b"a", b"2", 2).unwrap();
    mt.delete(b"a", 3).unwrap();
    mt.put_range_delete(b"a", b"z", 4).unwrap();
    assert_eq!(mt.entry_count(), 4);
    let frozen = mt.freeze(4);
    assert_eq!(frozen.entry_count(), 4);
}

/// 验证 MemTable 基本 Put 和 Get 操作
#[test]
fn test_memtable_put_get() {
    let mt = MemTable::new();
    mt.put(b"k", b"v", 1).unwrap();
    assert_eq!(mt.get_latest(b"k").unwrap(), Some(b"v".to_vec()));
}

/// 验证查询不存在的 Key 返回 None
#[test]
fn test_memtable_get_not_found() {
    let mt = MemTable::new();
    assert_eq!(mt.get_latest(b"missing").unwrap(), None);
}

/// 验证 Delete 操作在 MemTable 中的写入与生效
#[test]
fn test_memtable_delete() {
    let mt = MemTable::new();
    mt.put(b"k", b"v", 1).unwrap();
    mt.delete(b"k", 2).unwrap();
    assert_eq!(mt.get_latest(b"k").unwrap(), None);
}

/// 验证多版本 Key 覆盖写入取最新 Sequence 值
#[test]
fn test_memtable_overwrite() {
    let mt = MemTable::new();
    mt.put(b"k", b"v1", 1).unwrap();
    mt.put(b"k", b"v2", 2).unwrap();
    assert_eq!(mt.get_latest(b"k").unwrap(), Some(b"v2".to_vec()));
}

/// 验证根据不同 Snapshot Sequence 读出历史版本
#[test]
fn test_memtable_snapshot() {
    let mt = MemTable::new();
    mt.put(b"k", b"v1", 1).unwrap();
    mt.put(b"k", b"v2", 2).unwrap();
    assert_eq!(mt.get(b"k", 1).unwrap(), Some(b"v1".to_vec()));
    assert_eq!(mt.get(b"k", 2).unwrap(), Some(b"v2".to_vec()));
    assert_eq!(mt.get(b"k", K_MAX_SEQUENCE).unwrap(), Some(b"v2".to_vec()));
}

/// 验证 Seek/Search 底层跳表精确匹配
#[test]
fn test_memtable_search() {
    let mt = MemTable::new();
    mt.put(b"k", b"v", 5).unwrap();
    let seek = encode_internal_key(b"k", 5, ValueType::TypePut);
    let (val, ty) = mt.search(&seek).unwrap().unwrap();
    assert_eq!(&*val, b"v");
    assert_eq!(ty, ValueType::TypePut);

    let seek_old = encode_internal_key(b"k", 3, ValueType::TypePut);
    assert!(mt.search(&seek_old).unwrap().is_none());

    mt.delete(b"k", 6).unwrap();
    let seek_after = encode_internal_key(b"k", 6, ValueType::TypePut);
    let (_, ty) = mt.search(&seek_after).unwrap().unwrap();
    assert_eq!(ty, ValueType::TypeDelete);
}

/// 验证 MemTable 迭代器 Seek 定位到指定 Key
#[test]
fn test_memtable_iterator_seek() {
    let mt = MemTable::new();
    mt.put(b"a", b"1", 1).unwrap();
    mt.put(b"b", b"2", 2).unwrap();
    let mut it = mt.iter();
    it.seek(b"b");
    assert!(it.valid());
    assert_eq!(it.key(), encode_internal_key(b"b", 2, ValueType::TypePut));
}

/// 验证 MemTable 迭代器 SeekToFirst 定位到首条记录
#[test]
fn test_memtable_iterator_seek_to_first() {
    let mt = MemTable::new();
    mt.put(b"z", b"z", 1).unwrap();
    mt.put(b"a", b"a", 2).unwrap();
    let mut it = mt.iter();
    it.seek_to_first();
    assert!(it.valid());
    let seq = extract_sequence(it.key()).unwrap();
    assert_eq!(seq, 2);
}

/// 验证 MemTable 迭代器正向 Next 顺序遍历
#[test]
fn test_memtable_iterator_next() {
    let mt = MemTable::new();
    mt.put(b"a", b"1", 1).unwrap();
    mt.put(b"b", b"2", 2).unwrap();
    let mut it = mt.iter();
    it.seek_to_first();
    let mut keys = Vec::new();
    while it.valid() {
        keys.push(aidb::engine::memtable::extract_user_key(it.key()).to_vec());
        if !it.next() {
            break;
        }
    }
    assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
}

/// 验证 MemTable 迭代器反向 Prev 顺序遍历
#[test]
fn test_memtable_iterator_prev() {
    use aidb::engine::memtable::extract_user_key;

    let mt = MemTable::new();
    mt.put(b"a", b"1", 1).unwrap();
    mt.put(b"b", b"2", 2).unwrap();
    mt.put(b"c", b"3", 3).unwrap();

    let mut it = mt.iter();
    assert!(!it.valid());
    assert!(it.prev());
    assert_eq!(extract_user_key(it.key()), b"c");

    assert!(it.prev());
    assert_eq!(extract_user_key(it.key()), b"b");

    assert!(it.prev());
    assert_eq!(extract_user_key(it.key()), b"a");

    assert!(!it.prev());
    assert!(!it.valid());

    it.seek(b"b");
    assert!(it.valid());
    assert_eq!(extract_user_key(it.key()), b"b");
    assert!(it.prev());
    assert_eq!(extract_user_key(it.key()), b"a");
}

/// 验证 MemTable 估算内存字节尺寸自增
#[test]
fn test_memtable_size_approx() {
    let mt = MemTable::new();
    assert_eq!(mt.approximate_size(), 0);
    mt.put(b"ab", b"cd", 1).unwrap();
    let after_put = mt.approximate_size();
    assert!(
        after_put > 0,
        "size should increase after put, got {after_put}"
    );
    // InternalKey 含 8 字节 sequence/tag 编码头, 因此必须 >= key+value 字面值
    assert!(
        after_put >= 4,
        "put ab+cd should be at least 4 bytes, got {after_put}"
    );
    mt.delete(b"x", 2).unwrap();
    let after_delete = mt.approximate_size();
    assert!(
        after_delete > after_put,
        "size should keep increasing after delete: {after_put} -> {after_delete}"
    );
}

/// 验证 MemTable 冻结为 ImmutableMemTable 及只读隔离
#[test]
fn test_immutable_freeze() {
    let mt = MemTable::new();
    mt.put(b"k", b"v", 1).unwrap();
    let frozen = mt.freeze(1);
    assert_eq!(frozen.flush_seq(), 1);
    assert_eq!(frozen.get_latest(b"k").unwrap(), Some(b"v".to_vec()));

    let active = MemTable::new();
    active.put(b"k2", b"v2", 2).unwrap();
    assert_eq!(active.get_latest(b"k2").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(frozen.get_latest(b"k2").unwrap(), None);
}

/// 验证空 Key (0字节) 的正常写入与读取
#[test]
fn test_memtable_empty_user_key() {
    let mt = MemTable::new();
    mt.put(b"", b"v", 1).unwrap();
    assert_eq!(mt.get_latest(b"").unwrap(), Some(b"v".to_vec()));
}

/// 验证相同 Sequence 下 ValueType 的排序仲裁
#[test]
fn test_internal_key_value_type_tiebreak() {
    let put = encode_internal_key(b"k", 1, ValueType::TypePut);
    let del = encode_internal_key(b"k", 1, ValueType::TypeDelete);
    assert_eq!(compare_internal_key(&put, &del), std::cmp::Ordering::Less);
}

/// 验证损坏的 InternalKey 解码时返回错误
#[test]
fn test_decode_internal_key_corruption() {
    assert!(aidb::engine::memtable::decode_internal_key(b"short").is_err());
    let mut bad = encode_internal_key(b"k", 1, ValueType::TypePut);
    *bad.last_mut().unwrap() = 9;
    assert!(aidb::engine::memtable::decode_internal_key(&bad).is_err());
}

/// 验证 Sequence 超过上限值时写入拒绝
#[test]
fn test_memtable_sequence_overflow() {
    use aidb::engine::memtable::SEQUENCE_LIMIT;
    let mt = MemTable::new();
    assert!(mt.put(b"k", b"v", SEQUENCE_LIMIT).is_err());
}
