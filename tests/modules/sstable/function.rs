//! SSTable 功能测试
//! @component aidb-sstable

use std::fs;
use std::sync::Arc;
use std::thread;

use aidb::config::CompressionType;
use aidb::engine::memtable::{encode_internal_key, ValueType};
use aidb::engine::sstable::{
    parse_sstable_filename, sstable_path, Block, BlockBuilder, Footer, IndexBlock,
    IndexBlockBuilder, IndexEntry, SSTableBuilder, SSTableIterator, SSTableReader, MAGIC_NUMBER,
};
use aidb::engine::sstable::{BlockHandle, FOOTER_SIZE};
use aidb::Error;
use tempfile::tempdir;

fn ik(user: &[u8], seq: u64) -> Vec<u8> {
    encode_internal_key(user, seq, ValueType::TypePut)
}

/// 验证空 BlockBuilder 构造和生成空 Data Block
#[test]
fn test_block_builder_empty() {
    let b = BlockBuilder::new(16);
    assert!(b.is_empty());
    let mut b = b;
    let data = b.finish();
    let block = Block::new(data).unwrap();
    assert_eq!(block.num_restarts(), 0);
}

/// 验证 Block 写入单条 Record 的读取与 Iterator 迭代
#[test]
fn test_block_single_entry() {
    let mut b = BlockBuilder::new(16);
    let key = ik(b"key1", 1);
    b.add(&key, b"val1").unwrap();
    let data = b.finish();
    let block = Block::new(data).unwrap();
    let mut it = block.iter();
    assert!(it.valid());
    assert_eq!(it.key(), key.as_slice());
    assert_eq!(it.value(), b"val1");
    assert!(!it.advance());
}

/// 验证 Block 内部前缀压缩 (Prefix Compression) 编码
#[test]
fn test_block_prefix_compression() {
    let mut b = BlockBuilder::new(16);
    b.add(&ik(b"prefix_a", 1), b"v1").unwrap();
    b.add(&ik(b"prefix_b", 2), b"v2").unwrap();
    let data = b.finish();
    let block = Block::new(data).unwrap();
    let mut it = block.iter();
    assert!(it.valid());
    assert_eq!(it.key(), ik(b"prefix_a", 1).as_slice());
    assert!(it.advance());
    assert_eq!(it.key(), ik(b"prefix_b", 2).as_slice());
}

/// 验证 Block 乱序 Key 写入抛出错误
#[test]
fn test_block_unsorted_keys_rejected() {
    let mut b = BlockBuilder::new(16);
    b.add(&ik(b"z", 2), b"v").unwrap();
    assert!(b.add(&ik(b"a", 1), b"v").is_err());
}

/// 验证同前缀 UserKey 且不同 Sequence 的排列顺序
#[test]
fn test_block_prefix_user_key_order() {
    // user key "k:9" is a prefix of "k:99"; internal-key compare must allow flush order.
    let mut b = BlockBuilder::new(16);
    b.add(&ik(b"k:9", 10), b"v9").unwrap();
    b.add(&ik(b"k:99", 11), b"v99").unwrap();
    let block = Block::new(b.finish()).unwrap();
    let mut it = block.iter();
    assert!(it.valid());
    assert_eq!(it.key(), ik(b"k:9", 10).as_slice());
    assert!(it.advance());
    assert_eq!(it.key(), ik(b"k:99", 11).as_slice());
}

/// 验证 Block 迭代器顺序遍历全量记录
#[test]
fn test_block_iterator() {
    let mut b = BlockBuilder::new(2);
    for i in 0..5u8 {
        let k = ik(format!("key_{i:02}").as_bytes(), i as u64 + 1);
        b.add(&k, &[i]).unwrap();
    }
    let block = Block::new(b.finish()).unwrap();
    let mut it = block.iter();
    let mut n = 0;
    while it.valid() {
        n += 1;
        if !it.advance() {
            break;
        }
    }
    assert_eq!(n, 5);
}

/// 验证 SSTable Footer 结构的编解码
#[test]
fn test_footer_encode_decode() {
    let f = Footer::new(
        BlockHandle {
            offset: 10,
            size: 100,
        },
        BlockHandle {
            offset: 200,
            size: 50,
        },
    );
    let enc = f.encode();
    let dec = Footer::decode(&enc).unwrap();
    assert_eq!(dec.meta_index_handle, f.meta_index_handle);
    assert_eq!(dec.index_handle, f.index_handle);
    assert_eq!(
        u64::from_le_bytes(enc[40..48].try_into().unwrap()),
        MAGIC_NUMBER
    );
}

#[test]
fn test_footer_magic_validation() {
    let mut buf = [0u8; FOOTER_SIZE];
    buf[40..48].copy_from_slice(&0xBAD_u64.to_le_bytes());
    assert!(Footer::decode(&buf).is_err());
}

#[test]
fn test_index_find_exact() {
    let mut ib = IndexBlockBuilder::new();
    let k1 = encode_internal_key(b"a", 1, ValueType::TypePut);
    let k2 = encode_internal_key(b"m", 1, ValueType::TypePut);
    ib.add_entry(&IndexEntry {
        key: k1.clone(),
        handle: BlockHandle {
            offset: 0,
            size: 10,
        },
    })
    .unwrap();
    ib.add_entry(&IndexEntry {
        key: k2,
        handle: BlockHandle {
            offset: 10,
            size: 10,
        },
    })
    .unwrap();
    let idx = IndexBlock::new(ib.finish()).unwrap();
    let seek = encode_internal_key(b"a", 99, ValueType::TypePut);
    let h = idx.find_block(&seek).unwrap();
    assert_eq!(h.offset, 0);
}

#[test]
fn test_index_find_between() {
    let mut ib = IndexBlockBuilder::new();
    let k1 = encode_internal_key(b"a", 1, ValueType::TypePut);
    let k2 = encode_internal_key(b"m", 1, ValueType::TypePut);
    ib.add_entry(&IndexEntry {
        key: k1,
        handle: BlockHandle {
            offset: 0,
            size: 10,
        },
    })
    .unwrap();
    ib.add_entry(&IndexEntry {
        key: k2,
        handle: BlockHandle {
            offset: 10,
            size: 10,
        },
    })
    .unwrap();
    let idx = IndexBlock::new(ib.finish()).unwrap();
    let seek = encode_internal_key(b"g", 1, ValueType::TypePut);
    let h = idx.find_block(&seek).unwrap();
    assert_eq!(h.offset, 10);
}

#[test]
fn test_sstable_empty_key_rejected() {
    let dir = tempdir().unwrap();
    let path = sstable_path(dir.path(), 1, 0);
    let mut b = SSTableBuilder::new(&path, 4096, 16, CompressionType::None, 0.0).unwrap();
    assert!(b.add(b"", b"v").is_err());
}

#[test]
fn test_sstable_unsorted_keys_rejected() {
    let dir = tempdir().unwrap();
    let path = sstable_path(dir.path(), 1, 0);
    let mut b = SSTableBuilder::new(&path, 4096, 16, CompressionType::None, 0.0).unwrap();
    let k1 = encode_internal_key(b"b", 2, ValueType::TypePut);
    let k2 = encode_internal_key(b"a", 3, ValueType::TypePut);
    b.add(&k1, b"v1").unwrap();
    assert!(b.add(&k2, b"v2").is_err());
}

#[test]
fn test_parse_filename() {
    assert_eq!(parse_sstable_filename("000123_L5.sst"), Some((123, 5)));
    assert_eq!(parse_sstable_filename("000001.sst"), Some((1, 0)));
    assert_eq!(parse_sstable_filename("bad.txt"), None);
}

#[test]
fn test_finish_empty_rejected() {
    let dir = tempdir().unwrap();
    let path = sstable_path(dir.path(), 3, 0);
    let b = SSTableBuilder::new(&path, 4096, 16, CompressionType::None, 0.0).unwrap();
    let err = b.finish().unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
}

#[test]
fn test_block_size_below_min() {
    let dir = tempdir().unwrap();
    let path = sstable_path(dir.path(), 4, 0);
    assert!(SSTableBuilder::new(&path, 128, 16, CompressionType::None, 0.0).is_err());
}

#[test]
fn test_abandon_removes_tmp() {
    let dir = tempdir().unwrap();
    let path = sstable_path(dir.path(), 5, 0);
    let tmp = path.with_extension("sst.tmp");
    let mut b = SSTableBuilder::new(&path, 4096, 16, CompressionType::None, 0.0).unwrap();
    let k = encode_internal_key(b"k", 1, ValueType::TypePut);
    b.add(&k, b"v").unwrap();
    b.abandon().unwrap();
    assert!(!tmp.exists());
    assert!(!path.exists());
}

fn build_sorted_sst(dir: &std::path::Path, entries: &[(&[u8], &[u8], u64)]) -> std::path::PathBuf {
    let path = sstable_path(dir, 1, 0);
    let mut b = SSTableBuilder::new(&path, 256, 2, CompressionType::None, 0.0).unwrap();
    for (uk, val, seq) in entries {
        let ik = encode_internal_key(uk, *seq, ValueType::TypePut);
        b.add(&ik, val).unwrap();
    }
    b.finish().unwrap();
    path
}

#[test]
fn test_build_and_read() {
    let dir = tempdir().unwrap();
    let path = build_sorted_sst(dir.path(), &[(b"foo", b"bar", 10), (b"zzz", b"end", 20)]);
    let r = SSTableReader::open(&path, None).unwrap();
    let seek = encode_internal_key(b"foo", 10, ValueType::TypePut);
    let (v, ty) = r.get(&seek).unwrap().unwrap();
    assert_eq!(v, b"bar");
    assert_eq!(ty, ValueType::TypePut);
}

#[test]
fn test_multi_block_read() {
    let dir = tempdir().unwrap();
    let mut entries = Vec::new();
    for i in 0..50u64 {
        let uk = format!("key_{i:04}").into_bytes();
        let val = format!("val_{i}").into_bytes();
        entries.push((uk, val, i + 1));
    }
    let path = {
        let path = sstable_path(dir.path(), 2, 0);
        let mut b = SSTableBuilder::new(&path, 256, 4, CompressionType::None, 0.0).unwrap();
        for (uk, val, seq) in &entries {
            let ik = encode_internal_key(uk, *seq, ValueType::TypePut);
            b.add(&ik, val).unwrap();
        }
        b.finish().unwrap();
        path
    };
    let r = SSTableReader::open(&path, None).unwrap();
    let seek = encode_internal_key(b"key_0025", 26, ValueType::TypePut);
    let (v, _) = r.get(&seek).unwrap().unwrap();
    assert_eq!(v, b"val_25");
}

/// 回归测试 (2026-07-02): `SSTableBuilder::flush_data_block` 曾经用压缩前的
/// `block_data.len()` 计算 `BlockHandle.size`, 与磁盘上实际落盘的压缩后 payload
/// 长度不一致, 导致多 block SST 在读路径按错误偏移拆 trailer, 报
/// `Corruption("block CRC mismatch")`. 此前所有 SSTable 测试都只用
/// `CompressionType::None` (未压缩时 `data.len()` 恰好等于落盘长度, 掩盖了这个
/// bug), 因此这里专门覆盖 `Snap`/`Lz4` 的多 block 写读 roundtrip.
#[cfg(feature = "compression")]
fn multi_block_read_with_compression(compression: CompressionType) {
    let dir = tempdir().unwrap();
    let mut entries = Vec::new();
    // value 足够大且重复度低, 保证 Snap/Lz4 压缩后长度 < 压缩前长度,
    // 这样才能触发 handle.size 用错长度时的偏移错位.
    for i in 0..200u64 {
        let uk = format!("key_{i:05}").into_bytes();
        let val = format!("value_payload_{i:05}_{}", "x".repeat(64)).into_bytes();
        entries.push((uk, val, i + 1));
    }
    let path = {
        let path = sstable_path(dir.path(), 42, 0);
        // 小 block_size 强制产生多个 data block (触发多次 flush_data_block).
        let mut b = SSTableBuilder::new(&path, 512, 4, compression, 0.0).unwrap();
        for (uk, val, seq) in &entries {
            let ik = encode_internal_key(uk, *seq, ValueType::TypePut);
            b.add(&ik, val).unwrap();
        }
        assert!(
            b.block_count() >= 1,
            "sanity: builder should have flushed at least one block before finish()"
        );
        b.finish().unwrap();
        path
    };

    let r = SSTableReader::open(&path, None).unwrap();
    // 校验首、中、尾各若干 key, 确保跨越多个 (可能压缩后长度各异的) data block
    // 的偏移计算全部正确.
    for &i in &[0u64, 1, 50, 100, 150, 198, 199] {
        let uk = format!("key_{i:05}").into_bytes();
        let expected_val = format!("value_payload_{i:05}_{}", "x".repeat(64)).into_bytes();
        let seek = encode_internal_key(&uk, i + 1, ValueType::TypePut);
        let (v, _) = r
            .get(&seek)
            .unwrap_or_else(|e| panic!("get({i}) failed: {e}"))
            .unwrap_or_else(|| panic!("key {i} unexpectedly missing"));
        assert_eq!(v, expected_val, "value mismatch at key {i}");
    }

    // 全表扫描同样应该完整无损地拿到全部 200 条, 且顺序正确.
    let mut it = r.iter();
    let mut count = 0u64;
    while it.valid() {
        let k = it.key().unwrap();
        let uk = &k[..k.len() - 8];
        let expected_uk = format!("key_{count:05}").into_bytes();
        assert_eq!(uk, expected_uk.as_slice(), "scan order mismatch at {count}");
        count += 1;
        if !it.advance() {
            break;
        }
    }
    assert_eq!(count, entries.len() as u64);
}

#[cfg(feature = "compression")]
#[test]
fn test_multi_block_read_with_snap_compression() {
    multi_block_read_with_compression(CompressionType::Snap);
}

#[cfg(feature = "compression")]
#[test]
fn test_multi_block_read_with_lz4_compression() {
    multi_block_read_with_compression(CompressionType::Lz4);
}

#[test]
fn test_iterator_full_scan() {
    let dir = tempdir().unwrap();
    let path = build_sorted_sst(
        dir.path(),
        &[(b"a", b"1", 1), (b"b", b"2", 2), (b"c", b"3", 3)],
    );
    let r = SSTableReader::open(&path, None).unwrap();
    let mut it = r.iter();
    let mut keys = Vec::new();
    while it.valid() {
        keys.push(it.key().unwrap().to_vec());
        if !it.advance() {
            break;
        }
    }
    assert_eq!(keys.len(), 3);
}

#[test]
fn test_iterator_seek() {
    let dir = tempdir().unwrap();
    let path = build_sorted_sst(
        dir.path(),
        &[(b"a", b"1", 1), (b"m", b"2", 2), (b"z", b"3", 3)],
    );
    let r = SSTableReader::open(&path, None).unwrap();
    let mut it = r.iter();
    let target = encode_internal_key(b"m", 2, ValueType::TypePut);
    it.seek_to_target(&target);
    assert!(it.valid());
    assert_eq!(extract_user_key_from_it(&it), b"m");
}

#[test]
fn test_iterator_seek_past_max() {
    let dir = tempdir().unwrap();
    let path = build_sorted_sst(dir.path(), &[(b"a", b"1", 1), (b"z", b"3", 3)]);
    let r = SSTableReader::open(&path, None).unwrap();
    let mut it = r.iter();
    let past = encode_internal_key(b"zzz", 99, ValueType::TypePut);
    it.seek_to_target(&past);
    assert!(!it.valid());
}

#[test]
fn test_get_sequence_upper_bound() {
    let dir = tempdir().unwrap();
    let path = sstable_path(dir.path(), 6, 0);
    let mut b = SSTableBuilder::new(&path, 4096, 16, CompressionType::None, 0.0).unwrap();
    // 同一 user_key: 文件中 comparator 升序为 seq 大者在前
    let k_new = encode_internal_key(b"u", 20, ValueType::TypePut);
    let k_old = encode_internal_key(b"u", 10, ValueType::TypePut);
    b.add(&k_new, b"v_new").unwrap();
    b.add(&k_old, b"v_old").unwrap();
    b.finish().unwrap();

    let r = SSTableReader::open(&path, None).unwrap();
    let seek_new = encode_internal_key(b"u", 25, ValueType::TypePut);
    let (v, _) = r.get(&seek_new).unwrap().unwrap();
    assert_eq!(v, b"v_new");

    let seek_mid = encode_internal_key(b"u", 15, ValueType::TypePut);
    let (v, _) = r.get(&seek_mid).unwrap().unwrap();
    assert_eq!(v, b"v_old");

    let seek_before = encode_internal_key(b"u", 5, ValueType::TypePut);
    assert!(r.get(&seek_before).unwrap().is_none());
}

fn extract_user_key_from_it(it: &SSTableIterator) -> &[u8] {
    let k = it.key().unwrap();
    &k[..k.len() - 8]
}

#[test]
fn test_corrupted_checksum() {
    let dir = tempdir().unwrap();
    let path = build_sorted_sst(dir.path(), &[(b"k", b"v", 1)]);
    let mut bytes = fs::read(&path).unwrap();
    if bytes.len() > 20 {
        bytes[10] ^= 0xff;
    }
    fs::write(&path, &bytes).unwrap();
    assert!(SSTableReader::open(&path, None).is_err());
}

#[test]
fn test_corrupted_compression_type() {
    let dir = tempdir().unwrap();
    let path = build_sorted_sst(dir.path(), &[(b"k", b"v", 1)]);
    let mut bytes = fs::read(&path).unwrap();
    let footer_off = bytes.len() - FOOTER_SIZE;
    let index_off =
        u64::from_le_bytes(bytes[footer_off + 16..footer_off + 24].try_into().unwrap()) as usize;
    let index_size =
        u64::from_le_bytes(bytes[footer_off + 24..footer_off + 32].try_into().unwrap()) as usize;
    let type_off = index_off + index_size - 5;
    bytes[type_off] = 99;
    fs::write(&path, &bytes).unwrap();
    assert!(matches!(
        SSTableReader::open(&path, None),
        Err(Error::Corruption(_))
    ));
}

#[test]
fn test_no_bloom_open() {
    let dir = tempdir().unwrap();
    let path = build_sorted_sst(dir.path(), &[(b"x", b"y", 1)]);
    let r = SSTableReader::open(&path, None).unwrap();
    let seek = encode_internal_key(b"x", 1, ValueType::TypePut);
    assert!(r.get(&seek).unwrap().is_some());
}

#[test]
fn test_concurrent_reads() {
    let dir = tempdir().unwrap();
    let path = build_sorted_sst(dir.path(), &[(b"shared", b"v", 1)]);
    let r = Arc::new(SSTableReader::open(&path, None).unwrap());
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let r2 = Arc::clone(&r);
            thread::spawn(move || {
                let seek = encode_internal_key(b"shared", 1, ValueType::TypePut);
                r2.get(&seek).unwrap().is_some()
            })
        })
        .collect();
    for h in handles {
        assert!(h.join().unwrap());
    }
}

#[test]
fn test_crash_atomicity() {
    let dir = tempdir().unwrap();
    let path = sstable_path(dir.path(), 9, 0);
    let tmp = path.with_extension("sst.tmp");
    let mut b = SSTableBuilder::new(&path, 4096, 16, CompressionType::None, 0.0).unwrap();
    let k = encode_internal_key(b"k", 1, ValueType::TypePut);
    b.add(&k, b"v").unwrap();
    drop(b);
    assert!(tmp.exists());
    assert!(!path.exists());
    assert!(
        SSTableReader::open(&tmp, None).is_err()
            || tmp.metadata().unwrap().len() < FOOTER_SIZE as u64
    );
}

#[test]
fn test_block_iterator_prev() {
    let mut b = BlockBuilder::new(2);
    for i in 0..5u8 {
        let k = ik(format!("key_{i:02}").as_bytes(), i as u64 + 1);
        b.add(&k, &[i]).unwrap();
    }
    let block = Block::new(b.finish()).unwrap();
    let mut it = block.iter();
    // Walk to the last entry
    let mut count = 0;
    while it.valid() {
        count += 1;
        if !it.advance() {
            break;
        }
    }
    assert_eq!(count, 5);
    // it is now invalid (past the end), seek_to_last
    it.seek_to_last();
    assert!(it.valid());
    assert_eq!(it.key(), ik(b"key_04", 5).as_slice());
    assert!(it.prev());
    assert_eq!(it.key(), ik(b"key_03", 4).as_slice());
    assert!(it.prev());
    assert_eq!(it.key(), ik(b"key_02", 3).as_slice());
    assert!(it.prev());
    assert_eq!(it.key(), ik(b"key_01", 2).as_slice());
    assert!(it.prev());
    assert_eq!(it.key(), ik(b"key_00", 1).as_slice());
    // prev at first entry should fail
    assert!(!it.prev());
}

#[test]
fn test_block_iterator_prev_single_entry() {
    let mut b = BlockBuilder::new(16);
    let key = ik(b"only", 1);
    b.add(&key, b"val").unwrap();
    let block = Block::new(b.finish()).unwrap();
    let mut it = block.iter();
    assert!(it.valid());
    assert_eq!(it.key(), key.as_slice());
    // prev at first entry fails
    assert!(!it.prev());
    // advance and then prev back
    assert!(!it.advance());
    // should be at the end, need seek_to_last to go back
    it.seek_to_last();
    assert!(it.valid());
    assert_eq!(it.key(), key.as_slice());
    assert!(!it.prev());
}

#[test]
fn test_block_iterator_seek_to_last() {
    let mut b = BlockBuilder::new(3);
    for i in 0..4u8 {
        let k = ik(format!("k{i:02}").as_bytes(), i as u64 + 1);
        b.add(&k, &[i]).unwrap();
    }
    let block = Block::new(b.finish()).unwrap();
    let mut it = block.iter();
    it.seek_to_last();
    assert!(it.valid());
    assert_eq!(it.key(), ik(b"k03", 4).as_slice());
    assert_eq!(it.value(), &[3]);
    it.prev();
    assert_eq!(it.key(), ik(b"k02", 3).as_slice());
}

#[test]
fn test_sstable_iterator_prev() {
    let dir = tempdir().unwrap();
    let mut entries = Vec::new();
    for i in 0..10u64 {
        let uk = format!("key_{i:04}").into_bytes();
        let val = format!("val_{i}").into_bytes();
        entries.push((uk, val, i + 1));
    }
    let path = {
        let path = sstable_path(dir.path(), 1, 0);
        let mut b = SSTableBuilder::new(&path, 256, 2, CompressionType::None, 0.0).unwrap();
        for (uk, val, seq) in &entries {
            let ik = encode_internal_key(uk, *seq, ValueType::TypePut);
            b.add(&ik, val).unwrap();
        }
        b.finish().unwrap();
        path
    };
    let r = SSTableReader::open(&path, None).unwrap();
    let mut it = r.iter();
    // Seek to last
    it.seek_to_last();
    assert!(it.valid());
    let last_key = extract_user_key_from_it(&it);
    assert_eq!(last_key, b"key_0009");
    // Walk back
    it.prev();
    assert!(it.valid());
    let key = extract_user_key_from_it(&it);
    assert_eq!(key, b"key_0008");
    it.prev();
    let key = extract_user_key_from_it(&it);
    assert_eq!(key, b"key_0007");
    // Walk back to the beginning
    let expected_keys: Vec<&[u8]> = vec![
        b"key_0006",
        b"key_0005",
        b"key_0004",
        b"key_0003",
        b"key_0002",
        b"key_0001",
        b"key_0000",
    ];
    for expected in expected_keys {
        it.prev();
        assert!(it.valid(), "should be valid at {expected:?}");
        let actual = extract_user_key_from_it(&it);
        assert_eq!(actual, expected);
    }
    // No more prev
    assert!(!it.prev());
}

#[test]
fn test_sstable_iterator_seek_to_last_empty() {
    let dir = tempdir().unwrap();
    let path = sstable_path(dir.path(), 2, 0);
    let b = SSTableBuilder::new(&path, 4096, 16, CompressionType::None, 0.0).unwrap();
    let err = b.finish().unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
}

#[test]
fn get_does_not_return_adjacent_user_key() {
    let dir = tempdir().unwrap();
    let path = sstable_path(dir.path(), 1, 0);
    let mut b = SSTableBuilder::new(&path, 512, 16, CompressionType::None, 0.0).unwrap();
    b.add(&ik(b"a", 1), b"va").unwrap();
    b.add(&ik(b"c", 1), b"vc").unwrap();
    b.finish().unwrap();
    let reader = SSTableReader::open(&path, None).unwrap();
    let seek = ik(b"b", 10);
    assert_eq!(reader.get(&seek).unwrap(), None);
}
