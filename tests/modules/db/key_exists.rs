//! key_exists: 完整存在性判定 (不物化 Value)
//! @component aidb-engine

#[test]
fn key_exists_sees_key_after_flush_to_sst() {
    let dir = tempfile::tempdir().unwrap();
    let db = aidb::DB::open(dir.path(), aidb::config::Options::for_testing()).unwrap();
    let big = vec![0u8; 256 * 1024];
    db.put(b"big", &big).unwrap();
    db.flush().unwrap();
    assert!(db.key_exists(b"big").unwrap());
    assert!(!db.key_exists(b"missing").unwrap());
    assert_eq!(db.get(b"big").unwrap().as_deref(), Some(big.as_slice()));
}

#[test]
fn key_exists_without_stats_does_not_bump_block_seek() {
    let dir = tempfile::tempdir().unwrap();
    let db = aidb::DB::open(dir.path(), aidb::config::Options::for_testing()).unwrap();
    let big = vec![0u8; 256 * 1024];
    db.put(b"big", &big).unwrap();
    db.flush().unwrap();
    let before = db.statistics().snapshot().block_seek_count;
    assert!(db.key_exists_without_stats(b"big").unwrap());
    assert_eq!(db.statistics().snapshot().block_seek_count, before);
    assert!(db.key_exists(b"big").unwrap());
    assert!(db.statistics().snapshot().block_seek_count > before);
}

#[test]
fn key_exists_false_after_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = aidb::DB::open(dir.path(), aidb::config::Options::for_testing()).unwrap();
    db.put(b"k", b"v").unwrap();
    db.delete(b"k").unwrap();
    assert!(!db.key_exists(b"k").unwrap());
}
