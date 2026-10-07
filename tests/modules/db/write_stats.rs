//! 写路径不再为精确键数做存在性分类.
//! @component aidb-engine

fn assert_unit(_: ()) {}

#[test]
fn write_path_does_not_classify_keys() {
    let dir = tempfile::tempdir().unwrap();
    let db = aidb::DB::open(dir.path(), aidb::config::Options::for_testing()).unwrap();
    assert_unit(db.put(b"k", b"v1").unwrap());
    db.flush().unwrap();
    let sst_entries_before = db.estimate_entry_count();
    assert_unit(db.put(b"k", b"v2").unwrap());
    assert_eq!(db.estimate_entry_count(), sst_entries_before + 1);
    assert!(db.key_exists(b"k").unwrap());

    let mut wb = aidb::WriteBatch::new();
    wb.put(b"k", b"a");
    wb.delete(b"k");
    wb.put(b"k", b"b");
    assert_unit(db.write(&wb).unwrap());
    assert!(db.key_exists(b"k").unwrap());

    let before = db.estimate_entry_count();
    let mut cover = aidb::WriteBatch::new();
    cover.put(b"k", b"v3");
    assert_unit(db.write_without_wal(&cover).unwrap());
    assert_eq!(db.estimate_entry_count(), before + 1);
}
