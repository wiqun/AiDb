//! @component aidb-engine
//! PinnableSlice 零拷贝测试

use bytes::Bytes;
use tempfile::tempdir;

use aidb::config::Options;
use aidb::{PinnableSlice, DB};

#[test]
fn test_pinnable_slice_variants_and_conversions() {
    let raw = b"hello_world";
    let p_borrowed = PinnableSlice::Borrowed(raw);
    assert_eq!(p_borrowed.as_ref(), raw);
    assert_eq!(p_borrowed.as_slice(), raw);
    assert_eq!(&*p_borrowed, raw);
    assert_eq!(p_borrowed.clone().into_vec(), raw.to_vec());
    assert_eq!(p_borrowed.into_bytes(), Bytes::from_static(raw));

    let bytes = Bytes::from_static(b"shared_bytes");
    let p_shared = PinnableSlice::Shared(bytes.clone());
    assert_eq!(p_shared.as_ref(), b"shared_bytes");
    assert_eq!(p_shared.clone().into_bytes(), bytes);
    assert_eq!(p_shared.into_vec(), b"shared_bytes".to_vec());

    let vec = b"owned_vec".to_vec();
    let p_owned = PinnableSlice::Owned(vec.clone());
    assert_eq!(p_owned.as_ref(), b"owned_vec");
    assert_eq!(p_owned.clone().into_vec(), vec);
    assert_eq!(p_owned.into_bytes(), Bytes::from_static(b"owned_vec"));
}

#[test]
fn test_db_get_pinned_zero_copy_flow() {
    let dir = tempdir().unwrap();
    let mut opts = Options::for_testing();
    opts.row_cache_size = 1024 * 1024;
    let db = DB::open(dir.path(), opts).unwrap();

    let key = b"pin_key";
    let val = b"pin_value";

    db.put(key, val).unwrap();

    // 第一次读取: 未命中 RowCache, 读取 LSM 并回填
    let p1 = db.get_pinned(key).unwrap().expect("must find key");
    assert_eq!(p1.as_ref(), val);
    assert!(matches!(p1, PinnableSlice::Shared(_)));

    // 第二次读取: 命中 RowCache, 直接返回 Shared(Bytes)
    let p2 = db.get_pinned(key).unwrap().expect("must find key");
    assert_eq!(p2.as_ref(), val);
    assert!(matches!(p2, PinnableSlice::Shared(_)));

    // 传统 get() 结果一致
    let v = db.get(key).unwrap().expect("must find key");
    assert_eq!(v.as_slice(), val);
}
