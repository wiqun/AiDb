//! WAL 模块级 dataflow — Writer 写入与同步落盘
//! @component aidb-wal

use aidb::engine::wal::reader::Reader;
use aidb::engine::wal::record::RecordType;
use aidb::engine::wal::writer::Writer;
use tempfile::tempdir;

/// 验证 WAL Writer 写入、同步后文件持久化且可被 Reader 完整读回
#[test]
fn test_wal_event_order() {
    let d = tempdir().unwrap();
    let path = d.path().join("order.log");
    {
        let mut w = Writer::open(&path).unwrap();
        w.write_record(RecordType::Full, b"t").unwrap();
        w.sync_data().unwrap();
    }

    let meta = std::fs::metadata(&path).unwrap();
    assert!(
        meta.len() > 0,
        "WAL file must be non-empty after write+sync"
    );

    let mut r = Reader::open(&path).unwrap();
    let rec = r.read_record().unwrap();
    assert!(matches!(
        rec,
        aidb::engine::wal::reader::ReadStatus::Record(RecordType::Full, ref data) if data == b"t"
    ));
}
