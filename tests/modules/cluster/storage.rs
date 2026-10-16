//! @component aidb-cluster

use aidb::cluster::{OpenRaftStorage, DEFAULT_GROUP_ID};
use aidb::config::Options;
use aidb::DB;
use tempfile::TempDir;

fn test_storage() -> (OpenRaftStorage, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = DB::open(dir.path(), Options::for_testing()).unwrap();
    (
        OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap(),
        dir,
    )
}

#[test]
fn test_storage_state_default() {
    let (storage, _dir) = test_storage();
    assert_eq!(storage.group_id(), DEFAULT_GROUP_ID);
}

#[test]
fn raft_storage_open_new_db() {
    let (_storage, _dir) = test_storage();
}

#[test]
fn raft_storage_sm_read_empty() {
    let (storage, _dir) = test_storage();
    assert!(storage
        .get_state_machine_value(b"missing")
        .unwrap()
        .is_none());
}

#[test]
fn apply_put_does_not_report_inserted() {
    use aidb::cluster::types::{LogEntry, Request, Response};
    use openraft::vote::leader_id_std::CommittedLeaderId;
    use openraft::{EntryPayload, LogId};

    let (storage, _dir) = test_storage();
    let first = LogEntry {
        log_id: LogId::new(CommittedLeaderId::new(1), 1),
        payload: EntryPayload::Normal(Request::Put {
            key: b"k".to_vec(),
            value: b"v1".to_vec(),
        }),
    };
    let second = LogEntry {
        log_id: LogId::new(CommittedLeaderId::new(1), 2),
        payload: EntryPayload::Normal(Request::Put {
            key: b"k".to_vec(),
            value: b"v2".to_vec(),
        }),
    };
    let responses = storage.apply_entries_internal(&[first, second]).unwrap();
    assert_eq!(responses.len(), 2);
    assert!(matches!(responses[0], Response::Ok));
    assert!(matches!(responses[1], Response::Ok));
    assert_eq!(
        storage.get_state_machine_value(b"k").unwrap(),
        Some(b"v2".to_vec())
    );
}
