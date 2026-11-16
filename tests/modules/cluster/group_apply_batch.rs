//! ISSUE-005 — 数据 Group apply: SM + last_applied 单 WriteBatch 原子持久化 (B1.2 模板)
//! @component aidb-cluster
//!

use aidb::cluster::types::{LogEntry, Request, ThinWriteBatch};
use aidb::cluster::{OpenRaftStorage, DEFAULT_GROUP_ID};
use aidb::config::Options;
use aidb::DB;
use openraft::storage::RaftStateMachine;
use openraft::vote::leader_id_std::CommittedLeaderId;
use openraft::{BasicNode, EntryPayload, LogId, Membership};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use tempfile::TempDir;

fn cluster_opts() -> Options {
    let mut o = Options::for_testing();
    o.sync_wal = true;
    o
}

async fn reopen_storage(path: &Path) -> OpenRaftStorage {
    let db = DB::open(path, cluster_opts()).unwrap();
    OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap()
}

async fn last_applied_index(storage: &mut OpenRaftStorage) -> Option<u64> {
    storage.applied_state().await.unwrap().0.map(|id| id.index)
}

fn put_entry(index: u64, key: &[u8], value: &[u8]) -> LogEntry {
    LogEntry {
        log_id: LogId::new(CommittedLeaderId::new(1), index),
        payload: EntryPayload::Normal(Request::Put {
            key: key.to_vec(),
            value: bytes::Bytes::from(value.to_vec()),
        }),
    }
}

#[tokio::test]
async fn test_data_put_last_applied_consistent_after_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = DB::open(&path, cluster_opts()).unwrap();
        let storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();
        storage
            .apply_entries_internal(&[put_entry(1, b"k1", b"v1")])
            .unwrap();
        // apply 只写 memtable (write_without_wal), 崩溃恢复依赖 raft log 重放.
        // 本测试直接调 apply_entries_internal 绕过 raft log, 故需显式 flush 让
        // 状态机数据落 SST, 否则 reopen 时 memtable 未持久化会丢失.
        storage.db().flush().unwrap();
    }

    let mut storage = reopen_storage(&path).await;
    assert_eq!(
        storage.get_state_machine_value(b"k1").unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(last_applied_index(&mut storage).await, Some(1));
}

#[tokio::test]
async fn test_write_batch_entry_atomic_with_last_applied() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = DB::open(&path, cluster_opts()).unwrap();
        let storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();
        let mut wb = ThinWriteBatch::new();
        for i in 0..4 {
            wb.put(format!("k{i}").into_bytes(), format!("v{i}").into_bytes());
        }
        let entry = LogEntry {
            log_id: LogId::new(CommittedLeaderId::new(1), 1),
            payload: EntryPayload::Normal(Request::WriteBatch(wb)),
        };
        storage.apply_entries_internal(&[entry]).unwrap();
        storage.db().flush().unwrap();
    }

    let mut storage = reopen_storage(&path).await;
    for i in 0..4 {
        assert_eq!(
            storage
                .get_state_machine_value(format!("k{i}").as_bytes())
                .unwrap(),
            Some(format!("v{i}").into_bytes())
        );
    }
    assert_eq!(last_applied_index(&mut storage).await, Some(1));
}

#[tokio::test]
async fn test_multi_entry_apply_sequential_consistency() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = DB::open(&path, cluster_opts()).unwrap();
        let storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();
        storage
            .apply_entries_internal(&[
                put_entry(1, b"a", b"1"),
                put_entry(2, b"b", b"2"),
                put_entry(3, b"c", b"3"),
            ])
            .unwrap();
        storage.db().flush().unwrap();
    }

    let mut storage = reopen_storage(&path).await;
    assert_eq!(
        storage.get_state_machine_value(b"a").unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(
        storage.get_state_machine_value(b"b").unwrap(),
        Some(b"2".to_vec())
    );
    assert_eq!(
        storage.get_state_machine_value(b"c").unwrap(),
        Some(b"3".to_vec())
    );
    assert_eq!(last_applied_index(&mut storage).await, Some(3));
}

#[tokio::test]
async fn test_membership_and_last_applied_atomic() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = DB::open(&path, cluster_opts()).unwrap();
        let storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();
        let mut voters = BTreeSet::new();
        voters.insert(1);
        voters.insert(2);
        let mut nodes = BTreeMap::new();
        for id in [1u64, 2] {
            nodes.insert(
                id,
                BasicNode {
                    addr: format!("http://127.0.0.1:{id}"),
                },
            );
        }
        let membership = Membership::new(vec![voters], nodes).expect("valid membership");
        let entry = LogEntry {
            log_id: LogId::new(CommittedLeaderId::new(1), 5),
            payload: EntryPayload::Membership(membership),
        };
        storage.apply_entries_internal(&[entry]).unwrap();
        storage.db().flush().unwrap();
    }

    let mut storage = reopen_storage(&path).await;
    let (last_applied, stored_membership) = storage.applied_state().await.unwrap();
    assert_eq!(stored_membership.log_id().map(|id| id.index), Some(5));
    assert_eq!(last_applied.map(|id| id.index), Some(5));
}

#[tokio::test]
async fn test_put_conditional_skips_existing_key() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = DB::open(&path, cluster_opts()).unwrap();
        let storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();
        storage
            .apply_entries_internal(&[put_entry(1, b"k", b"v1")])
            .unwrap();
        let conditional = LogEntry {
            log_id: LogId::new(CommittedLeaderId::new(1), 2),
            payload: EntryPayload::Normal(Request::PutConditional {
                key: b"k".to_vec(),
                value: bytes::Bytes::from(b"v2".to_vec()),
                migration_epoch: None,
            }),
        };
        storage.apply_entries_internal(&[conditional]).unwrap();
        storage.db().flush().unwrap();
    }

    let mut storage = reopen_storage(&path).await;
    assert_eq!(
        storage.get_state_machine_value(b"k").unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(last_applied_index(&mut storage).await, Some(2));
}

#[tokio::test]
async fn test_apply_idempotent_after_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let entry = put_entry(1, b"k", b"v");
    {
        let db = DB::open(&path, cluster_opts()).unwrap();
        let storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();
        storage
            .apply_entries_internal(std::slice::from_ref(&entry))
            .unwrap();
        storage.apply_entries_internal(&[entry]).unwrap();
        storage.db().flush().unwrap();
    }

    let mut storage = reopen_storage(&path).await;
    assert_eq!(
        storage.get_state_machine_value(b"k").unwrap(),
        Some(b"v".to_vec())
    );
    assert_eq!(last_applied_index(&mut storage).await, Some(1));
}
