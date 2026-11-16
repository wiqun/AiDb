//! @component aidb-cluster

use aidb::cluster::types::LogEntry;
use aidb::cluster::{RaftNodeConfig, Request};
use openraft::vote::leader_id_std::CommittedLeaderId;
use openraft::{EntryPayload, LogId};

#[test]
fn test_request_serde_roundtrip() {
    let entry = LogEntry {
        log_id: LogId::new(CommittedLeaderId::new(1), 1),
        payload: EntryPayload::Normal(Request::Put {
            key: b"k".to_vec(),
            value: bytes::Bytes::from(b"v".to_vec()),
        }),
    };
    let bytes = rmp_serde::to_vec(&entry).unwrap();
    let decoded: LogEntry = rmp_serde::from_slice(&bytes).unwrap();
    assert!(matches!(
        decoded.payload,
        EntryPayload::Normal(Request::Put { .. })
    ));
}

#[test]
fn test_request_to_batch_conversion() {
    let batch = Request::Put {
        key: b"a".to_vec(),
        value: bytes::Bytes::from(b"b".to_vec()),
    }
    .to_batch();
    assert_eq!(batch.len(), 1);
}

#[test]
fn test_raft_config_validation() {
    assert!(RaftNodeConfig::default().validate().is_ok());
    let cfg = RaftNodeConfig {
        election_timeout_min: 2000,
        ..Default::default()
    };
    assert!(cfg.validate().is_err());
}
