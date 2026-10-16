//! Raft 类型配置与请求/响应.

use serde::{Deserialize, Serialize};

pub use crate::error::ClusterError;

pub use crate::cluster::meta_types::MetaRequest;

pub type NodeId = u64;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        NodeId = u64,
        Node = openraft::BasicNode,
        Term = u64,
        LeaderId = openraft::impls::leader_id_std::LeaderId<Self::Term, Self::NodeId>,
        Entry = openraft::Entry<<Self::LeaderId as openraft::vote::RaftLeaderId>::Committed, Self::D, Self::NodeId, Self::Node>,
        AsyncRuntime = openraft_rt_tokio::TokioRuntime,
);

pub type LogEntry = <TypeConfig as openraft::RaftTypeConfig>::Entry;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ThinWriteOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ThinWriteBatch {
    pub ops: Vec<ThinWriteOp>,
}

impl ThinWriteBatch {
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.ops.push(ThinWriteOp::Put { key, value });
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        self.ops.push(ThinWriteOp::Delete { key });
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// 估算 rmp_serde 序列化后的大小.
    pub fn estimated_serialized_size(&self) -> usize {
        self.ops
            .iter()
            .map(|op| match op {
                ThinWriteOp::Put { key, value } => key.len() + value.len() + 16,
                ThinWriteOp::Delete { key } => key.len() + 10,
            })
            .sum::<usize>()
            + 10 // vec header overhead
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
// Note: intentionally without adjacently tagged serde — rmp_serde cannot roundtrip
// tagged enums nested in openraft::Entry; see test_request_serde_roundtrip.
pub enum Request {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    /// 条件 put: `sm_key` 已存在则跳过. 全量拷贝 (`SlotMigrationExecutor`)
    /// 专用. `migration_epoch` 为 `Some` 时, apply 内还会先查该 epoch 下
    /// `key` 的迁移 tombstone —— 若最后一次是 Del, 直接跳过 (不复活),
    /// 即 FIX-0056-A1 "PutConditional 尊重 Del tombstone" 不变式;
    /// `None` 时行为与迁移无关, 完全等同修复前 (向后兼容).
    PutConditional {
        key: Vec<u8>,
        value: Vec<u8>,
        migration_epoch: Option<u64>,
    },
    WriteBatch(ThinWriteBatch),
    Meta(MetaRequest),
    /// Slot 迁移收尾写屏障: apply 时不改用户数据, 仅推进 Raft last_applied.
    MigrationBarrier {
        token: u64,
    },
    /// FIX-0056-A1: 迁移期用户写 (`ops`) 与该 epoch 的 tombstone/tip 更新
    /// 打进同一 apply, 一次 entry 原子生效. `ops` 里每个 Put/Delete 都会
    /// 在 apply 内被分配一个单调递增的 seq, 写入
    /// `mig_tombstone_key(group, epoch, key)`, 并推进
    /// `mig_tip_key(group, epoch)` (见 `migration_oplog.rs`).
    MigrationWrite {
        epoch: u64,
        ops: ThinWriteBatch,
    },
    /// FIX-0056-A1: 删除 target group 上 `epoch` 对应的全部
    /// `mig/{gid}/{epoch}/*` tombstone/tip key (Commit/Cancel 后 GC).
    /// 复制生效 (与用户写一样走 Raft apply), 而非旁路本地删除.
    MigrationGc {
        epoch: u64,
    },
}

impl std::fmt::Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Request::Put { key, .. } => write!(f, "Put(key_len={})", key.len()),
            Request::Delete { key } => write!(f, "Delete(key_len={})", key.len()),
            Request::PutConditional { key, .. } => {
                write!(f, "PutConditional(key_len={})", key.len())
            }
            Request::WriteBatch(batch) => write!(f, "WriteBatch(ops={})", batch.len()),
            Request::Meta(_) => write!(f, "Meta"),
            Request::MigrationBarrier { token } => write!(f, "MigrationBarrier({})", token),
            Request::MigrationWrite { epoch, ops } => {
                write!(f, "MigrationWrite(epoch={}, ops={})", epoch, ops.len())
            }
            Request::MigrationGc { epoch } => write!(f, "MigrationGc(epoch={})", epoch),
        }
    }
}

impl std::fmt::Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Response::Ok => write!(f, "Ok"),
            Response::Value(None) => write!(f, "Value(None)"),
            Response::Value(Some(v)) => write!(f, "Value(len={})", v.len()),
            Response::Error(msg) => write!(f, "Error({})", msg),
        }
    }
}

impl Request {
    pub fn to_batch(self) -> ThinWriteBatch {
        match self {
            Request::Put { key, value } => {
                let mut batch = ThinWriteBatch::new();
                batch.put(key, value);
                batch
            }
            Request::Delete { key } => {
                let mut batch = ThinWriteBatch::new();
                batch.delete(key);
                batch
            }
            Request::PutConditional { .. }
            | Request::Meta(_)
            | Request::MigrationBarrier { .. }
            | Request::MigrationGc { .. } => ThinWriteBatch::new(),
            Request::WriteBatch(batch) => batch,
            Request::MigrationWrite { ops, .. } => ops,
        }
    }

    /// 估算 rmp_serde 序列化后的大小, 避免完整序列化后丢弃.
    /// 上界估计 (保守略大), 用于 max_entry_size 校验.
    pub fn estimated_serialized_size(&self) -> usize {
        match self {
            Request::Put { key, value } => key.len() + value.len() + 16,
            Request::PutConditional { key, value, .. } => key.len() + value.len() + 24,
            Request::Delete { key } => key.len() + 10,
            Request::WriteBatch(batch) => batch.estimated_serialized_size(),
            Request::Meta(_) => 512,
            Request::MigrationBarrier { .. } => 24,
            // 每个 op 除自身大小外, 还多写一条 tombstone (user_key + 9 字节值 +
            // 编码 overhead); 外加一次 tip 更新.
            Request::MigrationWrite { ops, .. } => ops.estimated_serialized_size() * 2 + 24,
            Request::MigrationGc { .. } => 24,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Ok,
    Value(Option<Vec<u8>>),
    Error(String),
}

/// Raft 节点运行时配置
pub use crate::cluster::log_committer::LogCommitterConfig;

#[derive(Debug, Clone)]
pub struct RaftNodeConfig {
    pub node_id: NodeId,
    pub group_id: u64,
    pub election_timeout_min: u64,
    pub election_timeout_max: u64,
    pub heartbeat_interval: u64,
    pub max_payload_entries: u64,
    pub snapshot_logs_since_last: u64,
    /// 当 leader propose 成功累积的写估算字节数超此阈值时触发 snapshot (None = 禁用).
    pub snapshot_size_threshold: Option<u64>,
    /// 是否启用 linearizable read (ReadIndex).
    /// 启用后每次 get() 触发 quorum 心跳 + applied index 等待.
    pub linearizable_read: bool,
    pub max_entry_size: u64,
    pub rpc_timeout_ms: u64,
    pub grpc_max_message_size: u64,
    /// LogCommitter 配置 (Some 则启用异步批量写入, None 则为同步旧路径).
    pub log_committer_config: Option<LogCommitterConfig>,
}

impl Default for RaftNodeConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            group_id: crate::cluster::DEFAULT_GROUP_ID,
            election_timeout_min: 500,
            election_timeout_max: 1000,
            heartbeat_interval: 100,
            max_payload_entries: 512,
            snapshot_logs_since_last: 1000,
            snapshot_size_threshold: None,
            linearizable_read: false,
            max_entry_size: 8 * 1024 * 1024,
            rpc_timeout_ms: 200,
            grpc_max_message_size: 64 * 1024 * 1024,
            log_committer_config: Some(LogCommitterConfig::default()),
        }
    }
}

impl RaftNodeConfig {
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.election_timeout_min >= self.election_timeout_max {
            return Err(ClusterError::InvalidConfig(
                "election_timeout_min must be < election_timeout_max".into(),
            ));
        }
        if self.heartbeat_interval >= self.election_timeout_min {
            return Err(ClusterError::InvalidConfig(
                "heartbeat_interval must be < election_timeout_min".into(),
            ));
        }
        if self.max_payload_entries == 0 {
            return Err(ClusterError::InvalidConfig(
                "max_payload_entries must be > 0".into(),
            ));
        }
        if self.rpc_timeout_ms >= self.election_timeout_min {
            return Err(ClusterError::InvalidConfig(
                "rpc_timeout_ms must be < election_timeout_min".into(),
            ));
        }
        if let Some(0) = self.snapshot_size_threshold {
            return Err(ClusterError::InvalidConfig(
                "snapshot_size_threshold must be > 0".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_serde_roundtrip() {
        use openraft::vote::leader_id_std::CommittedLeaderId;
        use openraft::{EntryPayload, LogId};
        let entry = LogEntry {
            log_id: LogId::new(CommittedLeaderId::new(1), 1),
            payload: EntryPayload::Normal(Request::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
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
            value: b"b".to_vec(),
        }
        .to_batch();
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn test_raft_config_default_values() {
        let cfg = RaftNodeConfig::default();
        assert_eq!(cfg.max_payload_entries, 512);
        assert_eq!(cfg.max_entry_size, 8 * 1024 * 1024);
        assert_eq!(cfg.rpc_timeout_ms, 200);
        assert_eq!(cfg.grpc_max_message_size, 64 * 1024 * 1024);
        assert_eq!(cfg.heartbeat_interval, 100);
        assert_eq!(cfg.election_timeout_min, 500);
        assert_eq!(cfg.election_timeout_max, 1000);
        assert_eq!(cfg.snapshot_logs_since_last, 1000);
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
}
