//! Raft snapshot 构建与安装 — 以 DB 目录文件级打包传输 (借鉴 Kvrocks/TiKV),
//! 替代逐条 KV 拷贝, 避免 build 端全量 scan 与 install 端逐条 put 的开销.
//!
//! # 数据流
//!
//! ```text
//! build (OpenRaftSnapshotBuilder / get_current_snapshot)
//!   ├─ db.flush() -> 数据全部进 SST
//!   ├─ checkpoint 协议 pin SST (SnapshotCheckpointGuard)
//!   ├─ 收集文件 -> postcard(SnapshotBundle { relative_path, data })
//!   └─ meta = SnapshotMeta { last_log_id, last_membership }
//!
//! install (install_snapshot_atomic)
//!   ├─ 先写临时文件 + fsync 父目录 (crash 安全)
//!   ├─ 关闭旧 DB -> 清空目录 -> 写入接收文件 -> DB::open 重开
//!   ├─ 写 snapshot_meta / last_applied
//!   ├─ 重建 MetaStateMachine (旧实例持有已关闭 DB)
//!   └─ 清空 overlay 与内存态 -> load_state 重载
//! ```
//!
//! # Invariant
//!
//! - install 是"目录整体替换"式原子操作, 失败时清理临时文件.
//! - snapshot 安装后所有 in-memory 缓存 (overlay / StorageState) 一律作废重载.
//! - MetaRaft 也有快照: 目录打包天然包含 `\x00meta_raft/*`, 安装后重建
//!   `MetaStateMachine` 恢复集群元数据.

use std::io::Cursor;
use std::sync::Arc;

use openraft::{Snapshot, SnapshotMeta};
use serde::{Deserialize, Serialize};

use crate::cluster::storage::keys::snapshot_meta_key;
use crate::cluster::types::TypeConfig;
use crate::error::{ClusterError, Error, Result};
use crate::DB;

use super::{CLId, LIdOf, MOf, OpenRaftStorage, SMOf};

/// snapshot 传输格式: 由原来的逐条 KV (SnapshotKv) 改为 DB 目录文件级打包.
///
/// 借鉴 Kvrocks/TiKV 的做法: flush → SST 文件硬链接 → 发送文件内容 → 接收端替换 DB 目录.
/// 避免 build 端的 db.scan() 全量迭代开销和 install 端的逐条 batch.put() 开销.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SnapshotBundle {
    files: Vec<SnapshotFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SnapshotFile {
    /// DB 目录下的相对路径, 如 "CURRENT", "MANIFEST-000001", "000005_L0.sst"
    pub(crate) relative_path: String,
    pub(crate) data: Vec<u8>,
}

impl SnapshotBundle {
    fn from_files(db: &DB) -> Result<Self> {
        let _pinned = db.pin_sstables();
        let file_paths = db.collect_checkpoint_file_paths()?;
        let db_path = db.path();

        let mut files = Vec::with_capacity(file_paths.len());
        for src_path in &file_paths {
            let relative = src_path
                .strip_prefix(db_path)
                .map_err(|_| Error::InvalidArgument("checkpoint file outside db dir".into()))?
                .to_string_lossy()
                .to_string();
            let data = std::fs::read(src_path)?;
            files.push(SnapshotFile {
                relative_path: relative,
                data,
            });
        }
        Ok(SnapshotBundle { files })
    }
}

struct SnapshotCheckpointGuard<'a> {
    db: &'a DB,
}

impl<'a> SnapshotCheckpointGuard<'a> {
    fn new(db: &'a DB) -> Self {
        db.enter_checkpoint();
        Self { db }
    }
}

impl Drop for SnapshotCheckpointGuard<'_> {
    fn drop(&mut self) {
        self.db.leave_checkpoint();
    }
}

/// 构建文件级 snapshot bundle.
/// 先 flush 确保所有数据在 SST 文件中, 再用 checkpoint 协议读文件内容.
pub(crate) fn prepare_snapshot_bundle(db: &DB) -> Result<Vec<u8>> {
    db.flush()?;

    let _guard = SnapshotCheckpointGuard::new(db);
    let bundle = SnapshotBundle::from_files(db)?;

    postcard::to_allocvec(&bundle)
        .map_err(|e| Error::Cluster(ClusterError::Serialization(e.to_string())))
}

pub struct OpenRaftSnapshotBuilder {
    db: std::sync::Arc<DB>,
    group_id: u64,
}

impl OpenRaftSnapshotBuilder {
    pub fn new(db: std::sync::Arc<DB>, group_id: u64) -> Self {
        Self { db, group_id }
    }
}

impl OpenRaftStorage {
    /// 文件级 snapshot 安装: 关闭旧 DB → 清空目录 → 写入新文件 → 重新打开 DB.
    pub(crate) fn install_snapshot_atomic(&mut self, meta: &SMOf, data: &[u8]) -> Result<()> {
        let bundle: SnapshotBundle = postcard::from_bytes(data)
            .map_err(|e| Error::Cluster(ClusterError::Serialization(e.to_string())))?;

        // 关闭旧 DB, 终止后台线程 (flush/compaction).
        self.db.close()?;

        let db_path = self.db.path().to_path_buf();
        let options = Arc::clone(self.db.options());

        // 清空 DB 目录, 保留目录本身.
        if db_path.exists() {
            let dir_entries: Vec<_> = std::fs::read_dir(&db_path)
                .map_err(Error::Io)?
                .filter_map(|e| e.ok())
                .collect();
            for entry in dir_entries {
                let path = entry.path();
                if path.is_dir() {
                    std::fs::remove_dir_all(&path)?;
                } else {
                    std::fs::remove_file(&path)?;
                }
            }
        }

        // 写入接收到的文件.
        for file in &bundle.files {
            let dest = db_path.join(&file.relative_path);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&dest, &file.data)?;
        }

        // 重新打开 DB: CURRENT + MANIFEST + SST 文件已在目录中.
        let new_db = DB::open(&db_path, (*options).clone())?;
        self.db = new_db;

        // 写入 snapshot meta 等元数据 (与原逻辑相同).
        let meta_bytes = postcard::to_allocvec(meta)
            .map_err(|e| Error::Cluster(ClusterError::Serialization(e.to_string())))?;
        self.db
            .put(&snapshot_meta_key(self.group_id), &meta_bytes)?;
        if let Some(log_id) = meta.last_log_id {
            let la = rmp_serde::to_vec(&log_id)
                .map_err(|e| Error::Cluster(ClusterError::Serialization(e.to_string())))?;
            self.db
                .put(&super::keys::last_applied_key(self.group_id), &la)?;
        }

        // 重建 MetaStateMachine (旧实例持有已关闭的 DB 引用).
        if self.meta_state.is_some() {
            let new_meta = std::sync::Arc::new(
                crate::cluster::meta_state_machine::MetaStateMachine::new(Arc::clone(&self.db))?,
            );
            self.meta_state = Some(new_meta);
        }

        let mut state = self.state.write();
        state.snapshot_meta = Some(meta.clone());
        state.last_applied = meta.last_log_id;
        if let Some(log_id) = meta.last_log_id {
            state.last_purged_log_id = Some(log_id);
        }
        Ok(())
    }
}

#[cfg(feature = "cluster")]
impl openraft::RaftSnapshotBuilder<TypeConfig> for OpenRaftSnapshotBuilder {
    type SnapshotData = std::io::Cursor<Vec<u8>>;

    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<
        Snapshot<CLId, u64, openraft::BasicNode, Cursor<Vec<u8>>>,
        std::io::Error,
    > {
        let data =
            prepare_snapshot_bundle(&self.db).map_err(|e| std::io::Error::other(e.to_string()))?;

        let membership: MOf = self
            .db
            .get(&super::keys::membership_key(self.group_id))
            .map_err(|e| std::io::Error::other(e.to_string()))?
            .map(|d| postcard::from_bytes(&d).unwrap_or_default())
            .unwrap_or_default();

        let last_applied: Option<LIdOf> = self
            .db
            .get(&super::keys::last_applied_key(self.group_id))
            .map_err(|e| std::io::Error::other(e.to_string()))?
            .and_then(|d| rmp_serde::from_slice(&d).ok());

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
        };

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::meta_state_machine::MetaStateMachine;
    use crate::cluster::meta_types::{MetaRequest, METARAFT_GROUP_ID};
    use crate::cluster::storage::keys::DEFAULT_GROUP_ID;
    use crate::cluster::types::ThinWriteBatch;
    use crate::config::Options;
    use crate::DB;
    use openraft::{LogId, SnapshotMeta, StoredMembership};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_snapshot_install() {
        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let mut storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();

        // 先写入旧数据
        let mut batch = ThinWriteBatch::new();
        batch.put(b"old".to_vec(), b"gone".to_vec());
        storage.apply_batch_to_sm(&batch).unwrap();

        // 构建 snapshot bundle (模拟 SNAPSHOT 构建端).
        let bundle_data = prepare_snapshot_bundle(&storage.db).unwrap();

        let log_id = LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(5), 5);
        let meta = SnapshotMeta {
            last_log_id: Some(log_id),
            last_membership: StoredMembership::default(),
        };
        storage
            .install_snapshot_atomic(&meta, &bundle_data)
            .unwrap();

        assert_eq!(
            storage.get_state_machine_value(b"old").unwrap(),
            Some(b"gone".to_vec())
        );
        assert_eq!(
            storage
                .read_last_applied_from_db()
                .unwrap()
                .map(|id| id.index),
            Some(5)
        );
    }

    #[test]
    fn test_install_snapshot_atomic_crash_safety() {
        let dir = TempDir::new().unwrap();
        let opts = Options::for_testing();

        // 源 DB: 写入数据, 构建 bundle.
        let src_dir = TempDir::new().unwrap();
        let snap_data = {
            let db = DB::open(src_dir.path(), opts.clone()).unwrap();
            let storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();
            let mut b = ThinWriteBatch::new();
            b.put(b"key_a".to_vec(), b"val_a".to_vec());
            b.put(b"key_b".to_vec(), b"val_b".to_vec());
            storage.apply_batch_to_sm(&b).unwrap();
            prepare_snapshot_bundle(&storage.db).unwrap()
        };

        // 安装端: 安装到全新 DB 目录.
        {
            let db = DB::open(dir.path(), opts.clone()).unwrap();
            let mut storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();

            let log_id = LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(5), 5);
            let meta = SnapshotMeta {
                last_log_id: Some(log_id),
                last_membership: StoredMembership::default(),
            };
            storage.install_snapshot_atomic(&meta, &snap_data).unwrap();

            assert_eq!(
                storage.get_state_machine_value(b"key_a").unwrap(),
                Some(b"val_a".to_vec())
            );
            assert_eq!(
                storage.get_state_machine_value(b"key_b").unwrap(),
                Some(b"val_b".to_vec())
            );
            assert_eq!(
                storage
                    .read_last_applied_from_db()
                    .unwrap()
                    .map(|id| id.index),
                Some(5)
            );
        }

        // 重新打开 DB, 确认数据持久化.
        {
            let db = DB::open(dir.path(), opts).unwrap();
            let storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();
            assert_eq!(
                storage.get_state_machine_value(b"key_a").unwrap(),
                Some(b"val_a".to_vec())
            );
            assert_eq!(
                storage.get_state_machine_value(b"key_b").unwrap(),
                Some(b"val_b".to_vec())
            );
        }
    }

    #[test]
    fn test_meta_snapshot_roundtrip() {
        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let meta_sm = Arc::new(MetaStateMachine::new(db.clone()).unwrap());

        // Register a node via MetaStateMachine + persist ApplyOutput to DB
        let output = meta_sm
            .apply_meta_request(MetaRequest::RegisterNode {
                node_id: 1,
                rpc_addr: "http://127.0.0.1:1".into(),
                client_addr: None,
                tags: HashMap::new(),
            })
            .unwrap();
        {
            let mut wb = crate::engine::db::WriteBatch::new();
            for (k, v) in &output.kv_pairs {
                wb.put(k.clone(), v.clone());
            }
            db.write(&wb).unwrap();
        }

        let bundle_data = prepare_snapshot_bundle(&db).unwrap();

        // Fresh DB + install snapshot
        let dir2 = TempDir::new().unwrap();
        let db2 = DB::open(dir2.path(), Options::for_testing()).unwrap();
        let meta_sm2 = Arc::new(MetaStateMachine::new(db2.clone()).unwrap());
        let mut storage2 =
            OpenRaftStorage::new(db2.clone(), METARAFT_GROUP_ID, Some(meta_sm2.clone())).unwrap();

        let log_id = LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(3), 3);
        let snap_meta = SnapshotMeta {
            last_log_id: Some(log_id),
            last_membership: StoredMembership::default(),
        };
        storage2
            .install_snapshot_atomic(&snap_meta, &bundle_data)
            .unwrap();

        // Verify state reloaded correctly
        let recovered = storage2.meta_state.as_ref().unwrap().get_cluster_meta();
        assert_eq!(recovered.nodes.len(), 1);
        assert!(recovered.nodes.contains_key(&1));
        assert_eq!(recovered.cluster_id, "uninitialized");
        assert!(storage2
            .meta_state
            .as_ref()
            .unwrap()
            .get_migration_state()
            .is_none());
    }
}
