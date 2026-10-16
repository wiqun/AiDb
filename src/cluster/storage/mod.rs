//! `OpenRaftStorage` — OpenRaft 在单 Group 上所需的全部存储能力, 用一份结构体
//! 同时实现 `RaftLogStorage` / `RaftLogReader` / `RaftStateMachine` (openraft
//! v0.10 合并接口), 底层为每 Group 独立的 LSM DB.
//!
//! # 组合
//!
//! ```text
//! OpenRaftStorage (每 Group 一个实例, key 均带 gid 前缀)
//!   ├─ RaftLogStorage / RaftLogReader   -> log.rs (vote/log 读写 + 启动恢复)
//!   ├─ RaftStateMachine
//!   │     ├─ apply.rs    apply_entries_internal (SM + last_applied 原子)
//!   │     └─ snapshot.rs build / install (目录级文件打包)
//!   ├─ meta_state: Option<MetaStateMachine> (仅 gid=0 时 Some)
//!   └─ committer: Option<LogCommitterHandle> (异步批量 I/O, 可选)
//! ```
//!
//! 内存态 `StorageState` (vote / last_log_id / last_applied / snapshot_meta) 在
//! 构造时由 `load_state()` 从 DB 恢复; snapshot install 后整表重建并 reload.
//!
//! # Invariant
//!
//! - 单实例只服务一个 group; 不同 group 用独立 DB (目录 `data/group_{id}/`).
//! - log 写可走 `LogCommitter` (先入 overlay 立即可读, 再异步 flush) 或同步路径.
//! - 状态机 apply 的 fail-fast 语义见 `apply.rs` (存储错误必须上抛).

mod apply;
pub(crate) mod keys;
mod log;
mod snapshot;

use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::{
    storage::{LogState, RaftLogStorage, RaftStateMachine},
    LogId, RaftLogReader, Snapshot, SnapshotMeta, StoredMembership, Vote,
};
use parking_lot::RwLock;

pub use keys::DEFAULT_GROUP_ID;

use crate::cluster::log_committer::{LogCommitterHandle, LogCommitterMetrics};
use crate::cluster::meta_state_machine::MetaStateMachine;
use crate::cluster::storage::log::db_to_storage_err;
use crate::cluster::storage::snapshot::prepare_snapshot_bundle;
use crate::cluster::types::TypeConfig;
use crate::error::Result;
use crate::DB;

/// v0.10 std mode: CommittedLeaderId<u64> with term field only.
type CLId = openraft::vote::leader_id_std::CommittedLeaderId<u64>;
/// LogId in v0.10 with std committed leader id.
type LIdOf = LogId<CLId>;
/// VoteOf<TypeConfig> using std mode LeaderId.
type VOf = Vote<openraft::vote::leader_id_std::LeaderId<u64, u64>>;
/// Snapshot meta with std committed leader id.
type SMOf = SnapshotMeta<CLId, u64, openraft::BasicNode>;
/// Stored membership with std committed leader id.
type MOf = StoredMembership<CLId, u64, openraft::BasicNode>;

#[derive(Debug, Clone, Default)]
pub(crate) struct StorageState {
    vote: Option<VOf>,
    last_purged_log_id: Option<LIdOf>,
    last_log_id: Option<LIdOf>,
    last_applied: Option<LIdOf>,
    snapshot_meta: Option<SMOf>,
}

#[derive(Clone)]
pub struct OpenRaftStorage {
    pub(crate) db: Arc<DB>,
    pub(crate) state: Arc<RwLock<StorageState>>,
    pub(crate) group_id: u64,
    pub(crate) meta_state: Option<Arc<MetaStateMachine>>,
    pub(crate) snapshot_temp_path: Arc<RwLock<Option<PathBuf>>>,
    pub(crate) committer: Option<LogCommitterHandle>,
}

impl OpenRaftStorage {
    /// 获取 LogCommitter metrics (如果有).
    pub fn committer_metrics(&self) -> Option<Arc<LogCommitterMetrics>> {
        self.committer.as_ref().map(|h| h.metrics.clone())
    }

    /// 获取 PendingLogOverlay 引用 (用于 get_log_entries 读 pending entry).
    pub(crate) fn pending_overlay(
        &self,
    ) -> Option<
        Arc<
            parking_lot::Mutex<
                crate::cluster::pending_log::PendingLogOverlay<
                    <TypeConfig as openraft::RaftTypeConfig>::Entry,
                >,
            >,
        >,
    > {
        self.committer.as_ref().map(|h| h.overlay.clone())
    }
}

impl OpenRaftStorage {
    pub fn new(
        db: Arc<DB>,
        group_id: u64,
        meta_state: Option<Arc<MetaStateMachine>>,
    ) -> Result<Self> {
        Self::new_with_committer(db, group_id, meta_state, None)
    }

    pub fn new_with_committer(
        db: Arc<DB>,
        group_id: u64,
        meta_state: Option<Arc<MetaStateMachine>>,
        committer: Option<LogCommitterHandle>,
    ) -> Result<Self> {
        let storage = Self {
            db,
            state: Arc::new(RwLock::new(StorageState::default())),
            group_id,
            meta_state,
            snapshot_temp_path: Arc::new(RwLock::new(None)),
            committer,
        };
        storage.load_state()?;
        Ok(storage)
    }

    pub fn db(&self) -> &Arc<DB> {
        &self.db
    }

    pub fn group_id(&self) -> u64 {
        self.group_id
    }
}

impl RaftLogReader<TypeConfig> for OpenRaftStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + openraft::OptionalSend>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<<TypeConfig as openraft::RaftTypeConfig>::Entry>, std::io::Error>
    {
        self.get_log_entries(range).map_err(db_to_storage_err)
    }

    async fn read_vote(&mut self) -> std::result::Result<Option<VOf>, std::io::Error> {
        let state = self.state.read();
        Ok(state.vote)
    }
}

impl RaftLogStorage<TypeConfig> for OpenRaftStorage {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> std::result::Result<LogState<TypeConfig>, std::io::Error> {
        let state = self.state.read();
        Ok(LogState {
            last_purged_log_id: state.last_purged_log_id,
            last_log_id: state.last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VOf) -> std::result::Result<(), std::io::Error> {
        if let Some(ref committer) = self.committer {
            // 先更新内存状态, 再异步写 DB.
            self.state.write().vote = Some(*vote);
            committer.save_vote(*vote).await
        } else {
            self.save_vote_internal(vote)
                .map_err(|e| std::io::Error::other(e.to_string()))
        }
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::IOFlushed<TypeConfig>,
    ) -> std::result::Result<(), std::io::Error>
    where
        I: IntoIterator<Item = <TypeConfig as openraft::RaftTypeConfig>::Entry>
            + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let entries_vec: Vec<_> = entries.into_iter().collect();
        if entries_vec.is_empty() {
            callback.io_completed(Ok(()));
            return Ok(());
        }

        let first_index = entries_vec.first().map(|e| e.log_id.index).unwrap();
        let count = entries_vec.len() as u64;

        if let Some(ref committer) = self.committer {
            // 写入 PendingLogOverlay, 使其立即可读.
            {
                let mut ov = committer.overlay.lock();
                for entry in &entries_vec {
                    ov.insert_at(entry.log_id.index, entry.clone());
                }
            }

            // 发送到 committer 并等待 flush.
            committer.append(first_index, count).await?;

            // 更新 in-memory last_log_id
            if let Some(last) = entries_vec.last() {
                self.state.write().last_log_id = Some(last.log_id);
            }

            callback.io_completed(Ok(()));
            Ok(())
        } else {
            // 无 committer: 同步写入 (旧路径).
            #[cfg(feature = "cluster-test-util")]
            crate::cluster::failpoint::fire(crate::cluster::FailPoint::AppendBeforeDbWrite);
            self.append_log_entries(&entries_vec)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            callback.io_completed(Ok(()));
            Ok(())
        }
    }

    async fn truncate_after(
        &mut self,
        log_id: Option<LIdOf>,
    ) -> std::result::Result<(), std::io::Error> {
        if let Some(ref committer) = self.committer {
            // Truncate overlay immediately.
            {
                let mut ov = committer.overlay.lock();
                if let Some(lid) = log_id {
                    ov.truncate_after(lid.index);
                } else {
                    ov.clear();
                }
            }
            committer.truncate_after(log_id).await?;

            // 更新 in-memory last_log_id: 从 DB 读取将剩下的最后一条 entry 的 log_id
            let mut state = self.state.write();
            if let Some(lid) = log_id {
                if lid.index > 0 {
                    let prev_key =
                        crate::cluster::storage::keys::log_key(self.group_id, lid.index - 1);
                    let prev_entry = self
                        .db
                        .get(&prev_key)
                        .map_err(|e| std::io::Error::other(e.to_string()))?
                        .and_then(|data| {
                            rmp_serde::from_slice::<
                                    <TypeConfig as openraft::RaftTypeConfig>::Entry,
                                >(&data)
                                .ok()
                        });
                    state.last_log_id = prev_entry.map(|e| e.log_id);
                } else {
                    state.last_log_id = None;
                }
            } else {
                state.last_log_id = None;
            }
            Ok(())
        } else {
            let Some(lid) = log_id else {
                return Ok(());
            };
            #[cfg(feature = "cluster-test-util")]
            crate::cluster::failpoint::fire(crate::cluster::FailPoint::TruncateBeforePersist);
            self.delete_logs_from(lid)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            #[cfg(feature = "cluster-test-util")]
            crate::cluster::failpoint::fire(crate::cluster::FailPoint::TruncateAfterPersist);
            Ok(())
        }
    }

    async fn purge(&mut self, log_id: LIdOf) -> std::result::Result<(), std::io::Error> {
        if let Some(ref committer) = self.committer {
            // Purge overlay immediately.
            {
                let mut ov = committer.overlay.lock();
                ov.purge_upto(log_id.index);
            }
            committer.purge(log_id).await?;

            // 更新 in-memory last_purged_log_id
            let mut state = self.state.write();
            state.last_purged_log_id = Some(log_id);

            // 如果 last_log_id 也被 purged 了 (index <= last_log_id.index), 更新之
            if let Some(ref last) = state.last_log_id {
                if last.index <= log_id.index {
                    state.last_log_id = None;
                }
            }
            Ok(())
        } else {
            #[cfg(feature = "cluster-test-util")]
            crate::cluster::failpoint::fire(crate::cluster::FailPoint::PurgeBeforePersist);
            self.purge_logs_upto_internal(log_id)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            #[cfg(feature = "cluster-test-util")]
            crate::cluster::failpoint::fire(crate::cluster::FailPoint::PurgeAfterPersist);
            Ok(())
        }
    }
}

impl RaftStateMachine<TypeConfig> for OpenRaftStorage {
    type SnapshotData = std::io::Cursor<Vec<u8>>;
    type SnapshotBuilder = snapshot::OpenRaftSnapshotBuilder;

    async fn applied_state(&mut self) -> std::result::Result<(Option<LIdOf>, MOf), std::io::Error> {
        let state = self.state.read();
        let membership = self
            .load_membership()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok((state.last_applied, membership))
    }

    async fn apply<Strm>(&mut self, entries: Strm) -> std::result::Result<(), std::io::Error>
    where
        Strm: futures_util::Stream<
                Item = std::result::Result<
                    openraft::storage::EntryResponder<TypeConfig>,
                    std::io::Error,
                >,
            > + Unpin
            + openraft::OptionalSend,
    {
        use futures_util::StreamExt;
        tokio::pin!(entries);

        let mut batch_entries = Vec::new();
        let mut responders = Vec::new();

        while let Some(item) = entries.next().await {
            let (entry, responder) = item.map_err(|e| std::io::Error::other(e.to_string()))?;
            batch_entries.push(entry);
            responders.push(responder);
        }

        if batch_entries.is_empty() {
            return Ok(());
        }

        let responses = self
            .apply_entries_internal(&batch_entries)
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        for (response, responder_opt) in responses.into_iter().zip(responders) {
            if let Some(responder) = responder_opt {
                responder.send(response);
            }
        }

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        snapshot::OpenRaftSnapshotBuilder::new(self.db.clone(), self.group_id)
    }

    async fn install_snapshot(
        &mut self,
        meta: &SMOf,
        snapshot: Self::SnapshotData,
    ) -> std::result::Result<(), std::io::Error> {
        let data = snapshot.into_inner();

        // 初始化临时文件路径 (openraft >= alpha.33 不再调用 begin_receiving_snapshot,
        // 接收/安装流程统一在 install_snapshot 内完成临时文件初始化).
        let db_path = self.db.path().to_path_buf();
        let temp_path = db_path.join(format!(".snapshot_temp_{}", self.group_id));
        let _ = std::fs::remove_file(&temp_path);
        *self.snapshot_temp_path.write() = Some(temp_path);

        // Write to temp file first for crash safety
        if let Some(ref temp_path) = *self.snapshot_temp_path.read() {
            if let Err(e) = std::fs::write(temp_path, &data) {
                let _ = std::fs::remove_file(temp_path);
                return Err(std::io::Error::other(e.to_string()));
            }
            // fsync the parent directory to ensure metadata durability
            if let Some(parent) = temp_path.parent() {
                if let Ok(dir) = std::fs::File::open(parent) {
                    let _ = dir.sync_all();
                }
            }
        }

        // Apply the snapshot data atomically
        self.install_snapshot_atomic(meta, &data).map_err(|e| {
            // Clean up temp on install failure
            if let Some(ref temp_path) = *self.snapshot_temp_path.read() {
                let _ = std::fs::remove_file(temp_path);
            }
            *self.snapshot_temp_path.write() = None;
            std::io::Error::other(e.to_string())
        })?;

        // Clear PendingLogOverlay and in-memory state after snapshot install
        // (DB was fully replaced so all in-memory caches are stale).
        {
            let mut state = self.state.write();
            *state = StorageState::default();
        }
        if let Some(ref committer) = self.committer {
            committer.overlay.lock().clear();
        }
        // Reload state from the new DB
        self.load_state()
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        // Clean up temp file after successful install
        if let Some(ref temp_path) = *self.snapshot_temp_path.read() {
            let _ = std::fs::remove_file(temp_path);
        }
        *self.snapshot_temp_path.write() = None;

        Ok(())
    }

    #[allow(clippy::type_complexity)]
    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<
        Option<Snapshot<CLId, u64, openraft::BasicNode, Cursor<Vec<u8>>>>,
        std::io::Error,
    > {
        let state = self.state.read();
        let Some(meta) = state.snapshot_meta.clone() else {
            return Ok(None);
        };
        drop(state);

        let data =
            prepare_snapshot_bundle(&self.db).map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok(Some(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        }))
    }
}
