//! Raft log 持久化 — vote 与 log entry 在 LSM 上的读写, 以及启动时从 DB 恢复
//! 内存态. key 布局见 `storage/keys.rs` 的 `\x00raft/{gid}/*`.
//!
//! # 数据流
//!
//! ```text
//! RaftLogStorage (gid 维度)
//!   ├─ save_vote       -> vote_key
//!   ├─ append          -> log/{idx} 逐条 + last_log_id_key
//!   ├─ truncate_after  -> delete_range(log/{idx} .. log0) + 回写 last_log_id
//!   ├─ purge           -> delete_range(上次 purge 之后 .. idx) + last_purged_log_id_key
//!   └─ get_log_entries -> PendingLogOverlay 优先, DB 点查 fallback
//!
//! load_state (构造时)
//!   ├─ try_deser_or_clean: 反序列化失败 -> wipe_group_raft_data (openraft 升级兼容)
//!   ├─ last_log_id: 持久化 key 优先 (O(1)), 缺失/悬挂则 O(N) 扫描重建
//!   └─ membership 缺失时从 log 反查最近 Membership entry 重建
//! ```
//!
//! # Invariant
//!
//! - `get_log_entries` 按 index 点查: overlay 立即可读, DB 兜底.
//! - `purge` / `truncate` 使用 `delete_range` 批量删, 避免 log 累积后逐条 delete.
//! - `last_log_id` 持久化 key 存在但对应 entry 丢失 (悬挂) 时走扫描重建.
//! - 数据不兼容 (如版本升级) 时 wipe 该 group 全部 raft 数据并重建.

use std::ops::{Bound, RangeBounds};

use crate::cluster::storage::keys::{
    last_applied_key, last_log_id_key, last_purged_log_id_key, log_key, log_prefix, log_range_end,
    membership_key, snapshot_meta_key, snapshot_temp_global_end, snapshot_temp_global_start,
    vote_key,
};
use crate::cluster::types::TypeConfig;
use crate::error::{ClusterError, Error, Result};

use super::{CLId, LIdOf, OpenRaftStorage, StorageState, VOf};

impl OpenRaftStorage {
    pub(crate) fn save_vote_internal(&self, vote: &VOf) -> Result<()> {
        let data =
            rmp_serde::to_vec(vote).map_err(|e| ClusterError::Serialization(e.to_string()))?;
        self.db.read().put(&vote_key(self.group_id), &data)?;
        self.state.write().vote = Some(*vote);
        Ok(())
    }

    pub(crate) fn get_log_entries(
        &self,
        range: impl RangeBounds<u64>,
    ) -> Result<Vec<<TypeConfig as openraft::RaftTypeConfig>::Entry>> {
        let state = self.state.read();
        let start = match range.start_bound() {
            Bound::Included(&x) => x,
            Bound::Excluded(&x) => x.saturating_add(1),
            Bound::Unbounded => state.last_purged_log_id.map(|id| id.index + 1).unwrap_or(0),
        };
        let end = match range.end_bound() {
            Bound::Included(&x) => x.saturating_add(1),
            Bound::Excluded(&x) => x,
            Bound::Unbounded => state.last_log_id.map(|id| id.index + 1).unwrap_or(0),
        };
        drop(state);

        if start >= end {
            return Ok(Vec::new());
        }

        let overlay = self.pending_overlay();
        let mut by_index: std::collections::BTreeMap<
            u64,
            <TypeConfig as openraft::RaftTypeConfig>::Entry,
        > = std::collections::BTreeMap::new();

        // 1. 必须先读 PendingLogOverlay, 再扫 DB (LogCommitter 先写 DB 后删 overlay,
        //    先读 DB 再读 overlay 会在删 overlay 瞬间出现两处皆空的 TOCTOU 竞态).
        if let Some(ref ov) = overlay {
            let ov_guard = ov.lock();
            for entry in ov_guard.get_range(start, end) {
                by_index.insert(entry.log_id.index, (**entry).clone());
            }
        }

        // 2. 从 DB 批量扫描补齐 overlay 中未覆盖的历史条目
        if (by_index.len() as u64) < (end - start) {
            let prefix = log_prefix(self.group_id);
            let start_key = log_key(self.group_id, start);
            let end_key = log_key(self.group_id, end);
            let db = self.db.read();
            let iter = db.scan(Some(&start_key), Some(&end_key))?;
            for item in iter {
                let (k, v) = item?;
                if !k.starts_with(&prefix) || k.len() != prefix.len() + 8 {
                    continue;
                }
                let idx = u64::from_be_bytes(k[prefix.len()..prefix.len() + 8].try_into().unwrap());
                if idx < start || idx >= end || by_index.contains_key(&idx) {
                    continue;
                }
                let entry: <TypeConfig as openraft::RaftTypeConfig>::Entry =
                    rmp_serde::from_slice(&v).map_err(|e| {
                        ClusterError::Serialization(format!("log_entry(idx={}): {}", idx, e))
                    })?;
                by_index.insert(idx, entry);
            }
        }

        Ok(by_index.into_values().collect())
    }

    pub(crate) fn append_log_entries(
        &self,
        entries: &[<TypeConfig as openraft::RaftTypeConfig>::Entry],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let t0 = std::time::Instant::now();
        use crate::engine::db::WriteBatch;
        let mut batch = WriteBatch::new();
        for entry in entries {
            let key = log_key(self.group_id, entry.log_id.index);
            let data =
                rmp_serde::to_vec(entry).map_err(|e| ClusterError::Serialization(e.to_string()))?;
            batch.put(key, data);
        }
        if let Some(last) = entries.last() {
            let data = rmp_serde::to_vec(&last.log_id)
                .map_err(|e| ClusterError::Serialization(e.to_string()))?;
            batch.put(last_log_id_key(self.group_id), data);
        }
        self.db.read().write(&batch)?;
        let mut state = self.state.write();
        if let Some(last) = entries.last() {
            state.last_log_id = Some(last.log_id);
        }
        tracing::debug!(
            target: "perf",
            group_id = self.group_id,
            entry_count = entries.len(),
            us = t0.elapsed().as_micros(),
            "raft_append_log"
        );
        Ok(())
    }

    pub(crate) fn delete_logs_from(&self, log_id: LIdOf) -> Result<()> {
        let last_index = self
            .state
            .read()
            .last_log_id
            .map(|id| id.index)
            .unwrap_or(0);
        if log_id.index > last_index {
            return Ok(());
        }
        let start = log_key(self.group_id, log_id.index);
        let end = log_range_end(self.group_id);
        self.db.read().delete_range(&start, &end)?;

        let mut state = self.state.write();
        if log_id.index == 0 {
            state.last_log_id = None;
            self.db.read().delete(&last_log_id_key(self.group_id))?;
        } else {
            let prev = log_key(self.group_id, log_id.index - 1);
            state.last_log_id = self
                .db
                .read()
                .get(&prev)?
                .and_then(|data| rmp_serde::from_slice(&data).ok())
                .map(|e: <TypeConfig as openraft::RaftTypeConfig>::Entry| e.log_id);
            if let Some(ref lid) = state.last_log_id {
                let data = rmp_serde::to_vec(lid)
                    .map_err(|e| ClusterError::Serialization(e.to_string()))?;
                self.db.read().put(&last_log_id_key(self.group_id), &data)?;
            }
        }
        Ok(())
    }

    pub(crate) fn purge_logs_upto_internal(&self, log_id: LIdOf) -> Result<()> {
        let start_index = self
            .state
            .read()
            .last_purged_log_id
            .map(|id| id.index + 1)
            .unwrap_or(0);
        if log_id.index < start_index {
            return Ok(());
        }
        let t0 = std::time::Instant::now();
        let start_key = log_key(self.group_id, start_index);
        let end_key = log_key(self.group_id, log_id.index.saturating_add(1));
        self.db.read().delete_range(&start_key, &end_key)?;
        tracing::debug!(
            target: "perf",
            group_id = self.group_id,
            from_index = start_index,
            to_index = log_id.index,
            ms = t0.elapsed().as_millis(),
            "raft_purge_logs"
        );
        self.state.write().last_purged_log_id = Some(log_id);
        let data =
            rmp_serde::to_vec(&log_id).map_err(|e| ClusterError::Serialization(e.to_string()))?;
        self.db
            .read()
            .put(&last_purged_log_id_key(self.group_id), &data)?;
        Ok(())
    }

    /// Try to deserialize with rmp_serde, on failure: log warning, delete the key, return None.
    fn try_deser_or_clean<T: serde::de::DeserializeOwned>(
        &self,
        key: &[u8],
        label: &str,
        state: &mut StorageState,
    ) -> Result<Option<T>> {
        match self.db.read().get(key)? {
            None => Ok(None),
            Some(data) => match rmp_serde::from_slice(&data) {
                Ok(v) => Ok(Some(v)),
                Err(e) => {
                    tracing::warn!(
                        key = label,
                        len = data.len(),
                        error = %e,
                        "load_state: incompatible data, wiping all raft data for this group"
                    );
                    self.wipe_group_raft_data()?;
                    *state = StorageState::default();
                    Ok(None)
                }
            },
        }
    }

    /// Wipe all raft metadata + log entries for this group.
    /// Used when stale/incompatible data is detected (e.g. openraft version upgrade).
    fn wipe_group_raft_data(&self) -> Result<()> {
        let gid = self.group_id;
        tracing::warn!(gid, "wiping all raft data for group");
        // Delete all raft metadata keys
        self.db.read().delete(&vote_key(gid))?;
        self.db.read().delete(&last_applied_key(gid))?;
        self.db.read().delete(&last_log_id_key(gid))?;
        self.db.read().delete(&last_purged_log_id_key(gid))?;
        self.db.read().delete(&membership_key(gid))?;
        self.db.read().delete(&snapshot_meta_key(gid))?;
        // Delete all log entries for this group
        let start = log_prefix(gid);
        let end = log_range_end(gid);
        self.db.read().delete_range(&start, &end)?;
        Ok(())
    }

    pub(crate) fn load_state(&self) -> Result<()> {
        let mut state = self.state.write();
        let gid = self.group_id;

        tracing::debug!(group_id = gid, "load_state: loading Raft state from DB");

        if let Some(vote) = self.try_deser_or_clean::<VOf>(&vote_key(gid), "vote", &mut state)? {
            state.vote = Some(vote);
        }

        if let Some(last_applied) =
            self.try_deser_or_clean::<LIdOf>(&last_applied_key(gid), "last_applied", &mut state)?
        {
            state.last_applied = Some(last_applied);
        }

        if let Some(data) = self.db.read().get(&snapshot_meta_key(gid))? {
            tracing::debug!(
                key = "snapshot_meta",
                len = data.len(),
                "load_state: found key, deserializing"
            );
            state.snapshot_meta = Some(
                postcard::from_bytes(&data)
                    .map_err(|e| ClusterError::Serialization(format!("snapshot_meta: {}", e)))?,
            );
        }

        // 优先从持久化 key 读取 last_log_id (O(1)), 不存在则 fallback 到 O(N) 扫描.
        if let Some(persisted) =
            self.try_deser_or_clean::<LIdOf>(&last_log_id_key(gid), "last_log_id", &mut state)?
        {
            // 验证该 index 的 log entry 确实存在 (防止被手动删除导致的悬挂指针).
            if self
                .db
                .read()
                .get(&log_key(gid, persisted.index))?
                .is_some()
            {
                state.last_log_id = Some(persisted);
            } else {
                // 持久化 key 存在但 log entry 丢失 (异常情况), 走 fallback.
                reconstruct_last_log_id_from_scan(&self.db.read(), gid, &mut state)?;
            }
        } else {
            reconstruct_last_log_id_from_scan(&self.db.read(), gid, &mut state)?;
        }

        tracing::debug!(
            group_id = gid,
            last_log_id_term = state.last_log_id.as_ref().map(|id| id.leader_id.term),
            last_log_id_index = state.last_log_id.as_ref().map(|id| id.index),
            last_applied_index = state.last_applied.as_ref().map(|id| id.index),
            "load_state: state loaded",
        );

        let _ = self
            .db
            .read()
            .delete_range(snapshot_temp_global_start(), snapshot_temp_global_end());

        // Clean up any leftover snapshot temp files from previous crash
        let temp_path = self
            .db
            .read()
            .path()
            .join(format!(".snapshot_temp_{}", gid));
        let _ = std::fs::remove_file(&temp_path);

        if self.db.read().get(&membership_key(gid))?.is_none() {
            if let Some(last_idx) = state
                .last_applied
                .as_ref()
                .map(|id| id.index)
                .or(state.last_log_id.map(|id| id.index))
            {
                for idx in (1..=last_idx).rev() {
                    if let Some(data) = self.db.read().get(&log_key(gid, idx))? {
                        if let Ok(entry) = rmp_serde::from_slice::<
                            <TypeConfig as openraft::RaftTypeConfig>::Entry,
                        >(&data)
                        {
                            if let openraft::EntryPayload::Membership(ref m) = entry.payload {
                                let stored =
                                    openraft::StoredMembership::new(Some(entry.log_id), m.clone());
                                let mem_data = postcard::to_allocvec(&stored)
                                    .map_err(|e| ClusterError::Serialization(e.to_string()))?;
                                self.db.read().put(&membership_key(gid), &mem_data)?;
                                break;
                            }
                        }
                    }
                }
            }
        }

        // 优先从持久化 key 读取 last_purged_log_id (O(1)), 不存在则 fallback 到 O(N) 扫描.
        if let Some(purged) = self.try_deser_or_clean::<LIdOf>(
            &last_purged_log_id_key(gid),
            "last_purged_log_id",
            &mut state,
        )? {
            state.last_purged_log_id = Some(purged);
        } else if let Some(ref last_log) = state.last_log_id {
            // Fallback: 扫描全部 log key 找第一个 index (旧数据兼容).
            let leader_id = last_log.leader_id;
            reconstruct_last_purged_log_id_from_scan(&self.db.read(), gid, leader_id, &mut state)?;
        }

        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn read_last_applied_from_db(&self) -> Result<Option<LIdOf>> {
        match self.db.read().get(&last_applied_key(self.group_id))? {
            None => Ok(None),
            Some(data) => match rmp_serde::from_slice(&data) {
                Ok(v) => Ok(Some(v)),
                Err(e) => {
                    tracing::warn!(
                        len = data.len(),
                        error = %e,
                        "read_last_applied: incompatible data, deleting stale key"
                    );
                    self.db.read().delete(&last_applied_key(self.group_id))?;
                    Ok(None)
                }
            },
        }
    }

    pub(crate) fn load_membership(
        &self,
    ) -> std::result::Result<
        openraft::StoredMembership<CLId, openraft::Membership<u64, openraft::BasicNode>>,
        Error,
    > {
        match self.db.read().get(&membership_key(self.group_id))? {
            Some(data) => match postcard::from_bytes(&data) {
                Ok(m) => Ok(m),
                Err(e) => {
                    tracing::warn!(
                        len = data.len(),
                        error = %e,
                        "load_membership: incompatible data, deleting stale key"
                    );
                    self.db.read().delete(&membership_key(self.group_id))?;
                    Ok(openraft::StoredMembership::default())
                }
            },
            None => Ok(openraft::StoredMembership::default()),
        }
    }
}

pub(crate) fn map_db_err(e: Error) -> ClusterError {
    match e {
        Error::Io(io) => ClusterError::Io(io),
        other => ClusterError::Internal(other.to_string()),
    }
}

pub(crate) fn db_to_storage_err(e: Error) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

#[allow(dead_code)]
pub(crate) fn db_to_storage_write_err(e: Error) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

/// Fallback: 扫描全部 log key 找 max_index, 用于旧数据兼容.
fn reconstruct_last_log_id_from_scan(
    db: &crate::DB,
    gid: u64,
    state: &mut super::StorageState,
) -> Result<()> {
    let prefix = log_prefix(gid);
    let end = log_range_end(gid);
    let mut max_index: Option<u64> = None;
    if let Ok(iter) = db.scan(Some(&prefix), Some(&end)) {
        for item in iter {
            let (key, _) = item?;
            if key.len() >= prefix.len() + 8 {
                let idx = u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap());
                max_index = Some(max_index.map_or(idx, |m| m.max(idx)));
            }
        }
    }
    if let Some(max_idx) = max_index {
        let leader_id = match db.get(&log_key(gid, max_idx)) {
            Ok(Some(data)) => match rmp_serde::from_slice::<
                <TypeConfig as openraft::RaftTypeConfig>::Entry,
            >(&data)
            {
                Ok(entry) => entry.log_id.leader_id,
                Err(e) => {
                    tracing::warn!(gid, max_idx, error = %e, "reconstruct_last_log_id: failed to deserialize log entry, using default");
                    openraft::vote::leader_id_std::CommittedLeaderId::new(0)
                }
            },
            _ => openraft::vote::leader_id_std::CommittedLeaderId::new(0),
        };
        state.last_log_id = Some(openraft::LogId::new(leader_id, max_idx));
    }
    Ok(())
}

/// Fallback: 扫描全部 log key 找第一个 index, 用于旧数据兼容.
fn reconstruct_last_purged_log_id_from_scan(
    db: &crate::DB,
    gid: u64,
    leader_id: CLId,
    state: &mut super::StorageState,
) -> Result<()> {
    let prefix = log_prefix(gid);
    let end = log_range_end(gid);
    let mut first: Option<u64> = None;
    if let Ok(iter) = db.scan(Some(&prefix), Some(&end)) {
        for item in iter {
            let (key, _) = item?;
            if key.len() >= prefix.len() + 8 {
                let idx = u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap());
                first = Some(first.map_or(idx, |f| f.min(idx)));
            }
        }
    }
    if let Some(first_idx) = first {
        if first_idx > 0 {
            // Use the entry at first_idx to get the correct leader for the purged region.
            let lid = match db.get(&log_key(gid, first_idx)) {
                Ok(Some(data)) => match rmp_serde::from_slice::<
                    <TypeConfig as openraft::RaftTypeConfig>::Entry,
                >(&data)
                {
                    Ok(entry) => entry.log_id.leader_id,
                    Err(_) => leader_id,
                },
                _ => leader_id,
            };
            state.last_purged_log_id = Some(openraft::LogId::new(lid, first_idx - 1));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::storage::keys::{log_key, vote_key, DEFAULT_GROUP_ID};
    use crate::cluster::types::{Request, TypeConfig};
    use crate::config::Options;
    use crate::DB;
    use openraft::{EntryPayload, LogId, Vote};
    use tempfile::TempDir;

    type EOf = <TypeConfig as openraft::RaftTypeConfig>::Entry;

    fn put_entry(index: u64, term: u64) -> EOf {
        EOf {
            log_id: LogId::new(
                openraft::vote::leader_id_std::CommittedLeaderId::new(term),
                index,
            ),
            payload: EntryPayload::Normal(Request::Put {
                key: format!("k{index}").into_bytes(),
                value: bytes::Bytes::from(format!("v{index}").into_bytes()),
            }),
        }
    }

    fn open_storage(dir: &TempDir) -> OpenRaftStorage {
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap()
    }

    #[test]
    fn test_save_read_vote() {
        let dir = TempDir::new().unwrap();
        let storage = open_storage(&dir);
        let vote = Vote {
            leader_id: openraft::vote::leader_id_std::LeaderId {
                term: 2,
                voted_for: 1,
            },
            committed: true,
        };
        storage.save_vote_internal(&vote).unwrap();
        assert_eq!(storage.state.read().vote, Some(vote));
        assert!(storage
            .db
            .read()
            .get(&vote_key(DEFAULT_GROUP_ID))
            .unwrap()
            .is_some());
    }

    #[test]
    fn test_append_and_read_log() {
        let dir = TempDir::new().unwrap();
        let storage = open_storage(&dir);
        let entries = vec![put_entry(1, 1), put_entry(2, 1)];
        storage.append_log_entries(&entries).unwrap();
        let read = storage.get_log_entries(1..=2).unwrap();
        assert_eq!(read.len(), 2);
        assert_eq!(read[0].log_id.index, 1);
        assert_eq!(read[1].log_id.index, 2);
        assert!(storage
            .db
            .read()
            .get(&log_key(DEFAULT_GROUP_ID, 1))
            .unwrap()
            .is_some());
    }

    #[test]
    fn test_delete_logs_from() {
        let dir = TempDir::new().unwrap();
        let storage = open_storage(&dir);
        storage
            .append_log_entries(&[put_entry(1, 1), put_entry(2, 1), put_entry(3, 1)])
            .unwrap();
        let from = LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(1), 2);
        storage.delete_logs_from(from).unwrap();
        assert_eq!(storage.get_log_entries(1..=3).unwrap().len(), 1);
        assert_eq!(storage.state.read().last_log_id.map(|id| id.index), Some(1));
    }

    #[test]
    fn test_purge_logs_upto() {
        let dir = TempDir::new().unwrap();
        let storage = open_storage(&dir);
        storage
            .append_log_entries(&[
                put_entry(1, 1),
                put_entry(2, 1),
                put_entry(3, 1),
                put_entry(4, 1),
            ])
            .unwrap();
        let purge_to = LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(1), 3);
        storage.purge_logs_upto_internal(purge_to).unwrap();
        assert_eq!(storage.get_log_entries(1..=4).unwrap().len(), 1);
        assert_eq!(
            storage.state.read().last_purged_log_id.map(|id| id.index),
            Some(3)
        );
    }

    /// `RaftLogStorage::truncate_after(last_log_id)` 契约是 "exclusive": `last_log_id`
    /// 本身必须保留, 且不能留下洞. 这条覆盖不带 committer 的直连存储分支.
    #[tokio::test]
    async fn truncate_after_keeps_last_log_id_without_hole() {
        use openraft::storage::RaftLogStorage;

        let dir = TempDir::new().unwrap();
        let mut storage = open_storage(&dir);
        storage
            .append_log_entries(&[
                put_entry(1, 1),
                put_entry(2, 1),
                put_entry(3, 1),
                put_entry(4, 1),
            ])
            .unwrap();

        let keep = LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(1), 2);
        storage.truncate_after(Some(keep)).await.unwrap();

        let left = storage.get_log_entries(1..=4).unwrap();
        assert_eq!(left.len(), 2, "只保留 index 1, 2");
        assert_eq!(left[0].log_id.index, 1);
        assert_eq!(left[1].log_id.index, 2, "last_log_id 必须保留");
        assert_eq!(storage.state.read().last_log_id.map(|id| id.index), Some(2));
    }

    #[test]
    fn test_storage_restart_recovery() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();
        {
            let storage = open_storage(&dir);
            let vote = Vote {
                leader_id: openraft::vote::leader_id_std::LeaderId {
                    term: 3,
                    voted_for: 2,
                },
                committed: true,
            };
            storage.save_vote_internal(&vote).unwrap();
            storage
                .append_log_entries(&[put_entry(1, 3), put_entry(2, 3)])
                .unwrap();
        }
        let db = DB::open(&path, Options::for_testing()).unwrap();
        let storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();
        assert_eq!(
            storage.state.read().vote,
            Some(Vote {
                leader_id: openraft::vote::leader_id_std::LeaderId {
                    term: 3,
                    voted_for: 2
                },
                committed: true,
            })
        );
        let logs = storage.get_log_entries(1..=2).unwrap();
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].log_id.index, 1);
    }

    #[tokio::test]
    async fn test_get_log_entries_scan_and_overlay_merge() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let db_slot = std::sync::Arc::new(parking_lot::RwLock::new(db.clone()));
        let handle = crate::cluster::log_committer::spawn_committer(
            DEFAULT_GROUP_ID,
            db_slot.clone(),
            crate::cluster::log_committer::LogCommitterConfig::default(),
        );
        let storage = OpenRaftStorage::new_with_committer(
            db_slot,
            DEFAULT_GROUP_ID,
            None,
            Some(handle.clone()),
        )
        .unwrap();

        // DB 写入 index 1..=3
        storage
            .append_log_entries(&[put_entry(1, 1), put_entry(2, 1), put_entry(3, 1)])
            .unwrap();
        assert_eq!(storage.get_log_entries(1..4).unwrap().len(), 3);
        assert_eq!(storage.get_log_entries(2..3).unwrap().len(), 1);

        // overlay 覆盖 index 3 为 term 2, 并追加 index 4
        {
            let mut ov = handle.overlay.lock();
            ov.insert_at(3, std::sync::Arc::new(put_entry(3, 2)));
            ov.insert_at(4, std::sync::Arc::new(put_entry(4, 2)));
        }
        let merged = storage.get_log_entries(1..=4).unwrap();
        assert_eq!(merged.len(), 4);
        assert_eq!(
            merged[2].log_id.leader_id.term, 2,
            "overlay must win on overlap"
        );
        assert_eq!(merged[3].log_id.index, 4);
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn get_log_entries_concurrent_with_log_committer_never_drops_entry() {
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let db_slot = Arc::new(parking_lot::RwLock::new(db));
        let handle = crate::cluster::log_committer::spawn_committer(
            DEFAULT_GROUP_ID,
            db_slot.clone(),
            crate::cluster::log_committer::LogCommitterConfig::default(),
        );
        let storage = OpenRaftStorage::new_with_committer(
            db_slot,
            DEFAULT_GROUP_ID,
            None,
            Some(handle.clone()),
        )
        .unwrap();

        let written = Arc::new(AtomicU64::new(0));
        let done = Arc::new(AtomicBool::new(false));
        let reader_storage = storage.clone();
        let reader_written = Arc::clone(&written);
        let reader_done = Arc::clone(&done);

        let reader = std::thread::spawn(move || {
            while !reader_done.load(Ordering::Acquire) {
                let hi = reader_written.load(Ordering::Acquire);
                if hi == 0 {
                    continue;
                }
                let got = reader_storage.get_log_entries(hi..=hi).unwrap();
                assert_eq!(got.len(), 1, "TOCTOU gap at log index {hi}");
            }
        });

        for idx in 1..=2000u64 {
            let entry = put_entry(idx, 1);
            {
                let mut ov = handle.overlay.lock();
                ov.insert_at(idx, Arc::new(entry));
            }
            let cb = openraft::storage::IOFlushed::<TypeConfig>::noop();
            handle.submit_append_with_bytes(idx, 1, 64, cb).unwrap();
            written.store(idx, Ordering::Release);
            if idx % 32 == 0 {
                tokio::task::yield_now().await;
            }
        }
        done.store(true, Ordering::Release);
        reader.join().unwrap();
        handle.shutdown().await;
    }
}
