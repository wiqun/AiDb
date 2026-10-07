//! Raft log apply 路径 — 把已提交 entry 落进状态机 (SM) 并推进 `last_applied`.
//! 数据面 SM 写 `\x01sm/{gid}/{user_key}`; MetaRaft (gid=0) 走 `MetaStateMachine`.
//!
//! # 数据流
//!
//! ```text
//! apply_entries_internal(entries)
//!   ├─ 跳过 last_applied 已覆盖的 index
//!   ├─ Normal data entry: 合并连续 Normal -> 单 WriteBatch
//!   │     ├─ append_request_to_batch -> sm_key 写入 / PutConditional 去重 /
//!   │     │     MigrationWrite (tombstone+tip) / MigrationGc
//!   │     ├─ 追加 last_applied_key
//!   │     └─ db.write_without_wal (批量) 或全跳过时仅 persist_last_applied_atomic
//!   ├─ Meta entry: apply_meta_entry -> MetaStateMachine -> kv_pairs -> 单 WriteBatch
//!   ├─ Membership entry: apply_membership_entry_atomic (独立原子批)
//!   └─ Blank entry: persist_last_applied_atomic
//! ```
//!
//! # Invariant
//!
//! - **Apply fail-fast (最重要)**: 遇到真实存储错误 (如 `PutConditional` dedup
//!   读 I/O 失败) 必须直接 `?` 上抛, 交给 openraft 判定该 raft 实例 Fatal 并
//!   停止服务; 绝不能吞成业务级 `Response::Error` 继续下一条, 否则
//!   `last_applied` 会越过失败 entry 被持久化 -> 数据永久丢失且副本间可能分叉.
//! - 连续的 Normal data entry 合并为单个 WriteBatch (SM + last_applied 原子);
//!   Meta / Membership / Blank entry 各自独立原子写入.
//! - `PutConditional` 在 apply 内 (同一 entry) 查 Del tombstone, 消除
//!   "先查后写"窗口 (FIX-0056-A1).
//! - `MigrationWrite` 的用户写与 tombstone/tip 必须同 entry 原子可见;
//!   seq/tip 只在 apply 内单调分配.
//! - `MigrationGc` 只删 `\x02mig/{gid}/{epoch}/*`, 不动用户 `sm_key`.

use openraft::{EntryPayload, MessageSummary};
use std::collections::HashMap;

use crate::cluster::meta_types::{MetaRequest, METARAFT_GROUP_ID};
use crate::cluster::migration_oplog::{
    decode_tip, decode_tombstone, encode_tip, encode_tombstone, MigOp,
};
use crate::cluster::storage::keys::{
    last_applied_key, membership_key, mig_range_end, mig_range_start, mig_tip_key,
    mig_tombstone_key, sm_key,
};
use crate::cluster::types::{
    ClusterError, Request, Response, ThinWriteBatch, ThinWriteOp, TypeConfig,
};
use crate::engine::db::WriteBatch;
use crate::error::{Error, Result};

use super::{LIdOf, OpenRaftStorage};
use crate::cluster::storage::log::map_db_err;

impl OpenRaftStorage {
    #[cfg(test)]
    pub(crate) fn apply_batch_to_sm(&self, batch: &ThinWriteBatch) -> Result<()> {
        let mut db_batch = WriteBatch::new();
        let mut overlay = HashMap::new();
        let _ = self.append_thin_batch_to_db_batch(&mut db_batch, batch, &mut overlay)?;
        if db_batch.is_empty() {
            return Ok(());
        }
        let _ = self
            .db
            .write(&db_batch)
            .map_err(|e| Error::Cluster(map_db_err(e)))?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn apply_migration_write_to_sm(
        &self,
        epoch: u64,
        ops: &ThinWriteBatch,
    ) -> Result<()> {
        let mut db_batch = WriteBatch::new();
        let mut overlay = HashMap::new();
        let _ = self.append_migration_write_to_batch(&mut db_batch, epoch, ops, &mut overlay)?;
        if db_batch.is_empty() {
            return Ok(());
        }
        let _ = self
            .db
            .write(&db_batch)
            .map_err(|e| Error::Cluster(map_db_err(e)))?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn apply_put_conditional(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        migration_epoch: Option<u64>,
    ) -> Result<()> {
        let mut batch = WriteBatch::new();
        let mut overlay = HashMap::new();
        self.append_put_conditional_to_batch(
            &mut batch,
            &key,
            &value,
            migration_epoch,
            &mut overlay,
        )?;
        if batch.is_empty() {
            return Ok(());
        }
        let _ = self
            .db
            .write(&batch)
            .map_err(|e| Error::Cluster(map_db_err(e)))?;
        Ok(())
    }

    pub fn apply_entries_internal(
        &self,
        entries: &[<TypeConfig as openraft::RaftTypeConfig>::Entry],
    ) -> Result<Vec<Response>> {
        let mut last_applied = self.read_last_applied_from_db()?;
        let mut responses = Vec::with_capacity(entries.len());

        tracing::debug!(
            group_id = self.group_id,
            entry_count = entries.len(),
            last_applied_index = last_applied.as_ref().map(|id| id.index),
            "apply_entries_internal: applying batch",
        );

        let mut i = 0;
        while i < entries.len() {
            let entry = &entries[i];

            // 跳过已 apply 的 entry
            if let Some(ref applied) = last_applied {
                if entry.log_id.index <= applied.index {
                    tracing::trace!(index = entry.log_id.index, "skipping already-applied entry",);
                    responses.push(Response::Ok);
                    i += 1;
                    continue;
                }
            }

            tracing::debug!(
              index = entry.log_id.index,
              term = entry.log_id.leader_id.term,
              payload = %entry.payload.summary(),
              "applying entry",
            );

            match &entry.payload {
                EntryPayload::Normal(Request::Meta(meta_req))
                    if self.group_id == METARAFT_GROUP_ID && self.meta_state.is_some() =>
                {
                    let response = self.apply_meta_entry(meta_req, &entry.log_id)?;
                    last_applied = Some(entry.log_id);
                    responses.push(response);
                    i += 1;
                }
                EntryPayload::Normal(_request) => {
                    // 将连续的 data entry 批量合并到一个 WriteBatch.
                    //
                    // append_request_to_batch 目前唯一可能返回 Err 的路径是
                    // PutConditional 内部 self.db.get() 的磁盘 I/O 读取失败 —
                    // 这是基础设施故障, 不是业务结果 (Put/Delete/WriteBatch 三个
                    // 分支永远不失败; PutConditional "条件不满足" 本身走的是
                    // Ok(()) 分支, 不是 Err). 因此这里的错误必须和本文件里其它
                    // DB 写操作一样直接 `?` 向上抛, 交给 openraft 判定为该 raft
                    // 实例的致命 StorageError 并停止服务 (对齐 etcd/TiKV: apply
                    // 对已提交 entry 必须是确定性的, 不能把一次真实的存储故障
                    // 悄悄当成"这条 entry 失败了但继续处理下一条", 否则
                    // last_applied 会越过这条 entry 被持久化, 数据永久丢失且
                    // 不同副本可能因为存储故障的偶然性而分叉.
                    let t0 = std::time::Instant::now();
                    let mut batch = WriteBatch::new();
                    let mut overlay = HashMap::new();
                    let mut batched_responses: Vec<Response> = Vec::new();
                    let batch_start = i;
                    let mut last_log_id = entry.log_id;

                    while i < entries.len() {
                        let e = &entries[i];
                        if let Some(ref applied) = last_applied {
                            if e.log_id.index <= applied.index {
                                break; // 已 skip 的不纳入当前批
                            }
                        }
                        match &e.payload {
                            EntryPayload::Normal(Request::Meta(_)) => break,
                            EntryPayload::Membership(_) => break,
                            EntryPayload::Blank => break,
                            EntryPayload::Normal(req) => {
                                let response =
                                    self.append_request_to_batch(&mut batch, req, &mut overlay)?;
                                batched_responses.push(response);
                                last_log_id = e.log_id;
                                i += 1;
                            }
                        }
                    }

                    if !batch.is_empty() {
                        batch.put(
                            last_applied_key(self.group_id),
                            Self::serialize_last_applied(&last_log_id)?,
                        );
                        #[cfg(feature = "cluster-test-util")]
                        crate::cluster::failpoint::fire(
                            crate::cluster::FailPoint::ApplyBeforePersist,
                        );
                        let _ = self
                            .db
                            .write_without_wal(&batch)
                            .map_err(|e| Error::Cluster(map_db_err(e)))?;
                        #[cfg(feature = "cluster-test-util")]
                        crate::cluster::failpoint::fire(
                            crate::cluster::FailPoint::ApplyAfterPersist,
                        );
                        self.state.write().last_applied = Some(last_log_id);

                        let count = batched_responses.len();
                        tracing::debug!(
                            target: "perf",
                            group_id = self.group_id,
                            batch_size = count,
                            from_index = entries[batch_start].log_id.index,
                            to_index = last_log_id.index,
                            us = t0.elapsed().as_micros(),
                            "raft_apply_batch",
                        );

                        responses.extend(batched_responses);
                        last_applied = Some(last_log_id);
                    } else {
                        // 所有 entry 都是 PutConditional 跳过: 只需写 last_applied
                        self.persist_last_applied_atomic(&last_log_id)?;
                        responses.extend(batched_responses);
                        last_applied = Some(last_log_id);
                    }
                }
                EntryPayload::Membership(m) => {
                    let response = self.apply_membership_entry_atomic(m, &entry.log_id)?;
                    last_applied = Some(entry.log_id);
                    responses.push(response);
                    i += 1;
                }
                EntryPayload::Blank => {
                    self.persist_last_applied_atomic(&entry.log_id)?;
                    responses.push(Response::Ok);
                    last_applied = Some(entry.log_id);
                    i += 1;
                }
            }
        }

        Ok(responses)
    }

    /// 批内 overlay: key 为 `sm_key`, value 为当前批内可见的存在性.
    fn overlay_key_exists(
        overlay: &HashMap<Vec<u8>, bool>,
        db: &crate::DB,
        sk: &[u8],
    ) -> Result<bool> {
        match overlay.get(sk) {
            Some(present) => Ok(*present),
            None => db.key_exists(sk),
        }
    }

    fn append_thin_batch_to_db_batch(
        &self,
        batch: &mut WriteBatch,
        thin: &ThinWriteBatch,
        overlay: &mut HashMap<Vec<u8>, bool>,
    ) -> Result<Vec<bool>> {
        let mut effects = Vec::with_capacity(thin.ops.len());
        for op in &thin.ops {
            match op {
                ThinWriteOp::Put { key, value } => {
                    let sk = sm_key(self.group_id, key);
                    let existed = Self::overlay_key_exists(overlay, &self.db, &sk)?;
                    effects.push(!existed);
                    overlay.insert(sk.clone(), true);
                    batch.put(sk, value.clone());
                }
                ThinWriteOp::Delete { key } => {
                    let sk = sm_key(self.group_id, key);
                    let existed = Self::overlay_key_exists(overlay, &self.db, &sk)?;
                    effects.push(existed);
                    overlay.insert(sk.clone(), false);
                    batch.delete(sk);
                }
            }
        }
        Ok(effects)
    }

    fn append_put_conditional_to_batch(
        &self,
        batch: &mut WriteBatch,
        key: &[u8],
        value: &[u8],
        migration_epoch: Option<u64>,
        overlay: &mut HashMap<Vec<u8>, bool>,
    ) -> Result<()> {
        // FIX-0056-A1 不变式 #3: PutConditional 必须在 apply 内 (与这次条件
        // put 同一个 entry) 查 Del tombstone, 消除"先查后写"窗口 —— 不能在
        // propose 前查完就假定结论一直有效.
        if let Some(epoch) = migration_epoch {
            let ts_key = mig_tombstone_key(self.group_id, epoch, key);
            if let Some((MigOp::Del, _)) = self.db.get(&ts_key)?.and_then(|v| decode_tombstone(&v))
            {
                return Ok(());
            }
        }
        let sm = sm_key(self.group_id, key);
        if Self::overlay_key_exists(overlay, &self.db, &sm)? {
            return Ok(());
        }
        batch.put(sm.clone(), value.to_vec());
        overlay.insert(sm, true);
        Ok(())
    }

    /// FIX-0056-A1: 迁移期用户写 (`ops`) 与该 epoch 的 tombstone/tip 更新
    /// 打进同一 `WriteBatch`, 与用户 `sm_key` 变更同 entry 原子可见
    /// (不变式 #1). seq 只在这里 (Raft apply 内) 单调分配, 不允许旁路自增.
    fn append_migration_write_to_batch(
        &self,
        batch: &mut WriteBatch,
        epoch: u64,
        ops: &ThinWriteBatch,
        overlay: &mut HashMap<Vec<u8>, bool>,
    ) -> Result<Vec<bool>> {
        if ops.is_empty() {
            return Ok(Vec::new());
        }
        let tip_key = mig_tip_key(self.group_id, epoch);
        let tip_before = self
            .db
            .get(&tip_key)?
            .and_then(|v| decode_tip(&v))
            .unwrap_or(0);
        let mut effects = Vec::with_capacity(ops.ops.len());
        for (i, op) in ops.ops.iter().enumerate() {
            let seq = tip_before + i as u64 + 1;
            match op {
                ThinWriteOp::Put { key, value } => {
                    let sk = sm_key(self.group_id, key);
                    let existed = Self::overlay_key_exists(overlay, &self.db, &sk)?;
                    effects.push(!existed);
                    overlay.insert(sk.clone(), true);
                    batch.put(sk, value.clone());
                    batch.put(
                        mig_tombstone_key(self.group_id, epoch, key),
                        encode_tombstone(MigOp::Put, seq),
                    );
                }
                ThinWriteOp::Delete { key } => {
                    let sk = sm_key(self.group_id, key);
                    let existed = Self::overlay_key_exists(overlay, &self.db, &sk)?;
                    effects.push(existed);
                    overlay.insert(sk.clone(), false);
                    batch.delete(sk);
                    batch.put(
                        mig_tombstone_key(self.group_id, epoch, key),
                        encode_tombstone(MigOp::Del, seq),
                    );
                }
            }
        }
        batch.put(tip_key, encode_tip(tip_before + ops.len() as u64));
        Ok(effects)
    }

    /// FIX-0056-A1: Commit/Cancel 后 GC 该 epoch 的全部 `mig/{gid}/{epoch}/*`
    /// key (tombstone + tip). 与用户写一样走 Raft apply, 保证复制/可重放.
    fn append_migration_gc_to_batch(&self, batch: &mut WriteBatch, epoch: u64) -> Result<()> {
        let start = mig_range_start(self.group_id, epoch);
        let end = mig_range_end(self.group_id, epoch);
        for item in self.db.scan(Some(&start), Some(&end))? {
            let (key, _) = item?;
            batch.delete(key);
        }
        Ok(())
    }

    fn append_request_to_batch(
        &self,
        batch: &mut WriteBatch,
        request: &Request,
        overlay: &mut HashMap<Vec<u8>, bool>,
    ) -> Result<Response> {
        match request {
            Request::Meta(_) => Ok(Response::Error("meta request on non-meta storage".into())),
            Request::PutConditional {
                key,
                value,
                migration_epoch,
            } => {
                self.append_put_conditional_to_batch(batch, key, value, *migration_epoch, overlay)?;
                Ok(Response::Ok)
            }
            Request::Put { key, value } => {
                let sk = sm_key(self.group_id, key);
                let existed = Self::overlay_key_exists(overlay, &self.db, &sk)?;
                overlay.insert(sk.clone(), true);
                batch.put(sk, value.clone());
                Ok(Response::WriteStats {
                    effects: vec![!existed],
                })
            }
            Request::Delete { key } => {
                let sk = sm_key(self.group_id, key);
                let existed = Self::overlay_key_exists(overlay, &self.db, &sk)?;
                overlay.insert(sk.clone(), false);
                batch.delete(sk);
                Ok(Response::WriteStats {
                    effects: vec![existed],
                })
            }
            Request::WriteBatch(wb) => {
                let effects = self.append_thin_batch_to_db_batch(batch, wb, overlay)?;
                Ok(Response::WriteStats { effects })
            }
            // 迁移写屏障: 不改用户数据, 仅随 entry 推进 last_applied.
            Request::MigrationBarrier { .. } => Ok(Response::Ok),
            Request::MigrationWrite { epoch, ops } => {
                let effects = self.append_migration_write_to_batch(batch, *epoch, ops, overlay)?;
                Ok(Response::WriteStats { effects })
            }
            Request::MigrationGc { epoch } => {
                self.append_migration_gc_to_batch(batch, *epoch)?;
                Ok(Response::Ok)
            }
        }
    }

    fn serialize_last_applied(log_id: &LIdOf) -> Result<Vec<u8>> {
        rmp_serde::to_vec(log_id)
            .map_err(|e| Error::Cluster(ClusterError::Serialization(e.to_string())))
    }

    fn persist_last_applied_atomic(&self, log_id: &LIdOf) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(
            last_applied_key(self.group_id),
            Self::serialize_last_applied(log_id)?,
        );
        let _ = self
            .db
            .write(&batch)
            .map_err(|e| Error::Cluster(map_db_err(e)))?;
        self.state.write().last_applied = Some(*log_id);
        Ok(())
    }

    fn apply_membership_entry_atomic(
        &self,
        membership: &openraft::Membership<crate::cluster::types::NodeId, openraft::BasicNode>,
        log_id: &LIdOf,
    ) -> Result<Response> {
        let stored = openraft::StoredMembership::new(Some(*log_id), membership.clone());
        let data = postcard::to_allocvec(&stored)
            .map_err(|e| Error::Cluster(ClusterError::Serialization(e.to_string())))?;
        let mut batch = WriteBatch::new();
        batch.put(membership_key(self.group_id), data);
        batch.put(
            last_applied_key(self.group_id),
            Self::serialize_last_applied(log_id)?,
        );
        let _ = self
            .db
            .write(&batch)
            .map_err(|e| Error::Cluster(map_db_err(e)))?;
        self.state.write().last_applied = Some(*log_id);
        Ok(Response::Ok)
    }

    fn apply_meta_entry(&self, req: &MetaRequest, log_id: &LIdOf) -> Result<Response> {
        let meta_state = self.meta_state.as_ref().expect("meta_state required");
        let mut batch = WriteBatch::new();
        let response = match meta_state.apply_meta_request(req.clone()) {
            Ok(output) => {
                for (key, value) in output.kv_pairs {
                    batch.put(key, value);
                }
                Response::Ok
            }
            Err(e) => Response::Error(e.to_string()),
        };
        batch.put(
            last_applied_key(self.group_id),
            Self::serialize_last_applied(log_id)?,
        );
        let _ = self
            .db
            .write(&batch)
            .map_err(|e| Error::Cluster(map_db_err(e)))?;
        self.state.write().last_applied = Some(*log_id);
        Ok(response)
    }

    pub fn get_state_machine_value(&self, user_key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db
            .get(&sm_key(self.group_id, user_key))
            .map_err(|e| Error::Cluster(map_db_err(e)))
    }

    /// 读取本 group 在 `epoch` 下的迁移 oplog tip; 缺失视为 0 (从未写过).
    pub fn get_migration_tip(&self, epoch: u64) -> Result<u64> {
        let key = mig_tip_key(self.group_id, epoch);
        Ok(self
            .db
            .get(&key)
            .map_err(|e| Error::Cluster(map_db_err(e)))?
            .and_then(|v| decode_tip(&v))
            .unwrap_or(0))
    }

    /// FIX-0056-A1 合并读线性点第 1 步: 读取本 group 在 `epoch` 下 `user_key`
    /// 最后一次的迁移 tombstone 操作 (不含 seq). 无 tombstone (从未在该 epoch
    /// 内被 `MigrationWrite` 动过) 返回 `None`.
    pub fn get_migration_tombstone(&self, epoch: u64, user_key: &[u8]) -> Result<Option<MigOp>> {
        let key = mig_tombstone_key(self.group_id, epoch, user_key);
        Ok(self
            .db
            .get(&key)
            .map_err(|e| Error::Cluster(map_db_err(e)))?
            .and_then(|v| decode_tombstone(&v))
            .map(|(op, _seq)| op))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::meta_state_machine::MetaStateMachine;
    use crate::cluster::meta_types::MetaRequest;
    use crate::cluster::storage::keys::{meta_cluster_meta_key, sm_key, DEFAULT_GROUP_ID};
    use crate::config::Options;
    use crate::DB;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_storage() -> (OpenRaftStorage, TempDir) {
        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        (
            OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap(),
            dir,
        )
    }

    fn meta_storage() -> (OpenRaftStorage, TempDir) {
        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let meta = Arc::new(MetaStateMachine::new(db.clone()).unwrap());
        (
            OpenRaftStorage::new(db, METARAFT_GROUP_ID, Some(meta)).unwrap(),
            dir,
        )
    }

    #[test]
    fn test_apply_put_delete() {
        let (storage, _dir) = test_storage();
        let mut batch = ThinWriteBatch::new();
        batch.put(b"k1".to_vec(), b"v1".to_vec());
        storage.apply_batch_to_sm(&batch).unwrap();
        assert_eq!(
            storage.get_state_machine_value(b"k1").unwrap(),
            Some(b"v1".to_vec())
        );
        let mut del = ThinWriteBatch::new();
        del.delete(b"k1".to_vec());
        storage.apply_batch_to_sm(&del).unwrap();
        assert!(storage.get_state_machine_value(b"k1").unwrap().is_none());
    }

    /// 空库上 Put→Del→Put: effects 为 [inserted, deleted, inserted],
    /// 且依赖批内 overlay (中间 Del 必须可见, 否则第二次 Put 会误报非 insert).
    #[test]
    fn apply_write_batch_put_del_put_effects_no_double_insert() {
        use openraft::{EntryPayload, LogId};

        let (storage, _dir) = test_storage();
        let mut ops = ThinWriteBatch::new();
        ops.put(b"k".to_vec(), b"a".to_vec());
        ops.delete(b"k".to_vec());
        ops.put(b"k".to_vec(), b"b".to_vec());
        let entry = crate::cluster::types::LogEntry {
            log_id: LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(1), 1),
            payload: EntryPayload::Normal(Request::WriteBatch(ops)),
        };
        let responses = storage
            .apply_entries_internal(std::slice::from_ref(&entry))
            .unwrap();
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            Response::WriteStats { effects } => {
                assert_eq!(effects.as_slice(), &[true, true, true]);
            }
            other => panic!("expected WriteStats, got {other:?}"),
        }
        assert!(
            storage
                .db
                .key_exists(&sm_key(DEFAULT_GROUP_ID, b"k"))
                .unwrap(),
            "最终 key 应存在"
        );
    }

    #[test]
    fn test_request_put_conditional() {
        let (storage, _dir) = test_storage();
        storage
            .apply_put_conditional(b"k".to_vec(), b"v1".to_vec(), None)
            .unwrap();
        storage
            .apply_put_conditional(b"k".to_vec(), b"v2".to_vec(), None)
            .unwrap();
        assert_eq!(
            storage.get_state_machine_value(b"k").unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            storage.db.get(&sm_key(DEFAULT_GROUP_ID, b"k")).unwrap(),
            Some(b"v1".to_vec())
        );
    }

    /// FIX-0056-A1 不变式 #1: 迁移期用户写与 tombstone/tip 更新必须在**同一次**
    /// `apply_entries_internal` 调用内一起可见 (同 Raft entry 原子).
    #[test]
    fn test_migration_write_lands_user_op_and_tombstone_in_same_apply() {
        let (storage, _dir) = test_storage();
        use openraft::{EntryPayload, LogId};

        let mut ops = ThinWriteBatch::new();
        ops.put(b"k1".to_vec(), b"v1".to_vec());
        ops.delete(b"k2".to_vec());
        let entry = crate::cluster::types::LogEntry {
            log_id: LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(1), 1),
            payload: EntryPayload::Normal(Request::MigrationWrite { epoch: 7, ops }),
        };
        storage
            .apply_entries_internal(std::slice::from_ref(&entry))
            .unwrap();

        assert_eq!(
            storage.get_state_machine_value(b"k1").unwrap(),
            Some(b"v1".to_vec()),
            "Put op 必须落地 sm_key"
        );
        assert!(
            storage.get_state_machine_value(b"k2").unwrap().is_none(),
            "Delete op 必须落地 sm_key (即使 key 本不存在)"
        );

        let ts1 = storage
            .db
            .get(&mig_tombstone_key(DEFAULT_GROUP_ID, 7, b"k1"))
            .unwrap()
            .expect("Put op 必须同 entry 写入 tombstone");
        assert_eq!(decode_tombstone(&ts1), Some((MigOp::Put, 1)));
        let ts2 = storage
            .db
            .get(&mig_tombstone_key(DEFAULT_GROUP_ID, 7, b"k2"))
            .unwrap()
            .expect("Delete op 必须同 entry 写入 tombstone");
        assert_eq!(decode_tombstone(&ts2), Some((MigOp::Del, 2)));
        assert_eq!(
            storage.get_migration_tip(7).unwrap(),
            2,
            "tip 必须在同一次 apply 内推进到本批最后一个 seq"
        );
    }

    /// seq/tip 只在 Raft apply 内单调分配, 跨多次 apply 必须从上次的 tip 续接.
    #[test]
    fn test_migration_write_tip_continues_across_separate_applies() {
        let (storage, _dir) = test_storage();
        use openraft::{EntryPayload, LogId};

        let mk_entry = |index: u64, key: &'static [u8]| {
            let mut ops = ThinWriteBatch::new();
            ops.put(key.to_vec(), b"v".to_vec());
            crate::cluster::types::LogEntry {
                log_id: LogId::new(
                    openraft::vote::leader_id_std::CommittedLeaderId::new(1),
                    index,
                ),
                payload: EntryPayload::Normal(Request::MigrationWrite { epoch: 3, ops }),
            }
        };
        storage
            .apply_entries_internal(&[mk_entry(1, b"a")])
            .unwrap();
        assert_eq!(storage.get_migration_tip(3).unwrap(), 1);
        storage
            .apply_entries_internal(&[mk_entry(2, b"b")])
            .unwrap();
        assert_eq!(storage.get_migration_tip(3).unwrap(), 2);
        // 不同 epoch 互不影响.
        assert_eq!(storage.get_migration_tip(4).unwrap(), 0);
    }

    /// FIX-0056-A1 不变式 #2/#3: Del tombstone 必须阻止 PutConditional 复活.
    #[test]
    fn test_put_conditional_skips_when_del_tombstone_present() {
        let (storage, _dir) = test_storage();
        let mut del_ops = ThinWriteBatch::new();
        del_ops.delete(b"k".to_vec());
        storage.apply_migration_write_to_sm(5, &del_ops).unwrap();
        assert!(storage.get_state_machine_value(b"k").unwrap().is_none());

        storage
            .apply_put_conditional(b"k".to_vec(), b"v1".to_vec(), Some(5))
            .unwrap();
        assert!(
            storage.get_state_machine_value(b"k").unwrap().is_none(),
            "Del tombstone 必须阻止 PutConditional 复活 key"
        );
    }

    /// 没有任何 tombstone (从未在该 epoch 下动过这个 key) 时, PutConditional
    /// 即使带 `migration_epoch` 也要正常生效 —— 只有 Del tombstone 才拦截.
    #[test]
    fn test_put_conditional_applies_when_no_tombstone_even_with_epoch() {
        let (storage, _dir) = test_storage();
        storage
            .apply_put_conditional(b"k".to_vec(), b"v1".to_vec(), Some(9))
            .unwrap();
        assert_eq!(
            storage.get_state_machine_value(b"k").unwrap(),
            Some(b"v1".to_vec())
        );
    }

    /// Put tombstone (非 Del) 不触发"复活拦截"; 仍走常规"已存在则跳过"逻辑.
    #[test]
    fn test_put_conditional_applies_when_tombstone_is_put() {
        let (storage, _dir) = test_storage();
        let mut put_ops = ThinWriteBatch::new();
        put_ops.put(b"k".to_vec(), b"already-there".to_vec());
        storage.apply_migration_write_to_sm(2, &put_ops).unwrap();

        storage
            .apply_put_conditional(b"k".to_vec(), b"v2".to_vec(), Some(2))
            .unwrap();
        assert_eq!(
            storage.get_state_machine_value(b"k").unwrap(),
            Some(b"already-there".to_vec()),
            "Put tombstone 不拦截, 但常规去重逻辑仍应跳过已存在的 key"
        );
    }

    /// GC (`Request::MigrationGc`) 必须删干净该 epoch 的全部 tombstone/tip,
    /// 且不影响其它 epoch / sm_key 上的用户数据.
    #[test]
    fn test_migration_gc_removes_epoch_prefix_only() {
        let (storage, _dir) = test_storage();
        use openraft::{EntryPayload, LogId};

        let mut ops = ThinWriteBatch::new();
        ops.put(b"k1".to_vec(), b"v1".to_vec());
        ops.delete(b"k2".to_vec());
        storage.apply_migration_write_to_sm(1, &ops).unwrap();
        storage.apply_migration_write_to_sm(2, &ops).unwrap();
        assert_eq!(storage.get_migration_tip(1).unwrap(), 2);
        assert_eq!(storage.get_migration_tip(2).unwrap(), 2);
        assert_eq!(
            storage.get_state_machine_value(b"k1").unwrap(),
            Some(b"v1".to_vec())
        );

        let gc_entry = crate::cluster::types::LogEntry {
            log_id: LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(1), 1),
            payload: EntryPayload::Normal(Request::MigrationGc { epoch: 1 }),
        };
        storage
            .apply_entries_internal(std::slice::from_ref(&gc_entry))
            .unwrap();

        assert_eq!(
            storage.get_migration_tip(1).unwrap(),
            0,
            "epoch 1 的 tip 必须被 GC 清零"
        );
        assert!(
            storage
                .db
                .get(&mig_tombstone_key(DEFAULT_GROUP_ID, 1, b"k1"))
                .unwrap()
                .is_none(),
            "epoch 1 的 tombstone 必须被 GC 删除"
        );
        assert_eq!(
            storage.get_migration_tip(2).unwrap(),
            2,
            "GC 不应影响其它 epoch"
        );
        assert_eq!(
            storage.get_state_machine_value(b"k1").unwrap(),
            Some(b"v1".to_vec()),
            "GC 只清 mig/ 前缀, 不动用户数据 sm_key"
        );
    }

    #[test]
    fn test_append_request_to_batch_includes_sm_only() {
        let (storage, _dir) = test_storage();
        let mut batch = WriteBatch::new();
        let mut overlay = HashMap::new();
        storage
            .append_request_to_batch(
                &mut batch,
                &Request::Put {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                },
                &mut overlay,
            )
            .unwrap();
        assert_eq!(batch.len(), 1);
        assert!(batch
            .operations
            .iter()
            .all(|op| matches!(op, crate::engine::db::WriteOp::Put { .. })));
    }

    #[test]
    fn test_apply_idempotent_last_applied() {
        let (storage, _dir) = test_storage();
        use openraft::{EntryPayload, LogId};
        let entry = crate::cluster::types::LogEntry {
            log_id: LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(1), 1),
            payload: EntryPayload::Normal(Request::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }),
        };
        storage
            .apply_entries_internal(std::slice::from_ref(&entry))
            .unwrap();
        storage
            .apply_entries_internal(std::slice::from_ref(&entry))
            .unwrap();
        assert_eq!(
            storage.get_state_machine_value(b"k").unwrap(),
            Some(b"v".to_vec())
        );
        assert_eq!(
            storage
                .read_last_applied_from_db()
                .unwrap()
                .map(|id| id.index),
            Some(1)
        );
    }

    #[test]
    fn test_meta_storage_apply_output_integration() {
        let (storage, _dir) = meta_storage();
        use openraft::{EntryPayload, LogId};
        let entry = crate::cluster::types::LogEntry {
            log_id: LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(1), 1),
            payload: EntryPayload::Normal(Request::Meta(MetaRequest::RegisterNode {
                node_id: 1,
                rpc_addr: "http://127.0.0.1:1".into(),
                client_addr: None,
                tags: HashMap::new(),
            })),
        };
        storage.apply_entries_internal(&[entry]).unwrap();
        assert!(storage.db.get(&meta_cluster_meta_key()).unwrap().is_some());
        assert_eq!(
            storage
                .read_last_applied_from_db()
                .unwrap()
                .map(|id| id.index),
            Some(1)
        );
    }

    /// FIX-0056-A1: `migration_epoch` 必须随真实 Raft apply (经
    /// `apply_meta_entry` 写入同一 `WriteBatch`) 落盘, 并在"重启" (对同一
    /// 底层 db 重新构造 `MetaStateMachine`) 后被 `reload_from_db` 恢复 ——
    /// 与 `migration_state` 同等的 failover 安全性.
    #[test]
    fn test_migration_epoch_persists_through_apply_and_restart() {
        use crate::cluster::meta_state_machine::MetaStateMachine;
        use openraft::{EntryPayload, LogId};

        let (storage, _dir) = meta_storage();
        let entries = [
            MetaRequest::RegisterNode {
                node_id: 1,
                rpc_addr: "http://127.0.0.1:1".into(),
                client_addr: None,
                tags: HashMap::new(),
            },
            MetaRequest::CreateGroup {
                group_id: 1,
                initial_replicas: vec![(1, true)],
            },
            MetaRequest::CreateGroup {
                group_id: 2,
                initial_replicas: vec![(1, true)],
            },
            MetaRequest::AssignSlots {
                group_id: 1,
                slots: vec![0],
            },
            MetaRequest::BeginSlotMigration {
                source_group: 1,
                target_group: 2,
                slots: vec![0],
            },
        ];
        for (i, req) in entries.into_iter().enumerate() {
            let entry = crate::cluster::types::LogEntry {
                log_id: LogId::new(
                    openraft::vote::leader_id_std::CommittedLeaderId::new(1),
                    i as u64 + 1,
                ),
                payload: EntryPayload::Normal(Request::Meta(req)),
            };
            storage
                .apply_entries_internal(std::slice::from_ref(&entry))
                .unwrap();
        }

        let meta_state = storage.meta_state.as_ref().unwrap();
        let epoch_before = meta_state
            .get_migration_epoch()
            .expect("BeginSlotMigration 后 epoch 必须可读");
        assert_eq!(epoch_before, meta_state.get_cluster_meta().version);

        // 模拟重启: 对同一底层 db 重新构造 MetaStateMachine (走 reload_from_db).
        let restarted = MetaStateMachine::new(storage.db.clone()).unwrap();
        assert_eq!(
            restarted.get_migration_epoch(),
            Some(epoch_before),
            "重启后 migration_epoch 必须从 db 恢复, 与 migration_state 一致"
        );
        assert!(restarted.get_migration_state().is_some());
    }

    /// 回归测试: apply 过程中真实的存储故障必须直接向上抛错, 绝不能把
    /// last_applied 悄悄推进到失败的 entry —— 这正是修复前的 P0 bug
    /// (last_applied 越过失败 entry, 数据永久丢失且副本间可能分叉).
    #[test]
    fn test_apply_storage_error_does_not_advance_last_applied() {
        use openraft::{EntryPayload, LogId};

        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let storage = OpenRaftStorage::new(db.clone(), DEFAULT_GROUP_ID, None).unwrap();

        // Entry #1 applies cleanly.
        let e1 = crate::cluster::types::LogEntry {
            log_id: LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(1), 1),
            payload: EntryPayload::Normal(Request::Put {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            }),
        };
        storage
            .apply_entries_internal(std::slice::from_ref(&e1))
            .unwrap();
        assert_eq!(
            storage
                .read_last_applied_from_db()
                .unwrap()
                .map(|id| id.index),
            Some(1)
        );

        // Force a genuine storage I/O failure on the *next* entry: closing the
        // DB makes any further `db.get()`/`db.write()` return Err, simulating
        // e.g. a disk fault hit by PutConditional's dedup read.
        db.close().unwrap();

        let e2 = crate::cluster::types::LogEntry {
            log_id: LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(1), 2),
            payload: EntryPayload::Normal(Request::PutConditional {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                migration_epoch: None,
            }),
        };
        let result = storage.apply_entries_internal(std::slice::from_ref(&e2));
        assert!(
            result.is_err(),
            "a genuine storage error must propagate as Err, not be swallowed as a per-entry business Response::Error"
        );

        // In-memory last_applied must still be at index 1 — the failed entry
        // #2 was never marked as applied, in memory or (transitively) on disk.
        assert_eq!(
            storage.state.read().last_applied.map(|id| id.index),
            Some(1),
            "last_applied must not advance past the entry that failed to apply"
        );
    }

    #[test]
    fn test_membership_persist() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();
        {
            let db = DB::open(&path, Options::for_testing()).unwrap();
            let storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();
            use crate::cluster::types::NodeId;
            use openraft::{BasicNode, EntryPayload, LogId, Membership};
            use std::collections::{BTreeMap, BTreeSet};
            let mut voters = BTreeSet::new();
            voters.insert(1);
            voters.insert(2);
            voters.insert(3);
            let mut nodes = BTreeMap::new();
            for id in [1u64, 2, 3] {
                nodes.insert(
                    id,
                    BasicNode {
                        addr: format!("http://127.0.0.1:{id}"),
                    },
                );
            }
            let membership = Membership::<NodeId, BasicNode>::new(vec![voters], nodes).unwrap();
            let entry = crate::cluster::types::LogEntry {
                log_id: LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(1), 1),
                payload: EntryPayload::Membership(membership),
            };
            storage.apply_entries_internal(&[entry]).unwrap();
        }
        let db = DB::open(&path, Options::for_testing()).unwrap();
        let storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();
        let loaded = storage.load_membership().unwrap();
        assert_eq!(loaded.log_id().map(|id| id.index), Some(1));
        assert_eq!(loaded.membership().get_joint_config().len(), 1);
        assert_eq!(
            storage
                .read_last_applied_from_db()
                .unwrap()
                .map(|id| id.index),
            Some(1)
        );
    }
}
