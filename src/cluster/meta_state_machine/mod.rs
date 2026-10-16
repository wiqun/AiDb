//! MetaRaft 状态机 — 集群元数据 (节点 / Group / SlotTable / 迁移状态) 的权威
//! apply 路径. 本文件承载控制面 gid=0 上的 Raft 状态机: `MetaRequest` 先经
//! `validate_meta_request` (validate_with_state 只读校验) 再 `apply_meta_request`
//! (apply_mutate 修改内存态), 产出 `ApplyOutput` 交给外层 `storage/apply.rs`
//! 以单 WriteBatch 原子落库 (与 `last_applied` 同批).
//!
//! # 数据流 (apply 流程)
//!
//! ```text
//! MetaRequest (以 Raft Normal entry 到达, 仅作用于 gid=0)
//!   ├─ validate_meta_request
//!   │    └─ validate_with_state 只读 cluster_meta / slot_table / migration_state
//!   ├─ apply_meta_request
//!   │    ├─ apply_mutate 修改内存态 (cluster_meta / slot_table / migration_state)
//!   │    ├─ cluster_meta.version += 1
//!   │    ├─ migration_epoch: Begin -> 新 version; Commit/Cancel -> None
//!   │    └─ ApplyOutput { kv_pairs }: 4 个 meta key 的序列化快照
//!   └─ storage/apply.rs 单 WriteBatch 写 \x00meta_raft/* + last_applied
//! ```
//!
//! 持久化 key 见 `storage/keys.rs` 的 `\x00meta_raft/*` (cluster_meta / slot_table /
//! migration_state / migration_epoch). 启动时经 `reload_from_db` 恢复内存态;
//! 若发现 `format_version > 1` 则报 `Error::Corruption` (防止旧版本数据被静默误读).
//!
//! # Invariant
//!
//! - 每次成功 apply 后 `ClusterMeta.version += 1`; `BumpEpoch` 先 +1 再 apply
//!   (净 +2), 保证其 version 变化与普通变更可区分.
//! - Slot 状态转换受限: `AssignSlots` 仅接受 `Unallocated` 槽; 迁移起点必须是
//!   `Assigned(source)`. 违反视为 InvalidState, 由 leader 权威拒绝.
//! - 迁移相位严格推进: Prepare -> Migrating -> Frozen -> ReadyToCommit -> Commit,
//!   commit 阶段只接受 `ReadyToCommit` 目标.
//! - 内存态与磁盘态一致: apply 仅在全部修改成功后才落 ApplyOutput; 内存态是
//!   Raft 共识的唯一事实来源, 磁盘只做持久化.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::cluster::meta_types::{
    default_slot_table, ClusterMeta, MetaRequest, SlotMigrationState, SlotTable, SLOT_COUNT,
};
use crate::cluster::storage::keys::{
    meta_cluster_meta_key, meta_migration_epoch_key, meta_migration_state_key, meta_slot_table_key,
};
use crate::cluster::types::ClusterError;
use crate::error::{Error, Result};

type MetaSmResult<T> = std::result::Result<T, ClusterError>;

use crate::DB;

mod apply;
#[cfg(test)]
mod tests;
mod validate;

pub use apply::rebuild_slot_ranges;

/// MetaRaft apply output — persisted atomically by outer storage apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyOutput {
    pub kv_pairs: Vec<(Vec<u8>, Vec<u8>)>,
}

pub struct MetaStateMachine {
    db: Arc<DB>,
    cluster_meta: RwLock<ClusterMeta>,
    slot_table: RwLock<SlotTable>,
    migration_state: RwLock<Option<SlotMigrationState>>,
    /// FIX-0056-A1: 当前活跃迁移的 oplog epoch (= `BeginSlotMigration` apply
    /// 后的 `cluster_meta.version`, 与 `SlotMigrationManager::start_migration`
    /// 事后读到的 `migration_id` 一致). 与 `migration_state` 同生命周期, 但不
    /// 放进 `SlotMigrationState` 本身 —— 避免改动其穷尽 match 的所有调用点
    /// (aidb `apply_mutate`/`membership_coordinator` 与 aikv `router`/
    /// `cluster_adapter`), 是这条信息的最小侵入落地方式.
    migration_epoch: RwLock<Option<u64>>,
}

impl MetaStateMachine {
    pub fn new(db: Arc<DB>) -> Result<Self> {
        let sm = Self {
            db,
            cluster_meta: RwLock::new(ClusterMeta::default()),
            slot_table: RwLock::new(default_slot_table()),
            migration_state: RwLock::new(None),
            migration_epoch: RwLock::new(None),
        };
        sm.reload_from_db()?;
        Ok(sm)
    }

    pub fn reload_from_db(&self) -> Result<()> {
        let cluster_meta = match self.db.get(&meta_cluster_meta_key())? {
            Some(bytes) => {
                let meta: ClusterMeta = rmp_serde::from_slice(&bytes).map_err(|e| {
                    Error::Cluster(ClusterError::Serialization(format!("cluster_meta: {}", e)))
                })?;
                if meta.format_version > 1 {
                    return Err(Error::Corruption(format!(
                        "unsupported meta format_version {}",
                        meta.format_version
                    )));
                }
                meta
            }
            None => ClusterMeta::default(),
        };

        let slot_table = match self.db.get(&meta_slot_table_key())? {
            Some(bytes) => {
                let table: SlotTable = rmp_serde::from_slice(&bytes).map_err(|e| {
                    Error::Cluster(ClusterError::Serialization(format!("slot_table: {}", e)))
                })?;
                if table.len() != SLOT_COUNT {
                    return Err(Error::Corruption(format!(
                        "invalid slot_table length {}",
                        table.len()
                    )));
                }
                table
            }
            None => default_slot_table(),
        };

        let migration_state = match self.db.get(&meta_migration_state_key())? {
            Some(bytes) => rmp_serde::from_slice(&bytes).map_err(|e| {
                Error::Cluster(ClusterError::Serialization(format!(
                    "migration_state: {}",
                    e
                )))
            })?,
            None => None,
        };

        let migration_epoch = match self.db.get(&meta_migration_epoch_key())? {
            Some(bytes) => rmp_serde::from_slice(&bytes).map_err(|e| {
                Error::Cluster(ClusterError::Serialization(format!(
                    "migration_epoch: {}",
                    e
                )))
            })?,
            None => None,
        };

        *self.cluster_meta.write() = cluster_meta;
        *self.slot_table.write() = slot_table;
        *self.migration_state.write() = migration_state;
        *self.migration_epoch.write() = migration_epoch;
        Ok(())
    }

    pub fn get_cluster_meta(&self) -> ClusterMeta {
        self.cluster_meta.read().clone()
    }

    pub fn get_slot_table(&self) -> SlotTable {
        self.slot_table.read().clone()
    }

    pub fn get_migration_state(&self) -> Option<SlotMigrationState> {
        self.migration_state.read().clone()
    }

    /// FIX-0056-A1: 当前活跃迁移的 oplog epoch; 无活跃迁移时为 `None`.
    /// 供 aikv 迁移期写 (`Request::MigrationWrite`) / 合并读定位
    /// `mig/{gid}/{epoch}/*` tombstone/tip.
    pub fn get_migration_epoch(&self) -> Option<u64> {
        *self.migration_epoch.read()
    }

    /// Directly set migration state (for testing).
    /// Skips Raft consensus — only use in test scenarios.
    pub fn set_migration_state(&self, state: Option<SlotMigrationState>) {
        *self.migration_state.write() = state;
    }

    /// Directly set slot table (for testing).
    /// Skips Raft consensus — only use in test scenarios.
    pub fn set_slot_table(&self, table: SlotTable) {
        *self.slot_table.write() = table;
    }

    pub fn validate_meta_request(&self, request: &MetaRequest) -> MetaSmResult<()> {
        let cluster_meta = self.cluster_meta.read();
        let slot_table = self.slot_table.read();
        let migration_state = self.migration_state.read();
        validate::validate_with_state(request, &cluster_meta, &slot_table, &migration_state)
    }

    pub fn apply_meta_request(&self, request: MetaRequest) -> MetaSmResult<ApplyOutput> {
        self.validate_meta_request(&request)?;

        // FIX-0056-A1: request 在下面被 `apply_mutate` 按值消费, 先记下是否
        // Begin/Commit/Cancel 以便之后决定 migration_epoch 的生死.
        let is_begin_migration = matches!(request, MetaRequest::BeginSlotMigration { .. });
        let is_migration_end = matches!(
            request,
            MetaRequest::CommitSlotMigration | MetaRequest::CancelSlotMigration
        );

        let mut cluster_meta = self.cluster_meta.write();
        let mut slot_table = self.slot_table.write();
        let mut migration_state = self.migration_state.write();

        apply::apply_mutate(
            request,
            &mut cluster_meta,
            &mut slot_table,
            &mut migration_state,
        )?;

        cluster_meta.version += 1;

        // migration_epoch 与 migration_state 同生命周期: Begin 时取本次 apply
        // 后的新 version (与 `SlotMigrationManager::start_migration` 事后读到
        // 的 `cluster_meta.version` 一致); Commit/Cancel 时随 migration_state
        // 一起清空.
        let mut migration_epoch = self.migration_epoch.write();
        if is_begin_migration {
            *migration_epoch = Some(cluster_meta.version);
        } else if is_migration_end {
            *migration_epoch = None;
        }

        let kv_pairs = vec![
            (
                meta_cluster_meta_key(),
                rmp_serde::to_vec(&*cluster_meta)
                    .map_err(|e| ClusterError::Serialization(e.to_string()))?,
            ),
            (
                meta_slot_table_key(),
                rmp_serde::to_vec(&*slot_table)
                    .map_err(|e| ClusterError::Serialization(e.to_string()))?,
            ),
            (
                meta_migration_state_key(),
                rmp_serde::to_vec(&*migration_state)
                    .map_err(|e| ClusterError::Serialization(e.to_string()))?,
            ),
            (
                meta_migration_epoch_key(),
                rmp_serde::to_vec(&*migration_epoch)
                    .map_err(|e| ClusterError::Serialization(e.to_string()))?,
            ),
        ];

        Ok(ApplyOutput { kv_pairs })
    }
}
