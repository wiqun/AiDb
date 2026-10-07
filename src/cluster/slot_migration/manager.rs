use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;

use crate::cluster::meta_raft_node::MetaRaftNode;
use crate::cluster::meta_types::{MetaRequest, SlotMigrationState, SLOT_COUNT};
use crate::cluster::multi_raft_node::MultiRaftNode;
use crate::cluster::router::Router;
use crate::cluster::types::{ClusterError, NodeId, Request, Response};
use crate::config::MigrationConfig;
use crate::error::Result;

use super::executor::SlotMigrationExecutor;
use super::{
    ActiveMigration, BatchMigrateResult, CompletedRun, MigrationPhase, MigrationProgress,
    MigrationRunRecord, CANCEL_META_WAIT_ATTEMPTS, CANCEL_META_WAIT_MS, MIGRATION_RUN_FILE,
    QUIESCE_STABLE_MS, QUIESCE_TIMEOUT_MS, QUIESCE_TOKEN_FILE,
};

// ---------------------------------------------------------------------------
// SlotMigrationManager
// ---------------------------------------------------------------------------

pub struct SlotMigrationManager {
    meta_raft: Arc<MetaRaftNode>,
    multi_raft: Arc<MultiRaftNode>,
    // kept for reserved/future use
    #[expect(dead_code)]
    router: Arc<Router>,
    node_id: NodeId,
    executor: RwLock<Option<SlotMigrationExecutor>>,
    /// 最近一次 `run_pending_migration` 的结果, 用于收尾 / commit 校验.
    last_run: RwLock<Option<CompletedRun>>,
    checkpoint_dir: PathBuf,
    config: MigrationConfig,
}

impl SlotMigrationManager {
    pub fn new(
        meta_raft: Arc<MetaRaftNode>,
        multi_raft: Arc<MultiRaftNode>,
        router: Arc<Router>,
        node_id: NodeId,
        checkpoint_dir: PathBuf,
        config: MigrationConfig,
    ) -> Self {
        std::fs::create_dir_all(&checkpoint_dir).ok();
        let last_run = Self::load_run_record(&checkpoint_dir);
        Self {
            meta_raft,
            multi_raft,
            router,
            node_id,
            executor: RwLock::new(None),
            last_run: RwLock::new(last_run),
            checkpoint_dir,
            config,
        }
    }

    fn run_record_path(dir: &std::path::Path) -> PathBuf {
        dir.join(MIGRATION_RUN_FILE)
    }

    fn quiesce_token_path(&self) -> PathBuf {
        self.checkpoint_dir.join(QUIESCE_TOKEN_FILE)
    }

    fn load_run_record(dir: &std::path::Path) -> Option<MigrationRunRecord> {
        let path = Self::run_record_path(dir);
        let bytes = std::fs::read(&path).ok()?;
        postcard::from_bytes(&bytes).ok()
    }

    fn persist_run_record(&self, record: &MigrationRunRecord) -> Result<()> {
        let path = Self::run_record_path(&self.checkpoint_dir);
        let tmp = self
            .checkpoint_dir
            .join(format!("{}.tmp", MIGRATION_RUN_FILE));
        let bytes = postcard::to_allocvec(record)
            .map_err(|e| ClusterError::Serialization(e.to_string()))?;
        std::fs::write(&tmp, bytes).map_err(ClusterError::Io)?;
        std::fs::rename(&tmp, &path).map_err(ClusterError::Io)?;
        Ok(())
    }

    fn clear_run_record(&self) {
        let _ = std::fs::remove_file(Self::run_record_path(&self.checkpoint_dir));
        *self.last_run.write() = None;
    }

    fn write_quiesce_token(&self, token: u64) -> Result<()> {
        let path = self.quiesce_token_path();
        let tmp = self
            .checkpoint_dir
            .join(format!("{}.tmp", QUIESCE_TOKEN_FILE));
        std::fs::write(&tmp, token.to_string()).map_err(ClusterError::Io)?;
        std::fs::rename(&tmp, &path).map_err(ClusterError::Io)?;
        Ok(())
    }

    fn read_quiesce_token(&self) -> Option<u64> {
        let s = std::fs::read_to_string(self.quiesce_token_path()).ok()?;
        s.trim().parse().ok()
    }

    fn clear_quiesce_token(&self) {
        let _ = std::fs::remove_file(self.quiesce_token_path());
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// 单 key 迁移常达不到 `progress_report_interval`, Meta 可能仍停在 Prepare;
    /// Freeze 要求 Migrating, 因此在收尾前补一条进度推进.
    async fn ensure_migrating_phase(&self) -> Result<()> {
        match self.meta_raft.get_migration_state() {
            Some(SlotMigrationState::Prepare { .. }) => {
                let total = self
                    .last_run
                    .read()
                    .as_ref()
                    .map(|r| r.slots.len().max(1) as u64)
                    .unwrap_or(1);
                let progress = total.max(1);
                match self
                    .meta_raft
                    .propose(MetaRequest::UpdateMigrationProgress { progress, total })
                    .await?
                {
                    Response::Ok => Ok(()),
                    Response::Error(e) => Err(ClusterError::InvalidState(e).into()),
                    other => Err(ClusterError::Internal(format!(
                        "unexpected response advancing migration phase: {other:?}"
                    ))
                    .into()),
                }
            }
            Some(SlotMigrationState::Migrating { .. })
            | Some(SlotMigrationState::Frozen { .. })
            | Some(SlotMigrationState::ReadyToCommit { .. }) => Ok(()),
            None => Err(ClusterError::InvalidState("no active migration".into()).into()),
        }
    }

    fn require_completed_run_matching(
        &self,
        source_group: u64,
        target_group: u64,
        slots: &[u16],
    ) -> Result<MigrationRunRecord> {
        if self.executor.read().is_some() {
            return Err(ClusterError::InvalidState(
                "migration has not been executed yet (run_pending_migration was never called)"
                    .into(),
            )
            .into());
        }
        let run = self
            .last_run
            .read()
            .clone()
            .or_else(|| Self::load_run_record(&self.checkpoint_dir));
        match run {
            Some(r)
                if r.completed
                    && r.source_group == source_group
                    && r.target_group == target_group
                    && r.slots == slots
                    && r.node_id == self.node_id =>
            {
                Ok(r)
            }
            Some(_) => Err(ClusterError::InvalidState(
                "migration run record does not match current (source, target, slots) or node"
                    .into(),
            )
            .into()),
            None => Err(ClusterError::InvalidState(
                "migration progress not verified as complete for the current (source, target, \
                 slots); run_pending_migration must finish with is_completed=true first"
                    .into(),
            )
            .into()),
        }
    }

    /// Migrating → Frozen. 要求本节点已完成匹配的 execute.
    pub async fn freeze_for_commit(&self) -> Result<()> {
        let (source_group, target_group, slots) = self.current_migration_signature()?;
        self.require_completed_run_matching(source_group, target_group, &slots)?;
        self.ensure_migrating_phase().await?;

        match self
            .meta_raft
            .propose(MetaRequest::FreezeSlotMigration)
            .await?
        {
            Response::Ok => {}
            Response::Error(e) => return Err(ClusterError::InvalidState(e).into()),
            other => {
                return Err(ClusterError::Internal(format!(
                    "unexpected FreezeSlotMigration response: {other:?}"
                ))
                .into());
            }
        }

        // 确保磁盘上有 run record (execute 成功时已写; 此处幂等加固).
        if let Some(run) = self.last_run.read().clone() {
            self.persist_run_record(&run)?;
        }
        Ok(())
    }

    /// Frozen 后向 target propose 写屏障并短暂稳定观察, 再落盘 quiesce_token.
    pub async fn quiesce_writes(&self) -> Result<()> {
        match self.meta_raft.get_migration_state() {
            Some(SlotMigrationState::Frozen { target_group, .. }) => {
                let target = target_group;
                let work = async {
                    let token = Self::now_ms() ^ self.node_id.wrapping_mul(0x9e37);
                    match self
                        .multi_raft
                        .propose_group(target, Request::MigrationBarrier { token })
                        .await?
                    {
                        Response::Ok => {}
                        Response::Error(e) => {
                            return Err(ClusterError::InvalidState(format!(
                                "migration barrier failed: {e}"
                            ))
                            .into());
                        }
                        other => {
                            return Err(ClusterError::Internal(format!(
                                "unexpected MigrationBarrier response: {other:?}"
                            ))
                            .into());
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(QUIESCE_STABLE_MS)).await;
                    self.write_quiesce_token(token)?;
                    Ok(())
                };
                match tokio::time::timeout(
                    std::time::Duration::from_millis(QUIESCE_TIMEOUT_MS),
                    work,
                )
                .await
                {
                    Ok(r) => r,
                    Err(_) => Err(ClusterError::Timeout("quiesce_writes timed out".into()).into()),
                }
            }
            Some(_) => Err(ClusterError::InvalidState(
                "quiesce_writes requires Frozen migration phase".into(),
            )
            .into()),
            None => Err(ClusterError::InvalidState("no active migration".into()).into()),
        }
    }

    /// FIX-0056-A1: Frozen 后确认 target group 迁移 oplog tip 已稳定
    /// (`quiesce_writes` 之后, `final_verify` 之前的收尾前置) —— 两次相隔
    /// `QUIESCE_STABLE_MS` 读到的 tip 相等即认为 drain 完成. tip 在
    /// `MigrationWrite` apply 内单调递增, Frozen 下没有新客户端写会自然稳定.
    pub async fn drain_oplog_tip_stable(&self) -> Result<()> {
        let (source_group, target_group, slots) = match self.meta_raft.get_migration_state() {
            Some(SlotMigrationState::Frozen {
                source_group,
                target_group,
                slots,
            }) => (source_group, target_group, slots),
            Some(_) => {
                return Err(ClusterError::InvalidState(
                    "drain_oplog_tip_stable requires Frozen migration phase".into(),
                )
                .into());
            }
            None => return Err(ClusterError::InvalidState("no active migration".into()).into()),
        };
        let run = self.require_completed_run_matching(source_group, target_group, &slots)?;
        let epoch = run.migration_id;

        let work = async {
            let first = self
                .multi_raft
                .read_migration_tip(target_group, epoch)
                .await?;
            tokio::time::sleep(std::time::Duration::from_millis(QUIESCE_STABLE_MS)).await;
            let second = self
                .multi_raft
                .read_migration_tip(target_group, epoch)
                .await?;
            if first != second {
                return Err(ClusterError::InvalidState(
                    "migration oplog tip not stable yet, retry drain".into(),
                )
                .into());
            }
            Ok(())
        };
        match tokio::time::timeout(std::time::Duration::from_millis(QUIESCE_TIMEOUT_MS), work).await
        {
            Ok(r) => r,
            Err(_) => Err(ClusterError::Timeout("drain_oplog_tip_stable timed out".into()).into()),
        }
    }

    /// 收尾就绪检查: Frozen + run record 签名匹配 + quiesce_token 存在.
    /// 不做 source⊆target 存在性比对, 也不做 PutConditional 补拷.
    /// FIX-0056-A1: 调用顺序上位于 `drain_oplog_tip_stable` 之后 (见
    /// `finish_migration`) —— 本函数不重复校验 tip 是否稳定, 只信任链路顺序.
    pub async fn final_verify(&self) -> Result<()> {
        let (source_group, target_group, slots) = match self.meta_raft.get_migration_state() {
            Some(SlotMigrationState::Frozen {
                source_group,
                target_group,
                slots,
            }) => (source_group, target_group, slots),
            Some(_) => {
                return Err(ClusterError::InvalidState(
                    "final_verify requires Frozen migration phase".into(),
                )
                .into());
            }
            None => {
                return Err(ClusterError::InvalidState("no active migration".into()).into());
            }
        };
        self.require_completed_run_matching(source_group, target_group, &slots)?;
        if self.read_quiesce_token().is_none() {
            return Err(ClusterError::InvalidState(
                "quiesce_token missing; quiesce_writes must succeed before final_verify".into(),
            )
            .into());
        }
        Ok(())
    }

    /// Frozen → ReadyToCommit. 要求 quiesce_token 已落盘 (选择 A: Manager 内校验).
    pub async fn mark_ready(&self) -> Result<()> {
        self.final_verify().await?;
        if self.read_quiesce_token().is_none() {
            return Err(ClusterError::InvalidState(
                "quiesce_token missing; cannot MarkMigrationReady".into(),
            )
            .into());
        }
        match self
            .meta_raft
            .propose(MetaRequest::MarkMigrationReady)
            .await?
        {
            Response::Ok => {}
            Response::Error(e) => return Err(ClusterError::InvalidState(e).into()),
            other => {
                return Err(ClusterError::Internal(format!(
                    "unexpected MarkMigrationReady response: {other:?}"
                ))
                .into());
            }
        }
        self.clear_quiesce_token();
        Ok(())
    }

    /// 完整收尾链: freeze → quiesce → drain_oplog_tip_stable → final_verify
    /// → mark_ready → commit.
    pub async fn finish_migration(&self) -> Result<()> {
        self.freeze_for_commit().await?;
        self.quiesce_writes().await?;
        self.drain_oplog_tip_stable().await?;
        self.final_verify().await?;
        self.mark_ready().await?;
        self.commit_migration().await
    }

    pub async fn start_migration(
        &self,
        source_group: u64,
        target_group: u64,
        slots: Vec<u16>,
    ) -> Result<u64> {
        // Validate
        if source_group == target_group {
            return Err(
                ClusterError::InvalidConfig("source and target group must differ".into()).into(),
            );
        }
        if slots.is_empty() {
            return Err(ClusterError::InvalidConfig("slots must not be empty".into()).into());
        }
        {
            let mut sorted = slots.clone();
            sorted.sort();
            sorted.dedup();
            if sorted.len() != slots.len() {
                return Err(ClusterError::InvalidConfig("slots contains duplicates".into()).into());
            }
        }
        for &s in &slots {
            if s >= SLOT_COUNT as u16 {
                return Err(ClusterError::InvalidConfig("slot out of range".into()).into());
            }
        }

        let cluster_meta = self.meta_raft.get_cluster_meta();
        if !cluster_meta.groups.contains_key(&source_group) {
            return Err(ClusterError::InvalidState("source group not found".into()).into());
        }
        if !cluster_meta.groups.contains_key(&target_group) {
            return Err(ClusterError::InvalidState("target group not found".into()).into());
        }
        if self.meta_raft.get_migration_state().is_some() {
            return Err(ClusterError::InvalidState("migration already in progress".into()).into());
        }

        // Begin via MetaRaft
        self.meta_raft
            .propose(MetaRequest::BeginSlotMigration {
                source_group,
                target_group,
                slots: slots.clone(),
            })
            .await?;

        let migration_id = self.meta_raft.get_cluster_meta().version;
        let executor = SlotMigrationExecutor::new(
            self.meta_raft.clone(),
            self.multi_raft.clone(),
            self.checkpoint_dir.clone(),
            self.config.clone(),
        );

        let _migration = ActiveMigration {
            migration_id,
            source_group,
            target_group,
            slots,
            checkpoint: Vec::new(),
        };

        // Store executor; caller invokes run_pending_migration() to execute.
        *self.executor.write() = Some(executor);

        Ok(migration_id)
    }

    /// Run the pending migration synchronously in the current task.
    ///
    /// Must be called after `start_migration()` to drive actual key migration.
    /// Without this call, the migration remains in `Prepare` state indefinitely.
    /// Caller should spawn this on a separate task if concurrent execution is desired.
    pub async fn run_pending_migration(
        &self,
        migration: ActiveMigration,
    ) -> Result<BatchMigrateResult> {
        let exec = self.executor.write().take();
        let executor = match exec {
            Some(e) => e,
            None => return Err(ClusterError::InvalidState("no active executor".into()).into()),
        };
        let result = executor.execute(migration.clone()).await?;
        // 记录这一批 (source, target, slots) 是否真正跑完了整个拷贝
        // (`is_completed=true`) —— 收尾链 / `commit_migration` 靠这份记录.
        let record = MigrationRunRecord {
            source_group: migration.source_group,
            target_group: migration.target_group,
            slots: migration.slots,
            completed: result.is_completed,
            node_id: self.node_id,
            completed_at_ms: Self::now_ms(),
            migration_id: migration.migration_id,
        };
        *self.last_run.write() = Some(record.clone());
        if result.is_completed {
            self.persist_run_record(&record)?;
        }
        Ok(result)
    }

    pub fn get_migration_status(&self) -> Result<Option<MigrationProgress>> {
        let state = self.meta_raft.get_migration_state();
        match state {
            None => Ok(None),
            Some(s) => {
                let (source_group, target_group, slots) = match &s {
                    SlotMigrationState::Prepare {
                        source_group,
                        target_group,
                        slots,
                    }
                    | SlotMigrationState::Frozen {
                        source_group,
                        target_group,
                        slots,
                    }
                    | SlotMigrationState::ReadyToCommit {
                        source_group,
                        target_group,
                        slots,
                    } => (*source_group, *target_group, slots.clone()),
                    SlotMigrationState::Migrating {
                        source_group,
                        target_group,
                        slots,
                        ..
                    } => (*source_group, *target_group, slots.clone()),
                };
                let (completed_keys, total_keys) = match &s {
                    SlotMigrationState::Migrating {
                        progress, total, ..
                    } => (*progress, *total),
                    _ => (0, 0),
                };
                let phase = match &s {
                    SlotMigrationState::Prepare { .. } => MigrationPhase::Prepare,
                    SlotMigrationState::Migrating { .. } => MigrationPhase::Migrating,
                    SlotMigrationState::Frozen { .. } => MigrationPhase::Frozen,
                    SlotMigrationState::ReadyToCommit { .. } => MigrationPhase::ReadyToCommit,
                };
                Ok(Some(MigrationProgress {
                    migration_id: self.meta_raft.get_cluster_meta().version,
                    source_group,
                    target_group,
                    slots,
                    completed_keys,
                    total_keys,
                    state: phase,
                }))
            }
        }
    }

    /// 提交迁移 — 将 slot 所有权原子切换到 target_group.
    ///
    /// 校验规则:
    /// 1. Meta 相位必须为 `ReadyToCommit` (F-056; Meta validate 是权威, 此处双保险).
    /// 2. `executor` 仍然存在时拒绝 —— target 上必然没有完整拷贝.
    /// 3. 必须存在匹配 `(source, target, slots)` 且 `completed` 的 run record.
    pub async fn commit_migration(&self) -> Result<()> {
        let (source_group, target_group, slots) = self.current_migration_signature()?;

        if !matches!(
            self.meta_raft.get_migration_state(),
            Some(SlotMigrationState::ReadyToCommit { .. })
        ) {
            return Err(ClusterError::InvalidState(
                "commit requires ReadyToCommit state; call finish_migration (or mark_ready) first"
                    .into(),
            )
            .into());
        }

        let run = self.require_completed_run_matching(source_group, target_group, &slots)?;

        self.meta_raft
            .propose(MetaRequest::CommitSlotMigration)
            .await?;
        // FIX-0056-A1: oplog GC 是清理性动作 (风险矩阵: "Commit 后异步删除;
        // 可限速"), 失败不应回滚已经生效的 Commit —— 记警告, 允许后续重试.
        if let Err(e) = self
            .cleanup_migration_oplog(target_group, run.migration_id)
            .await
        {
            tracing::warn!(
                target_group,
                epoch = run.migration_id,
                error = %e,
                "migration oplog gc failed after commit, safe to retry later (best effort)"
            );
        }
        self.clear_run_record();
        self.clear_quiesce_token();
        Ok(())
    }

    /// 取消迁移 — 先 Meta 回滚到 Assigned(source), 再清理 target 残留.
    ///
    /// F-056: Ready/Frozen 下读可能已指向 target; 必须先 Cancel 让读回 source,
    /// 再 `cleanup_target_residuals`, 避免读空洞.
    pub async fn cancel_migration(&self) -> Result<()> {
        let (_source_group, target_group, slots) = self.current_migration_signature()?;

        if let Some(ref e) = *self.executor.read() {
            e.request_cancellation();
        }

        self.meta_raft
            .propose(MetaRequest::CancelSlotMigration)
            .await?;

        for _ in 0..CANCEL_META_WAIT_ATTEMPTS {
            if self.meta_raft.get_migration_state().is_none() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(CANCEL_META_WAIT_MS)).await;
        }
        if self.meta_raft.get_migration_state().is_some() {
            return Err(ClusterError::InvalidState(
                "CancelSlotMigration proposed but migration_state still visible".into(),
            )
            .into());
        }

        self.cleanup_target_residuals(target_group, &slots).await?;
        // FIX-0056-A1: cancel 可能发生在 execute 从未跑完 (甚至从未开始)
        // 的窗口, epoch 未必已知 —— 找不到就跳过, 只记警告 (best effort,
        // 残留 tombstone 不影响正确性, 只是占一点空间直到下次 GC/复用).
        match self.current_migration_epoch(target_group, &slots) {
            Some(epoch) => {
                if let Err(e) = self.cleanup_migration_oplog(target_group, epoch).await {
                    tracing::warn!(
                        target_group,
                        epoch,
                        error = %e,
                        "migration oplog gc failed on cancel (best effort)"
                    );
                }
            }
            None => tracing::warn!(
                target_group,
                "migration oplog epoch unknown on this node, skipping oplog gc on cancel (best effort)"
            ),
        }
        self.executor.write().take();
        self.clear_run_record();
        self.clear_quiesce_token();
        Ok(())
    }

    /// FIX-0056-A1: 尽力获取当前迁移的 oplog epoch, 不要求 `run_pending_migration`
    /// 已跑完 (`cancel_migration` 可能发生在拷贝完成前). 找不到匹配
    /// `(target_group, slots)` 的本地记录时返回 `None`.
    fn current_migration_epoch(&self, target_group: u64, slots: &[u16]) -> Option<u64> {
        let run = self
            .last_run
            .read()
            .clone()
            .or_else(|| Self::load_run_record(&self.checkpoint_dir))?;
        (run.target_group == target_group && run.slots == slots).then_some(run.migration_id)
    }

    /// FIX-0056-A1: 通过 Raft apply 删除 target group 上 `epoch` 对应的全部
    /// `mig/{gid}/{epoch}/*` tombstone/tip key. 调用方按 best-effort 处理
    /// 返回的错误 (不阻塞已经生效的 Commit/Cancel).
    async fn cleanup_migration_oplog(&self, target_group: u64, epoch: u64) -> Result<()> {
        match self
            .multi_raft
            .propose_group(target_group, Request::MigrationGc { epoch })
            .await?
        {
            Response::Ok => Ok(()),
            Response::Error(e) => {
                Err(ClusterError::InvalidState(format!("migration oplog gc failed: {e}")).into())
            }
            other => Err(ClusterError::Internal(format!(
                "unexpected MigrationGc response: {other:?}"
            ))
            .into()),
        }
    }

    /// 从 MetaRaft 的权威迁移状态读取当前 `(source_group, target_group,
    /// slots)`, 而不是从本地 `executor`/`last_run` 推断 —— 这两者在
    /// `run_pending_migration` 执行后会被清空/消费, 不能用来判断"当前是否有
    /// 迁移在进行".
    fn current_migration_signature(&self) -> Result<(u64, u64, Vec<u16>)> {
        match self.meta_raft.get_migration_state() {
            None => Err(ClusterError::InvalidState("no active migration".into()).into()),
            Some(SlotMigrationState::Prepare {
                source_group,
                target_group,
                slots,
            })
            | Some(SlotMigrationState::Frozen {
                source_group,
                target_group,
                slots,
            })
            | Some(SlotMigrationState::ReadyToCommit {
                source_group,
                target_group,
                slots,
            }) => Ok((source_group, target_group, slots)),
            Some(SlotMigrationState::Migrating {
                source_group,
                target_group,
                slots,
                ..
            }) => Ok((source_group, target_group, slots)),
        }
    }

    /// 删除 target_group 上属于 `slots` 的所有残留 key.
    async fn cleanup_target_residuals(&self, target_group: u64, slots: &[u16]) -> Result<()> {
        let slot_set: HashSet<u16> = slots.iter().copied().collect();
        let keys = self.multi_raft.scan_keys(target_group, None).await?;
        for key in keys {
            if slot_set.contains(&crate::cluster::router::key_to_slot(&key)) {
                self.multi_raft
                    .propose_group(target_group, Request::Delete { key })
                    .await?;
            }
        }
        Ok(())
    }
}
