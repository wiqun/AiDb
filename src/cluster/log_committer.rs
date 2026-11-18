//! LogCommitter — 异步批量 I/O actor, 聚合 Raft log 写入并保序 flush.
//!
//! # 架构
//!
//! ```text
//! OpenRaftStorage::append(entries, callback)
//!   ├─ PendingLogOverlay.insert (立即可读)
//!   └─ mpsc → IoCommand::Append { first_index, count, done: tx }
//!               │
//!         ┌─────┴──────┐
//!         │ LogCommitter│  batch: max_commands / max_entries / max_bytes / delay_us
//!         │  (actor)    │
//!         └─────┬──────┘
//!               ├─ spawn_blocking: DB write (WriteBatch)
//!               └─ db.write 返回后通知 oneshot 或 IOFlushed
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use openraft::vote::leader_id_std::{CommittedLeaderId, LeaderId};
use openraft::{LogId, Vote};
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};

use crate::cluster::pending_log::PendingLogOverlay;
use crate::cluster::types::TypeConfig;
use crate::engine::db::WriteBatch;
use crate::error::{ClusterError, Result as CrateResult};
use crate::DB;

// ── Type aliases ──

/// 安装快照时换掉里面的 `Arc<DB>`. 日志提交和状态机都读这个槽, 避免关掉对方正在用的库.
pub(crate) type DbSlot = Arc<parking_lot::RwLock<Arc<DB>>>;

pub(crate) type EOf = <TypeConfig as openraft::RaftTypeConfig>::Entry;
pub(crate) type CLId = CommittedLeaderId<u64>;
pub(crate) type LIdOf = LogId<CLId>;
pub(crate) type VOf = Vote<LeaderId<u64, u64>>;

// ── Configuration ──

/// LogCommitter 配置参数.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct LogCommitterConfig {
    /// 单个 batch 最多聚合的 IoCommand 数.
    pub max_commands: usize,
    /// 单个 batch 最多聚合的 entry 总数.
    pub max_entries: usize,
    /// 单个 batch 最多聚合的序列化字节数 (粗略估计, 超过即 flush).
    pub max_bytes: usize,
    /// 最大延迟 (μs), 从收到第一个命令开始计时.
    /// 0 = 不凑批, 每个命令立即触发 spawn_blocking.
    pub delay_us: u64,
}

pub(crate) type BeforeWriteHook = Arc<dyn Fn() -> CrateResult<()> + Send + Sync>;

impl Default for LogCommitterConfig {
    fn default() -> Self {
        Self {
            max_commands: 64,
            max_entries: 512,
            max_bytes: 2_048_576, // 2MB
            delay_us: 0,
        }
    }
}

impl LogCommitterConfig {
    /// 创建默认配置, 但设置指定的 delay_us.
    pub fn with_delay(delay_us: u64) -> Self {
        Self {
            delay_us,
            ..Default::default()
        }
    }

    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.max_commands == 0 {
            return Err(ClusterError::InvalidConfig(
                "log_committer.max_commands must be > 0".into(),
            ));
        }
        if self.max_entries == 0 {
            return Err(ClusterError::InvalidConfig(
                "log_committer.max_entries must be > 0".into(),
            ));
        }
        if self.max_bytes == 0 {
            return Err(ClusterError::InvalidConfig(
                "log_committer.max_bytes must be > 0".into(),
            ));
        }
        Ok(())
    }
}

// ── Metrics ──

/// LogCommitter 运行时指标, 可通过原子读取.
#[derive(Debug, Default)]
pub struct LogCommitterMetrics {
    pub flush_count: AtomicU64,
    pub total_entries_flushed: AtomicU64,
    pub total_commands_flushed: AtomicU64,
    pub durable_index: AtomicU64,
    pub pending_entries: AtomicU64,
    pub pending_commands: AtomicU64,
}

// ── IoCommand ──

pub(crate) struct FlushCallback {
    inner: Option<openraft::storage::IOFlushed<TypeConfig>>,
}

impl FlushCallback {
    fn new(callback: openraft::storage::IOFlushed<TypeConfig>) -> Self {
        Self {
            inner: Some(callback),
        }
    }

    fn complete(mut self, result: std::io::Result<()>) {
        if let Some(callback) = self.inner.take() {
            callback.io_completed(result);
        }
    }
}

impl Drop for FlushCallback {
    fn drop(&mut self) {
        if let Some(callback) = self.inner.take() {
            callback.io_completed(Err(std::io::Error::other(
                "log flush callback dropped before db.write completed",
            )));
        }
    }
}

pub(crate) enum IoCommand {
    Append {
        first_index: u64,
        count: u64,
        estimated_bytes: usize,
        done: Option<oneshot::Sender<CrateResult<()>>>,
        on_flush: Option<FlushCallback>,
    },
    SaveVote {
        vote: VOf,
        done: oneshot::Sender<CrateResult<()>>,
    },
    TruncateAfter {
        log_id: Option<LIdOf>,
        done: oneshot::Sender<CrateResult<()>>,
    },
    Purge {
        start_index: u64,
        log_id: LIdOf,
        done: oneshot::Sender<CrateResult<()>>,
    },
    FlushAndSync {
        done: oneshot::Sender<()>,
    },
    #[allow(dead_code)]
    Shutdown,
}

// ── LogCommitterHandle ──

/// 对外暴露的 LogCommitter 操作句柄.
#[derive(Clone)]
pub struct LogCommitterHandle {
    tx: mpsc::UnboundedSender<IoCommand>,
    /// actor 任务句柄, `shutdown()` 时 await 以确保 Arc<DB> 完全释放
    /// (自愈重开前必须等它退出, 否则 WAL LOCK 仍被占用 → `Database already in use`).
    join: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    pub metrics: Arc<LogCommitterMetrics>,
    pub fatal: Arc<AtomicBool>,
    pub overlay: Arc<Mutex<PendingLogOverlay<Arc<EOf>>>>,
}

impl LogCommitterHandle {
    fn send(&self, cmd: IoCommand) -> CrateResult<()> {
        self.tx
            .send(cmd)
            .map_err(|_| ClusterError::Internal("LogCommitter channel closed".into()).into())
    }

    /// 优雅关闭: 发送 `Shutdown` 命令, 等待 actor 处理完当前 batch 后退出.
    /// 之后调用方即可安全 drop 底层 DB (文件锁随之释放).
    pub async fn shutdown(&self) {
        // 通道已断 (actor 已退出) 时 send 失败, 属正常情况.
        let _ = self.send(IoCommand::Shutdown);
        let join = self.join.lock().take();
        if let Some(join) = join {
            let _ = join.await;
        }
    }

    /// 发送 Append 命令, 等待 flush 完成.
    pub async fn append(&self, first_index: u64, count: u64) -> std::io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send(IoCommand::Append {
            first_index,
            count,
            estimated_bytes: (count as usize).saturating_mul(64),
            done: Some(tx),
            on_flush: None,
        })
        .map_err(|e| std::io::Error::other(e.to_string()))?;
        rx.await
            .map_err(|_| std::io::Error::other("committer channel closed"))?
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    /// 入队后立即返回. `callback` 在该批 `db.write` 返回后触发.
    pub fn submit_append(
        &self,
        first_index: u64,
        count: u64,
        callback: openraft::storage::IOFlushed<TypeConfig>,
    ) -> std::io::Result<()> {
        self.submit_append_with_bytes(
            first_index,
            count,
            (count as usize).saturating_mul(64),
            callback,
        )
    }

    pub fn submit_append_with_bytes(
        &self,
        first_index: u64,
        count: u64,
        estimated_bytes: usize,
        callback: openraft::storage::IOFlushed<TypeConfig>,
    ) -> std::io::Result<()> {
        self.send(IoCommand::Append {
            first_index,
            count,
            estimated_bytes,
            done: None,
            on_flush: Some(FlushCallback::new(callback)),
        })
        .map_err(|e| std::io::Error::other(e.to_string()))
    }

    /// 发送 SaveVote 命令, 等待完成.
    pub async fn save_vote(&self, vote: VOf) -> std::io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send(IoCommand::SaveVote { vote, done: tx })
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        rx.await
            .map_err(|_| std::io::Error::other("committer channel closed"))?
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    /// 发送 TruncateAfter 命令, 等待完成.
    pub async fn truncate_after(&self, log_id: Option<LIdOf>) -> std::io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send(IoCommand::TruncateAfter { log_id, done: tx })
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        rx.await
            .map_err(|_| std::io::Error::other("committer channel closed"))?
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    /// 发送 Purge 命令, 等待完成.
    /// `start_index` 是上次 purge 的下一条, 删除区间为 `[start_index, log_id.index]`.
    pub async fn purge(&self, start_index: u64, log_id: LIdOf) -> std::io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send(IoCommand::Purge {
            start_index,
            log_id,
            done: tx,
        })
        .map_err(|e| std::io::Error::other(e.to_string()))?;
        rx.await
            .map_err(|_| std::io::Error::other("committer channel closed"))?
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    /// 同步等待所有 pending 命令 flush 完毕.
    pub async fn flush(&self) -> CrateResult<()> {
        let (tx, rx) = oneshot::channel();
        self.send(IoCommand::FlushAndSync { done: tx })?;
        rx.await
            .map_err(|_| ClusterError::Internal("LogCommitter flush sync failed".into()).into())
    }
}

// ── 从 IoCommand 中提取的操作数据 (不含 sender) ──

struct AppendOp {
    first_index: u64,
    count: u64,
    done: Option<oneshot::Sender<CrateResult<()>>>,
    on_flush: Option<FlushCallback>,
}

struct TruncateOp {
    log_id: Option<LIdOf>,
    done: oneshot::Sender<CrateResult<()>>,
}

struct PurgeOp {
    start_index: u64,
    log_id: LIdOf,
    done: oneshot::Sender<CrateResult<()>>,
}

// ── Actor entry point ──

/// 启动 LogCommitter actor, 返回 handle.
pub(crate) fn spawn_committer(
    group_id: u64,
    db: DbSlot,
    cfg: LogCommitterConfig,
) -> LogCommitterHandle {
    spawn_committer_inner(group_id, db, cfg, None)
}

#[cfg(test)]
pub(crate) fn spawn_committer_for_test(
    group_id: u64,
    db: DbSlot,
    cfg: LogCommitterConfig,
    before_write: BeforeWriteHook,
) -> LogCommitterHandle {
    spawn_committer_inner(group_id, db, cfg, Some(before_write))
}

fn spawn_committer_inner(
    group_id: u64,
    db: DbSlot,
    cfg: LogCommitterConfig,
    before_write: Option<BeforeWriteHook>,
) -> LogCommitterHandle {
    let (tx, rx) = mpsc::unbounded_channel();
    let metrics = Arc::new(LogCommitterMetrics::default());
    let fatal = Arc::new(AtomicBool::new(false));
    let overlay = Arc::new(Mutex::new(PendingLogOverlay::new()));

    let handle = LogCommitterHandle {
        tx,
        join: Arc::new(Mutex::new(None)),
        metrics: metrics.clone(),
        fatal: fatal.clone(),
        overlay: overlay.clone(),
    };

    let join = tokio::spawn(run_committer(
        group_id,
        db,
        rx,
        cfg,
        metrics,
        fatal,
        overlay,
        before_write,
    ));

    handle.join.lock().replace(join);

    handle
}

#[allow(clippy::too_many_arguments)]
async fn run_committer(
    group_id: u64,
    db: DbSlot,
    mut rx: mpsc::UnboundedReceiver<IoCommand>,
    cfg: LogCommitterConfig,
    metrics: Arc<LogCommitterMetrics>,
    _fatal: Arc<AtomicBool>,
    overlay: Arc<Mutex<PendingLogOverlay<Arc<EOf>>>>,
    before_write: Option<BeforeWriteHook>,
) {
    let mut pending_commands: u64 = 0;
    let mut pending_entries: u64 = 0;

    loop {
        let batch = match build_batch(&mut rx, &cfg).await {
            Some(b) => b,
            None => {
                tracing::info!(group_id, "LogCommitter channel closed, exiting");
                return;
            }
        };

        let should_shutdown = batch.iter().any(|cmd| matches!(cmd, IoCommand::Shutdown));

        flush_batch(
            group_id,
            &db,
            batch,
            &overlay,
            &metrics,
            &mut pending_commands,
            &mut pending_entries,
            before_write.clone(),
        )
        .await;

        if should_shutdown {
            tracing::info!(group_id, "LogCommitter shutdown");
            return;
        }
    }
}

/// 构建一个 batch: 收到首个命令后, 使用非阻塞 try_recv 快速清空积压; 若 `delay_us > 0` 则在微秒窗口内继续凑批.
async fn build_batch(
    rx: &mut mpsc::UnboundedReceiver<IoCommand>,
    cfg: &LogCommitterConfig,
) -> Option<Vec<IoCommand>> {
    let first = rx.recv().await?;
    let mut batch = BatchState::new(first);
    let deadline = (cfg.delay_us > 0)
        .then(|| tokio::time::Instant::now() + std::time::Duration::from_micros(cfg.delay_us));

    while batch.can_add(cfg) {
        match rx.try_recv() {
            Ok(cmd) => batch.add(cmd),
            Err(mpsc::error::TryRecvError::Empty) => {
                let Some(dl) = deadline else {
                    break;
                };
                match tokio::time::timeout_at(dl, rx.recv()).await {
                    Ok(Some(cmd)) => batch.add(cmd),
                    _ => break,
                }
            }
            Err(mpsc::error::TryRecvError::Disconnected) => break,
        }
    }
    Some(batch.commands)
}

struct BatchState {
    commands: Vec<IoCommand>,
    entry_count: u64,
    total_bytes: usize,
}

impl BatchState {
    fn new(cmd: IoCommand) -> Self {
        let (entry_count, total_bytes) = match &cmd {
            IoCommand::Append {
                count,
                estimated_bytes,
                ..
            } => (*count, *estimated_bytes),
            _ => (0, 0),
        };
        Self {
            commands: vec![cmd],
            entry_count,
            total_bytes,
        }
    }

    fn can_add(&self, cfg: &LogCommitterConfig) -> bool {
        self.commands.len() < cfg.max_commands
            && self.entry_count < cfg.max_entries as u64
            && self.total_bytes < cfg.max_bytes
    }

    fn add(&mut self, cmd: IoCommand) {
        if let IoCommand::Append {
            count,
            estimated_bytes,
            ..
        } = &cmd
        {
            self.entry_count += count;
            self.total_bytes = self.total_bytes.saturating_add(*estimated_bytes);
        }
        self.commands.push(cmd);
    }
}

// ── flush_batch 核心逻辑 ──

#[allow(clippy::too_many_arguments)]
async fn flush_batch(
    group_id: u64,
    db: &DbSlot,
    batch: Vec<IoCommand>,
    overlay: &Arc<Mutex<PendingLogOverlay<Arc<EOf>>>>,
    metrics: &Arc<LogCommitterMetrics>,
    pending_commands: &mut u64,
    pending_entries: &mut u64,
    before_write: Option<BeforeWriteHook>,
) {
    let t0 = Instant::now();

    // 分离命令: 提取操作数据和 sender
    let mut appends: Vec<AppendOp> = Vec::new();
    let mut vote_to_save: Option<VOf> = None;
    let mut vote_done: Option<oneshot::Sender<CrateResult<()>>> = None;
    let mut truncates: Vec<TruncateOp> = Vec::new();
    let mut purges: Vec<PurgeOp> = Vec::new();
    let mut sync_dones: Vec<oneshot::Sender<()>> = Vec::new();

    for cmd in batch {
        match cmd {
            IoCommand::Append {
                first_index,
                count,
                done,
                on_flush,
                ..
            } => {
                appends.push(AppendOp {
                    first_index,
                    count,
                    done,
                    on_flush,
                });
                *pending_commands = pending_commands.saturating_sub(1);
                *pending_entries = pending_entries.saturating_sub(count);
            }
            IoCommand::SaveVote { vote, done } => {
                vote_to_save = Some(vote);
                vote_done = Some(done);
            }
            IoCommand::TruncateAfter { log_id, done } => {
                truncates.push(TruncateOp { log_id, done });
            }
            IoCommand::Purge {
                start_index,
                log_id,
                done,
            } => {
                purges.push(PurgeOp {
                    start_index,
                    log_id,
                    done,
                });
            }
            IoCommand::FlushAndSync { done } => {
                sync_dones.push(done);
            }
            IoCommand::Shutdown => {}
        }
    }

    metrics
        .pending_commands
        .store(*pending_commands, Ordering::Relaxed);
    metrics
        .pending_entries
        .store(*pending_entries, Ordering::Relaxed);

    // 提取 append entry 数据 (从 overlay 取副本, 落盘成功后才真正移除)
    // 递增 generation 以防止过时 flush 误删新 entry
    //
    // 不能在提取时就移除: 从提取到 `db.write` 返回之间, entry 既不在 overlay
    // 也还没进 DB, 而 `append` 早已把 `last_log_id` 推进到本批末尾. 这段窗口里
    // `get_log_entries` 读到的是"位点存在但数据不存在"的洞, openraft 会据此
    // apply 一个残缺区间, 触发
    // `core/sm/worker.rs: assert_eq!(end - 1, got_last_index)`.
    let flushed_generation: u64;
    let mut flushed_indices: Vec<u64> = Vec::new();
    let batch_entries: Vec<(u64, Vec<Arc<EOf>>)> = {
        let mut ov = overlay.lock();
        flushed_generation = ov.current_generation();
        ov.next_generation(); // 新 insert 进入新 generation
        let mut result = Vec::with_capacity(appends.len());
        for op in &appends {
            let end = op.first_index + op.count;
            let entries: Vec<Arc<EOf>> = ov
                .get_range(op.first_index, end)
                .into_iter()
                .cloned()
                .collect();
            flushed_indices.extend(entries.iter().map(|e| e.log_id.index));
            if !entries.is_empty() {
                result.push((op.first_index, entries));
            }
        }
        result
    };

    let total_entries: u64 = batch_entries.iter().map(|(_, e)| e.len() as u64).sum();
    let last_log_id = batch_entries
        .last()
        .and_then(|(_, entries)| entries.last())
        .map(|entry| entry.log_id);
    let total_cmds_val =
        (appends.len() + truncates.len() + purges.len() + usize::from(vote_to_save.is_some()))
            as u64;

    // 读锁在阻塞线程里盖住这次写, 不能跨 await (parking_lot 的守卫不是 Send).
    // 安装快照拿同一把写锁, 所以关库会等这次写结束.
    let db_slot = Arc::clone(db);
    let gid = group_id;
    let truncate_data: Vec<(Option<LIdOf>,)> = truncates.iter().map(|t| (t.log_id,)).collect();
    let purge_data: Vec<(u64, LIdOf)> = purges.iter().map(|p| (p.start_index, p.log_id)).collect();
    let vote = vote_to_save;

    let result = tokio::task::spawn_blocking(move || {
        let db_guard = db_slot.read();
        sync_flush(
            &db_guard,
            gid,
            &batch_entries,
            vote,
            &truncate_data,
            &purge_data,
            before_write,
        )
    })
    .await;

    match result {
        Ok(Ok(())) => {
            metrics.flush_count.fetch_add(1, Ordering::Relaxed);
            metrics
                .total_entries_flushed
                .fetch_add(total_entries, Ordering::Relaxed);
            metrics
                .total_commands_flushed
                .fetch_add(total_cmds_val, Ordering::Relaxed);

            if let Some(last) = last_log_id {
                metrics.durable_index.store(last.index, Ordering::Relaxed);
            }

            // 从 overlay 移除已 flush 的 entry (仅移除本轮写成功且 generation 匹配的)
            {
                let mut ov = overlay.lock();
                ov.mark_durable(&flushed_indices, flushed_generation);
                for op in &truncates {
                    if let Some(lid) = op.log_id {
                        ov.truncate_after(lid.index);
                    } else {
                        ov.clear();
                    }
                }
                for op in &purges {
                    ov.purge_upto(op.log_id.index);
                }
            }

            // 通知所有调用方: success. 回调与 oneshot 同在 db.write 返回之后.
            notify_appends(appends, Ok(()));
            if let Some(done) = vote_done {
                let _ = done.send(Ok(()));
            }
            for op in truncates {
                let _ = op.done.send(Ok(()));
            }
            for op in purges {
                let _ = op.done.send(Ok(()));
            }
            for d in sync_dones {
                let _ = d.send(());
            }

            tracing::trace!(
                group_id = gid,
                entries = total_entries,
                cmds = total_cmds_val,
                elapsed_us = t0.elapsed().as_micros(),
                "LogCommitter flush complete",
            );
        }
        Ok(Err(e)) => {
            let err_msg = e.to_string();
            send_all_errors(appends, truncates, purges, vote_done, &err_msg);
        }
        Err(join_err) => {
            tracing::error!(group_id = gid, error = %join_err, "LogCommitter spawn_blocking panicked");
            let err_msg = format!("spawn_blocking panicked: {}", join_err);
            send_all_errors(appends, truncates, purges, vote_done, &err_msg);
        }
    }
}

/// 向所有等待中的操作发送错误信息.
fn send_all_errors(
    appends: Vec<AppendOp>,
    truncates: Vec<TruncateOp>,
    purges: Vec<PurgeOp>,
    vote_done: Option<oneshot::Sender<CrateResult<()>>>,
    err_msg: &str,
) {
    notify_appends(appends, Err(std::io::Error::other(err_msg)));
    if let Some(done) = vote_done {
        let _ = done.send(Err(crate::error::Error::Cluster(ClusterError::Internal(
            err_msg.to_string(),
        ))));
    }
    for op in truncates {
        let _ = op
            .done
            .send(Err(crate::error::Error::Cluster(ClusterError::Internal(
                err_msg.to_string(),
            ))));
    }
    for op in purges {
        let _ = op
            .done
            .send(Err(crate::error::Error::Cluster(ClusterError::Internal(
                err_msg.to_string(),
            ))));
    }
}

fn notify_appends(appends: Vec<AppendOp>, result: std::io::Result<()>) {
    for op in appends {
        let io_result = match &result {
            Ok(()) => Ok(()),
            Err(err) => Err(std::io::Error::other(err.to_string())),
        };
        if let Some(done) = op.done {
            let crate_result = match &result {
                Ok(()) => Ok(()),
                Err(err) => Err(crate::error::Error::Cluster(ClusterError::Internal(
                    err.to_string(),
                ))),
            };
            let _ = done.send(crate_result);
        }
        if let Some(callback) = op.on_flush {
            callback.complete(io_result);
        }
    }
}

/// 把 `[start_index, log_id.index]` 收成一条范围删除, 并在同一批写下 `last_purged`.
fn queue_log_purge(
    batch: &mut WriteBatch,
    group_id: u64,
    purge_data: &[(u64, LIdOf)],
) -> CrateResult<()> {
    use crate::cluster::storage::keys::{last_purged_log_id_key, log_key};

    let mut purged_to: Option<&LIdOf> = None;
    for (start_index, lid) in purge_data {
        if lid.index < *start_index {
            continue;
        }
        batch.delete_range(
            log_key(group_id, *start_index),
            log_key(group_id, lid.index.saturating_add(1)),
        );
        purged_to = Some(match purged_to {
            Some(prev) if prev.index >= lid.index => prev,
            _ => lid,
        });
    }
    if let Some(lid) = purged_to {
        let data =
            rmp_serde::to_vec(lid).map_err(|e| ClusterError::Serialization(e.to_string()))?;
        batch.put(last_purged_log_id_key(group_id), data);
    }
    Ok(())
}

fn sync_flush(
    db: &Arc<DB>,
    group_id: u64,
    append_batches: &[(u64, Vec<Arc<EOf>>)],
    vote: Option<VOf>,
    truncate_data: &[(Option<LIdOf>,)],
    purge_data: &[(u64, LIdOf)],
    before_write: Option<BeforeWriteHook>,
) -> CrateResult<()> {
    use crate::cluster::storage::keys;

    if let Some(hook) = before_write {
        hook()?;
    }

    // 1. 先执行批量写入 (PUT ops in WriteBatch)
    {
        let mut batch = WriteBatch::new();

        for (_, entries) in append_batches {
            for entry in entries {
                let key = keys::log_key(group_id, entry.log_id.index);
                let data = rmp_serde::to_vec(entry.as_ref())
                    .map_err(|e| ClusterError::Serialization(e.to_string()))?;
                batch.put(key, data);
            }
            if let Some(last) = entries.last() {
                let data = rmp_serde::to_vec(&last.log_id)
                    .map_err(|e| ClusterError::Serialization(e.to_string()))?;
                batch.put(keys::last_log_id_key(group_id), data);
            }
        }

        if let Some(v) = vote {
            let data =
                rmp_serde::to_vec(&v).map_err(|e| ClusterError::Serialization(e.to_string()))?;
            batch.put(keys::vote_key(group_id), data);
        }

        queue_log_purge(&mut batch, group_id, purge_data)?;

        db.write(&batch)?;
    }

    // 2. Truncate — 使用 DB 级别的 delete_range
    //
    // `truncate_after(last_log_id)` 保留 `last_log_id` 本身, 从它的下一条开始删
    // (openraft 契约: "Truncate logs after `last_log_id`, exclusive"). 从
    // `lid.index` 开始删会把这条也删掉, 在日志中间留洞 —— openraft 仍认为该位点
    // 存在并据此 apply, 实际取到的 entry 少于请求区间.
    for (lid,) in truncate_data {
        if let Some(lid) = lid {
            let start = keys::log_key(group_id, lid.index.saturating_add(1));
            let end = keys::log_range_end(group_id);
            db.delete_range(&start, &end)?;

            // 保留的最后一条就是 `lid.index`, 直接落盘该 log_id. 不回读 DB:
            // 这条 entry 可能仍在 overlay 未落盘, 回读落空会留下过期的 last_log_id.
            let lid_data =
                rmp_serde::to_vec(&lid).map_err(|e| ClusterError::Serialization(e.to_string()))?;
            let mut b = WriteBatch::new();
            b.put(keys::last_log_id_key(group_id), lid_data);
            db.write(&b)?;
        } else {
            let start = keys::log_prefix(group_id);
            let end = keys::log_range_end(group_id);
            db.delete_range(&start, &end)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::storage::keys::{
        last_log_id_key, last_purged_log_id_key, log_key, vote_key,
    };
    use crate::cluster::types::Request;
    use crate::config::Options;
    use openraft::vote::leader_id_std::CommittedLeaderId;
    use openraft::vote::leader_id_std::LeaderId;
    use openraft::{EntryPayload, LogId};
    use tempfile::TempDir;

    fn setup_db() -> (TempDir, Arc<DB>) {
        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        (dir, db)
    }

    fn mk_entry(index: u64, term: u64) -> EOf {
        EOf {
            log_id: LogId::new(CommittedLeaderId::new(term), index),
            payload: EntryPayload::Normal(Request::Put {
                key: format!("k{index}").into_bytes(),
                value: bytes::Bytes::from(format!("v{index}").into_bytes()),
            }),
        }
    }

    #[tokio::test]
    async fn test_append_and_flush() {
        let (_dir, db) = setup_db();
        let handle = spawn_committer(
            1,
            Arc::new(parking_lot::RwLock::new(db.clone())),
            LogCommitterConfig::default(),
        );

        {
            let mut ov = handle.overlay.lock();
            ov.insert_at(1, Arc::new(mk_entry(1, 1)));
            ov.insert_at(2, Arc::new(mk_entry(2, 1)));
        }

        handle.append(1, 2).await.unwrap();

        let data1 = db.get(&log_key(1, 1)).unwrap().unwrap();
        let entry1: EOf = rmp_serde::from_slice(&data1).unwrap();
        assert_eq!(entry1.log_id.index, 1);

        let ov = handle.overlay.lock();
        assert!(ov.is_empty());
    }

    #[tokio::test]
    async fn test_save_vote() {
        let (_dir, db) = setup_db();
        let handle = spawn_committer(
            1,
            Arc::new(parking_lot::RwLock::new(db.clone())),
            LogCommitterConfig::default(),
        );

        let vote = VOf {
            leader_id: LeaderId {
                term: 1,
                voted_for: 1,
            },
            committed: true,
        };
        handle.save_vote(vote).await.unwrap();

        let data = db.get(&vote_key(1)).unwrap().unwrap();
        let loaded: VOf = rmp_serde::from_slice(&data).unwrap();
        assert_eq!(loaded.leader_id.term, 1);
    }

    /// `truncate_after(last_log_id)` 语义是"保留 `last_log_id`, 删除它之后的"
    /// (openraft `RaftLogStorage::truncate_after`: "Truncate logs after `last_log_id`,
    /// exclusive"). 把 `last_log_id` 自己也删掉会在日志中间留一个洞: openraft 仍认为
    /// 该位点存在并据此 apply, 取回的 entry 数少于请求区间, 触发它的不变式断言
    /// (`core/sm/worker.rs: assert_eq!(end - 1, got_last_index)`), 或在不做断言的
    /// release 构建里把未 apply 的 entry 标记成已 apply.
    #[tokio::test]
    async fn test_truncate_after() {
        let (_dir, db) = setup_db();
        let handle = spawn_committer(
            1,
            Arc::new(parking_lot::RwLock::new(db.clone())),
            LogCommitterConfig::default(),
        );

        {
            let mut ov = handle.overlay.lock();
            ov.insert_at(1, Arc::new(mk_entry(1, 1)));
            ov.insert_at(2, Arc::new(mk_entry(2, 1)));
            ov.insert_at(3, Arc::new(mk_entry(3, 1)));
        }
        handle.append(1, 3).await.unwrap();

        handle
            .truncate_after(Some(LogId::new(CommittedLeaderId::new(1), 2)))
            .await
            .unwrap();

        assert!(db.get(&log_key(1, 1)).unwrap().is_some(), "entry 1 exists");
        assert!(
            db.get(&log_key(1, 2)).unwrap().is_some(),
            "entry 2 是 last_log_id, 必须保留"
        );
        assert!(db.get(&log_key(1, 3)).unwrap().is_none(), "entry 3 deleted");
        let stored: LIdOf = rmp_serde::from_slice(
            &db.get(&last_log_id_key(1))
                .unwrap()
                .expect("last_log_id must be persisted"),
        )
        .unwrap();
        assert_eq!(stored.index, 2, "last_log_id 应回落到保留的最后一条");
    }

    #[tokio::test]
    async fn test_purge_upto() {
        let (_dir, db) = setup_db();
        let handle = spawn_committer(
            1,
            Arc::new(parking_lot::RwLock::new(db.clone())),
            LogCommitterConfig::default(),
        );

        {
            let mut ov = handle.overlay.lock();
            ov.insert_at(1, Arc::new(mk_entry(1, 1)));
            ov.insert_at(2, Arc::new(mk_entry(2, 1)));
            ov.insert_at(3, Arc::new(mk_entry(3, 1)));
        }
        handle.append(1, 3).await.unwrap();

        handle
            .purge(0, LogId::new(CommittedLeaderId::new(1), 2))
            .await
            .unwrap();

        assert!(db.get(&log_key(1, 1)).unwrap().is_none(), "entry 1 purged");
        assert!(db.get(&log_key(1, 2)).unwrap().is_none(), "entry 2 purged");
        assert!(db.get(&log_key(1, 3)).unwrap().is_some(), "entry 3 exists");
        let stored: LIdOf = rmp_serde::from_slice(
            &db.get(&last_purged_log_id_key(1))
                .unwrap()
                .expect("last_purged"),
        )
        .unwrap();
        assert_eq!(stored.index, 2);

        handle
            .purge(3, LogId::new(CommittedLeaderId::new(1), 3))
            .await
            .unwrap();
        assert!(
            db.get(&log_key(1, 1)).unwrap().is_none(),
            "entry 1 stays purged"
        );
        assert!(db.get(&log_key(1, 3)).unwrap().is_none(), "entry 3 purged");
    }

    #[test]
    fn purge_range_drops_inclusive_index_and_keeps_newer_append() {
        let (_dir, db) = setup_db();
        let purge_to = LogId::new(CommittedLeaderId::new(1), 2);
        sync_flush(
            &db,
            1,
            &[(
                1,
                vec![
                    Arc::new(mk_entry(1, 1)),
                    Arc::new(mk_entry(2, 1)),
                    Arc::new(mk_entry(3, 1)),
                ],
            )],
            None,
            &[],
            &[(0, purge_to)],
            None,
        )
        .unwrap();
        assert!(db.get(&log_key(1, 1)).unwrap().is_none());
        assert!(db.get(&log_key(1, 2)).unwrap().is_none());
        assert!(db.get(&log_key(1, 3)).unwrap().is_some());

        sync_flush(
            &db,
            1,
            &[(4, vec![Arc::new(mk_entry(4, 1))])],
            None,
            &[],
            &[(3, LogId::new(CommittedLeaderId::new(1), 3))],
            None,
        )
        .unwrap();
        assert!(db.get(&log_key(1, 1)).unwrap().is_none());
        assert!(db.get(&log_key(1, 3)).unwrap().is_none());
        assert!(db.get(&log_key(1, 4)).unwrap().is_some());
    }

    fn signal_callback() -> (
        openraft::storage::IOFlushed<TypeConfig>,
        tokio::sync::oneshot::Receiver<std::io::Result<()>>,
    ) {
        use openraft::type_config::TypeConfigExt;
        let (tx, rx) = TypeConfig::oneshot();
        (openraft::storage::IOFlushed::signal(tx), rx)
    }

    #[tokio::test]
    async fn dropped_flush_callback_is_error() {
        let (cb, rx) = signal_callback();
        drop(FlushCallback::new(cb));
        let result = rx.await.unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn submit_append_returns_before_db_write() {
        let (_dir, db) = setup_db();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<CrateResult<()>>();
        let release_rx = Mutex::new(release_rx);
        let first_flush = AtomicBool::new(true);
        let hook: BeforeWriteHook = Arc::new(move || {
            if !first_flush.swap(false, Ordering::SeqCst) {
                return Ok(());
            }
            entered_tx
                .send(())
                .map_err(|_| ClusterError::Internal("flush gate closed".into()))?;
            release_rx
                .lock()
                .recv()
                .map_err(|_| ClusterError::Internal("flush gate dropped".into()))?
        });
        let handle = spawn_committer_for_test(
            1,
            Arc::new(parking_lot::RwLock::new(db.clone())),
            LogCommitterConfig::default(),
            hook,
        );
        {
            let mut ov = handle.overlay.lock();
            ov.insert_at(1, Arc::new(mk_entry(1, 1)));
            ov.insert_at(2, Arc::new(mk_entry(2, 1)));
        }
        let (cb1, rx1) = signal_callback();
        let (cb2, mut rx2) = signal_callback();
        handle.submit_append(1, 1, cb1).unwrap();
        tokio::task::spawn_blocking(move || {
            entered_rx.recv_timeout(std::time::Duration::from_secs(2))
        })
        .await
        .unwrap()
        .expect("flush reached db.write");
        handle.submit_append(2, 1, cb2).unwrap();
        assert!(rx2.try_recv().is_err());
        assert!(db.get(&log_key(1, 1)).unwrap().is_none());
        release_tx.send(Ok(())).unwrap();
        assert!(rx1.await.unwrap().is_ok());
        assert!(rx2.await.unwrap().is_ok());
        assert!(db.get(&log_key(1, 1)).unwrap().is_some());
        assert!(db.get(&log_key(1, 2)).unwrap().is_some());
    }

    /// 回归点: 提取批次时曾用 `drain_range` 立刻移除 overlay 条目. 于是
    /// "已取出但还没落盘"的窗口里, `get_log_entries` 两边都读不到, 而
    /// `append` 早已把 `last_log_id` 推到本批末尾 —— openraft 会据此 apply 一个
    /// 残缺区间, 触发 `core/sm/worker.rs: assert_eq!(end - 1, got_last_index)`
    /// (debug 构建 panic; release 构建把未落盘的 entry 当成已 apply).
    ///
    /// 不受时序影响的检查方式: 落盘失败时条目必然没持久化, 它必须仍留在 overlay.
    #[tokio::test]
    async fn failed_flush_keeps_entries_in_overlay() {
        let (_dir, db) = setup_db();
        let hook: BeforeWriteHook =
            Arc::new(|| Err(ClusterError::Internal("injected write failure".into()).into()));
        let handle = spawn_committer_for_test(
            1,
            Arc::new(parking_lot::RwLock::new(db.clone())),
            LogCommitterConfig::default(),
            hook,
        );
        {
            let mut ov = handle.overlay.lock();
            ov.insert_at(1, Arc::new(mk_entry(1, 1)));
            ov.insert_at(2, Arc::new(mk_entry(2, 1)));
        }
        let (cb, rx) = signal_callback();
        handle.submit_append(1, 2, cb).unwrap();
        assert!(rx.await.unwrap().is_err());

        assert!(db.get(&log_key(1, 1)).unwrap().is_none(), "落盘失败");
        assert!(
            handle.overlay.lock().get(1).is_some(),
            "未落盘的 entry 不能被提前移出 overlay"
        );
        assert!(
            handle.overlay.lock().get(2).is_some(),
            "未落盘的 entry 不能被提前移出 overlay"
        );
    }

    #[tokio::test]
    async fn submit_append_callback_is_err_when_write_fails() {
        let (_dir, db) = setup_db();
        let hook: BeforeWriteHook =
            Arc::new(|| Err(ClusterError::Internal("injected write failure".into()).into()));
        let handle = spawn_committer_for_test(
            1,
            Arc::new(parking_lot::RwLock::new(db.clone())),
            LogCommitterConfig::default(),
            hook,
        );
        {
            let mut ov = handle.overlay.lock();
            ov.insert_at(1, Arc::new(mk_entry(1, 1)));
        }
        let (cb, rx) = signal_callback();
        handle.submit_append(1, 1, cb).unwrap();
        assert!(rx.await.unwrap().is_err());
        assert!(db.get(&log_key(1, 1)).unwrap().is_none());
    }

    #[test]
    fn validate_rejects_zero_limits() {
        assert!(LogCommitterConfig::default().validate().is_ok());
        assert!(LogCommitterConfig {
            max_commands: 0,
            ..Default::default()
        }
        .validate()
        .is_err());
        assert!(LogCommitterConfig {
            max_entries: 0,
            ..Default::default()
        }
        .validate()
        .is_err());
        assert!(LogCommitterConfig {
            max_bytes: 0,
            ..Default::default()
        }
        .validate()
        .is_err());
    }

    #[test]
    fn batch_respects_max_bytes() {
        let cfg = LogCommitterConfig {
            max_commands: 100,
            max_entries: 100,
            max_bytes: 256,
            delay_us: 0,
        };
        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();
        let mut batch = BatchState::new(IoCommand::Append {
            first_index: 1,
            count: 1,
            estimated_bytes: 260,
            done: Some(tx1),
            on_flush: None,
        });
        assert!(
            !batch.can_add(&cfg),
            "exceeding max_bytes must close the batch"
        );
        batch.add(IoCommand::Append {
            first_index: 2,
            count: 1,
            estimated_bytes: 10,
            done: Some(tx2),
            on_flush: None,
        });
        assert_eq!(batch.total_bytes, 270);
    }

    #[tokio::test]
    async fn build_batch_waits_when_delay_us_nonzero() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let cfg = LogCommitterConfig {
            max_commands: 8,
            max_entries: 64,
            max_bytes: 64 * 1024,
            delay_us: 20_000, // 20ms
        };
        let (d1, _) = oneshot::channel();
        let (d2, _) = oneshot::channel();
        tx.send(IoCommand::Append {
            first_index: 1,
            count: 1,
            estimated_bytes: 64,
            done: Some(d1),
            on_flush: None,
        })
        .unwrap();
        let tx2 = tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            let _ = tx2.send(IoCommand::Append {
                first_index: 2,
                count: 1,
                estimated_bytes: 64,
                done: Some(d2),
                on_flush: None,
            });
        });
        let batch = build_batch(&mut rx, &cfg).await.unwrap();
        assert_eq!(
            batch.len(),
            2,
            "delay_us window should coalesce delayed arrival"
        );
    }
}
