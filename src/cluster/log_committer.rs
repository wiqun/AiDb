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
//!               └─ 通过 oneshot 通知调用方
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

pub(crate) type EOf = <TypeConfig as openraft::RaftTypeConfig>::Entry;
pub(crate) type CLId = CommittedLeaderId<u64>;
pub(crate) type LIdOf = LogId<CLId>;
pub(crate) type VOf = Vote<LeaderId<u64, u64>>;

// ── Configuration ──

/// LogCommitter 配置参数.
#[derive(Debug, Clone)]
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

pub(crate) enum IoCommand {
    Append {
        first_index: u64,
        count: u64,
        done: oneshot::Sender<CrateResult<()>>,
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
    pub overlay: Arc<Mutex<PendingLogOverlay<EOf>>>,
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
            done: tx,
        })
        .map_err(|e| std::io::Error::other(e.to_string()))?;
        rx.await
            .map_err(|_| std::io::Error::other("committer channel closed"))?
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
    pub async fn purge(&self, log_id: LIdOf) -> std::io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send(IoCommand::Purge { log_id, done: tx })
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
    done: oneshot::Sender<CrateResult<()>>,
}

struct TruncateOp {
    log_id: Option<LIdOf>,
    done: oneshot::Sender<CrateResult<()>>,
}

struct PurgeOp {
    log_id: LIdOf,
    done: oneshot::Sender<CrateResult<()>>,
}

// ── Actor entry point ──

/// 启动 LogCommitter actor, 返回 handle.
pub(crate) fn spawn_committer(
    group_id: u64,
    db: Arc<DB>,
    cfg: LogCommitterConfig,
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
        group_id, db, rx, cfg, metrics, fatal, overlay,
    ));

    handle.join.lock().replace(join);

    handle
}

async fn run_committer(
    group_id: u64,
    db: Arc<DB>,
    mut rx: mpsc::UnboundedReceiver<IoCommand>,
    cfg: LogCommitterConfig,
    metrics: Arc<LogCommitterMetrics>,
    _fatal: Arc<AtomicBool>,
    overlay: Arc<Mutex<PendingLogOverlay<EOf>>>,
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
        )
        .await;

        if should_shutdown {
            tracing::info!(group_id, "LogCommitter shutdown");
            return;
        }
    }
}

/// 构建一个 batch: 收到首个命令后, 使用非阻塞 try_recv 快速清空通道积压命令 (自适应凑批, 0 超时硬等待).
async fn build_batch(
    rx: &mut mpsc::UnboundedReceiver<IoCommand>,
    cfg: &LogCommitterConfig,
) -> Option<Vec<IoCommand>> {
    let first = rx.recv().await?;
    let mut batch = BatchState::new(first);

    while batch.can_add(cfg) {
        match rx.try_recv() {
            Ok(cmd) => batch.add(cmd),
            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => break,
        }
    }
    Some(batch.commands)
}

struct BatchState {
    commands: Vec<IoCommand>,
    entry_count: u64,
    approx_bytes: usize,
    #[allow(dead_code)]
    first_cmd_at: Instant,
}

impl BatchState {
    fn new(cmd: IoCommand) -> Self {
        let entry_count = match &cmd {
            IoCommand::Append { count, .. } => *count,
            _ => 0,
        };
        Self {
            commands: vec![cmd],
            entry_count,
            approx_bytes: 0,
            first_cmd_at: Instant::now(),
        }
    }

    fn can_add(&self, cfg: &LogCommitterConfig) -> bool {
        self.commands.len() < cfg.max_commands
            && self.entry_count < cfg.max_entries as u64
            && self.approx_bytes < cfg.max_bytes
    }

    fn add(&mut self, cmd: IoCommand) {
        if let IoCommand::Append { count, .. } = &cmd {
            self.entry_count += count;
        }
        self.commands.push(cmd);
    }
}

// ── flush_batch 核心逻辑 ──

async fn flush_batch(
    group_id: u64,
    db: &Arc<DB>,
    batch: Vec<IoCommand>,
    overlay: &Arc<Mutex<PendingLogOverlay<EOf>>>,
    metrics: &Arc<LogCommitterMetrics>,
    pending_commands: &mut u64,
    pending_entries: &mut u64,
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
            } => {
                appends.push(AppendOp {
                    first_index,
                    count,
                    done,
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
            IoCommand::Purge { log_id, done } => {
                purges.push(PurgeOp { log_id, done });
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

    // 提取 append entry 数据 (从 overlay drain)
    // 递增 generation 以防止过时 flush 误删新 entry
    let flushed_generation: u64;
    let batch_entries: Vec<(u64, Vec<EOf>)> = {
        let mut ov = overlay.lock();
        flushed_generation = ov.current_generation();
        ov.next_generation(); // 新 insert 进入新 generation
        let mut result = Vec::with_capacity(appends.len());
        for op in &appends {
            let end = op.first_index + op.count;
            let entries: Vec<_> = ov
                .drain_range(op.first_index, end)
                .into_iter()
                .map(|(_, entry)| entry)
                .collect();
            if !entries.is_empty() {
                result.push((op.first_index, entries));
            }
        }
        result
    };
    // Overlay 清理所需的数据: 每个 append batch 的范围.
    let overlay_ranges: Vec<(u64, u64)> = batch_entries
        .iter()
        .map(|(start, entries)| (*start, entries.len() as u64))
        .collect();

    let total_entries: u64 = batch_entries.iter().map(|(_, e)| e.len() as u64).sum();
    let last_entry = batch_entries
        .last()
        .and_then(|(_, entries)| entries.last())
        .cloned();
    let total_cmds_val =
        (appends.len() + truncates.len() + purges.len() + usize::from(vote_to_save.is_some()))
            as u64;

    let db_clone = Arc::clone(db);
    let gid = group_id;
    let truncate_data: Vec<(Option<LIdOf>,)> = truncates.iter().map(|t| (t.log_id,)).collect();
    let purge_data: Vec<(LIdOf,)> = purges.iter().map(|p| (p.log_id,)).collect();
    let vote = vote_to_save;

    let result = tokio::task::spawn_blocking(move || {
        sync_flush(
            &db_clone,
            gid,
            &batch_entries,
            vote,
            &truncate_data,
            &purge_data,
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

            if let Some(ref last) = last_entry {
                metrics
                    .durable_index
                    .store(last.log_id.index, Ordering::Relaxed);
            }

            // 从 overlay 移除已 flush 的 entry
            {
                let mut ov = overlay.lock();
                for (start, len) in &overlay_ranges {
                    let end = start + len;
                    let indices: Vec<u64> = (*start..end).collect();
                    ov.mark_durable(&indices, flushed_generation);
                }
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

            // 通知所有调用方: success
            for op in appends {
                let _ = op.done.send(Ok(()));
            }
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
    for op in appends {
        let _ = op
            .done
            .send(Err(crate::error::Error::Cluster(ClusterError::Internal(
                err_msg.to_string(),
            ))));
    }
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

/// 同步执行 DB 写入 (在 spawn_blocking 中运行).
fn sync_flush(
    db: &Arc<DB>,
    group_id: u64,
    append_batches: &[(u64, Vec<EOf>)],
    vote: Option<VOf>,
    truncate_data: &[(Option<LIdOf>,)],
    purge_data: &[(LIdOf,)],
) -> CrateResult<()> {
    use crate::cluster::storage::keys;

    // 1. 先执行批量写入 (PUT ops in WriteBatch)
    {
        let mut batch = WriteBatch::new();

        for (_, entries) in append_batches {
            for entry in entries {
                let key = keys::log_key(group_id, entry.log_id.index);
                let data = rmp_serde::to_vec(entry)
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

        // Purge: 扫描 range 内每一条 key, 加入 batch delete
        for (lid,) in purge_data {
            let start = keys::log_key(group_id, 0);
            let end = keys::log_key(group_id, lid.index.saturating_add(1));
            if let Ok(iter) = db.scan(Some(&start), Some(&end)) {
                for (key, _) in iter.flatten() {
                    batch.delete(key);
                }
            }
        }

        let _ = db.write(&batch)?;
    }

    // 2. Truncate — 使用 DB 级别的 delete_range
    for (lid,) in truncate_data {
        if let Some(lid) = lid {
            let start = keys::log_key(group_id, lid.index);
            let end = keys::log_range_end(group_id);
            db.delete_range(&start, &end)?;

            // 更新 last_log_id
            if lid.index > 0 {
                let prev_key = keys::log_key(group_id, lid.index - 1);
                if let Some(prev_data) = db.get(&prev_key)? {
                    if let Ok(prev_entry) = rmp_serde::from_slice::<EOf>(&prev_data) {
                        let lid_data = rmp_serde::to_vec(&prev_entry.log_id)
                            .map_err(|e| ClusterError::Serialization(e.to_string()))?;
                        let mut b = WriteBatch::new();
                        b.put(keys::last_log_id_key(group_id), lid_data);
                        let _ = db.write(&b)?;
                    }
                }
            }
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
    use crate::cluster::storage::keys::{log_key, vote_key};
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
                value: format!("v{index}").into_bytes(),
            }),
        }
    }

    #[tokio::test]
    async fn test_append_and_flush() {
        let (_dir, db) = setup_db();
        let handle = spawn_committer(1, db.clone(), LogCommitterConfig::default());

        {
            let mut ov = handle.overlay.lock();
            ov.insert_at(1, mk_entry(1, 1));
            ov.insert_at(2, mk_entry(2, 1));
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
        let handle = spawn_committer(1, db.clone(), LogCommitterConfig::default());

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

    #[tokio::test]
    async fn test_truncate_after() {
        let (_dir, db) = setup_db();
        let handle = spawn_committer(1, db.clone(), LogCommitterConfig::default());

        {
            let mut ov = handle.overlay.lock();
            ov.insert_at(1, mk_entry(1, 1));
            ov.insert_at(2, mk_entry(2, 1));
        }
        handle.append(1, 2).await.unwrap();

        handle
            .truncate_after(Some(LogId::new(CommittedLeaderId::new(1), 2)))
            .await
            .unwrap();

        assert!(db.get(&log_key(1, 1)).unwrap().is_some(), "entry 1 exists");
        assert!(db.get(&log_key(1, 2)).unwrap().is_none(), "entry 2 deleted");
    }

    #[tokio::test]
    async fn test_purge_upto() {
        let (_dir, db) = setup_db();
        let handle = spawn_committer(1, db.clone(), LogCommitterConfig::default());

        {
            let mut ov = handle.overlay.lock();
            ov.insert_at(1, mk_entry(1, 1));
            ov.insert_at(2, mk_entry(2, 1));
            ov.insert_at(3, mk_entry(3, 1));
        }
        handle.append(1, 3).await.unwrap();

        handle
            .purge(LogId::new(CommittedLeaderId::new(1), 2))
            .await
            .unwrap();

        assert!(db.get(&log_key(1, 3)).unwrap().is_some(), "entry 3 exists");
    }
}
