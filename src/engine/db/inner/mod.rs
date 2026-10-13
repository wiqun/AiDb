//! LSM DB 引擎总协调器: 聚合 WAL / MemTable / SSTable / VersionSet / SnapshotList, 对外提供
//! `open` / `put` / `get` / `key_exists` / `delete` / `write` / `snapshot` / `iter` / `scan` /
//! `flush` / `close`, 并驱动后台 flush 与 compaction 线程.
//!
//! 本文件为拆分后的模块根, 子模块按职责划分:
//! - `write`: 单 key 写与 WriteBatch 写入, WAL 落盘与 MemTable 更新
//! - `read`: get/scan/snapshot 读路径聚合
//! - `exists`: key_exists 完整存在性判定 (不物化 Value)
//! - `flush`: 冻结 MemTable 与后台 Flush 调度
//! - `compaction`: 文件 claim、subcompaction 与后台压缩循环
//!
//! # 架构
//!
//! ```text
//! 写路径:
//!   check_write_stall → write_lock → alloc_sequence → WAL append
//!   → (sync_wal: wait_group_commit_sync) → MemTable put/delete
//!   → maybe_freeze (超过 memtable_size) → ImmutableMemTable
//!   → 后台 flush 线程 (flush_pending) → L0 SSTable
//! 读路径 (get_at_sequence / key_exists_at_sequence):
//!   active MemTable → immutable MemTable (新→旧) → L0 SSTable (新→旧全扫)
//!   → L1+ (find_sstable_for_key 按 user_key range 定位)
//! 后台线程: flush 线程 (poll flush_poll_ms); compaction 线程 (compaction_signals 触发
//!   run_compaction_once: pick → claim → trivial move / subcompaction → apply)
//! ```
//!
//! freeze: `maybe_freeze` / `freeze_active_if_nonempty` 消费 active MemTable (`std::mem::take`),
//! 生成 ImmutableMemTable, `flush_seq` 记为冻结时刻的 sequence; immutable 由
//! `flush_immutable_memtables` 依序写出 L0 SST 并 `VersionEdit::AddFile`, 随后 rotate + cleanup WAL.
//!
//! # Invariant
//!
//! - 写顺序: WAL append 先于 MemTable 写入 (`put` / `delete` / `write` / `delete_range` 均如此),
//!   crash 时已落 WAL 未进 MemTable 的数据可在 recover 阶段重放.
//! - Sequence: 合法范围 `[1, 2^56)`; `alloc_sequence` 分配后检查溢出, 越界报 `Error::InvalidState`.
//! - Batch 崩溃原子性: recover 时 `BatchStart` 标记的不完整 batch 整批丢弃 (单条写无 BatchStart).
//! - Snapshot 创建: 在 `write_lock` 下读 sequence 并注册 `SnapshotList`, register 必须先于释放锁;
//!   compaction 读 `min_snapshot_sequence()` 时同样短暂持有 `write_lock`, 二者形成 happens-before,
//!   避免读到 "seq 已确定但尚未 register" 的中间态.
//! - `iter` / `scan` 使用 `K_MAX_SEQUENCE` 见全部已写版本; `get` 使用 `sequence.load()` —
//!   行为 intentionally 不同 (详见 `docs/modules/01-engine.md`).

mod compaction;
mod exists;
mod flush;
mod read;
mod write;

pub(super) use super::iterator::DbIterGuard;
pub(super) use super::numbers::scan_next_wal_file_number;
pub(super) use super::replay::replay_entries;
pub(super) use super::snapshot::{Snapshot, SnapshotList};
pub(super) use super::write_batch::{WriteBatch, WriteOp};
pub(super) use crate::config::Options;
pub(super) use crate::engine::cache::{BlockCache, CacheStats};
pub(super) use crate::engine::compaction::{
    current_exists, load_sstables_from_version, remove_orphan_sstables,
    scan_version_edits_from_dir, CompactionFilter, CompactionJob, CompactionPicker,
    CompactionRemovalListener, CompactionTask, VersionEdit, VersionSet,
};
pub(super) use crate::engine::memtable::{
    encode_internal_key_buffered, extract_sequence, ImmutableMemTable, MemTable, PointState,
    ValueType, SEQUENCE_LIMIT,
};
pub(super) use crate::engine::sstable::{sstable_path, SSTableBuilder, SSTableReader};
pub(super) use crate::engine::wal::manager::WALManager;
pub(super) use crate::engine::wal::record::{OpType, WalEntry};
pub(super) use crate::error::{Error, Result};
pub(super) use crate::statistics::Statistics;
pub(super) use crossbeam_channel::{Receiver, Sender};
pub(super) use parking_lot::{Mutex, RwLock};
pub(super) use std::collections::HashSet;
pub(super) use std::path::{Path, PathBuf};
pub(super) use std::sync::atomic::{
    AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering,
};
pub(super) use std::sync::{Arc, Weak};
pub(super) use std::thread::JoinHandle;
pub(super) use std::time::Duration;

#[expect(dead_code)]
pub const DEFAULT_MAX_LEVELS: usize = 7;
/// LSM-Tree 存储引擎.
pub struct DB {
    path: PathBuf,
    options: Arc<Options>,
    memtable: RwLock<MemTable>,
    immutable_memtables: RwLock<Vec<ImmutableMemTable>>,
    wal: RwLock<WALManager>,
    sstables: RwLock<Vec<Vec<Arc<SSTableReader>>>>,
    version_set: RwLock<VersionSet>,
    compaction_picker: Arc<CompactionPicker>,
    sequence: AtomicU64,
    /// 已提交到 memtable 的最大 sequence (区别于 `sequence`: 后者是已分配的
    /// 最大 sequence, 分配与插入之间存在"已分配但未插入"的窗口). 所有写路径
    /// 在写锁内完成插入后才推进此值; snapshot / iter / scan 边界必须取此值
    /// (见 A-005 竞态).
    committed_sequence: AtomicU64,
    write_lock: Mutex<()>,
    flush_lock: Mutex<()>,
    flush_shutdown: Arc<AtomicBool>,
    flush_handle: Mutex<Option<JoinHandle<()>>>,
    compaction_shutdown: Arc<AtomicBool>,
    compaction_signals: Vec<Sender<()>>,
    compaction_handles: Mutex<Vec<JoinHandle<()>>>,
    compacting: Mutex<HashSet<u64>>,
    closed: AtomicBool,
    /// Compaction 过滤器: 在 entry 写入输出 SST 前判断是否保留.
    compaction_filter: RwLock<Option<Arc<dyn CompactionFilter>>>,
    /// 最新 Put 被 filter Remove 时的监听器 (filter 保持无副作用).
    compaction_removal_listener: RwLock<Option<Arc<dyn CompactionRemovalListener>>>,
    checkpoint_in_progress: AtomicBool,
    block_cache: Arc<BlockCache>,
    pub(crate) snapshots: Arc<SnapshotList>,
    /// Group commit: leader election mutex
    group_commit_lock: Mutex<()>,
    /// Group commit: last synced sequence number (monotonic)
    group_commit_synced_seq: AtomicU64,
    /// L0 SSTable count for lock-free stall checks (F-050/051/054).
    /// Maintained atomically inside all `sstables.write()` critical sections.
    l0_sstable_count: AtomicUsize,
    pub(crate) stats: Arc<Statistics>,
}

impl DB {
    /// 访问当前 DB 实例绑定的无锁原子指标集合
    pub fn statistics(&self) -> Arc<Statistics> {
        Arc::clone(&self.stats)
    }

    pub fn open(path: impl AsRef<Path>, options: Options) -> Result<Arc<Self>> {
        options.validate()?;

        // 校验外部注入的 statistics 层数一致性契约, 避免后续越界访问
        if let Some(ref stats) = options.statistics {
            if stats.sstable_count.len() != options.max_levels {
                return Err(Error::InvalidArgument(format!(
                    "statistics.sstable_count.len() ({}) must equal options.max_levels ({})",
                    stats.sstable_count.len(),
                    options.max_levels
                )));
            }
        }

        let stats = options
            .statistics
            .clone()
            .unwrap_or_else(|| Arc::new(Statistics::new(options.max_levels)));
        stats.l0_compaction_trigger.store(
            options.level0_compaction_trigger as u64,
            AtomicOrdering::Relaxed,
        );
        stats.l0_slowdown_writes_trigger.store(
            options.level0_slowdown_writes_trigger as u64,
            AtomicOrdering::Relaxed,
        );
        stats.l0_stop_writes_trigger.store(
            options.level0_stop_writes_trigger as u64,
            AtomicOrdering::Relaxed,
        );
        stats
            .memtable_limit_bytes
            .store(options.memtable_size as u64, AtomicOrdering::Relaxed);

        let mut options = options;
        if options.statistics.is_none() {
            options.statistics = Some(Arc::clone(&stats));
        }

        let path = path.as_ref().to_path_buf();
        let options = Arc::new(options);

        let exists = path.exists();
        if !exists {
            if options.create_if_missing {
                std::fs::create_dir_all(&path)?;
            } else {
                return Err(Error::NotFound);
            }
        } else if options.error_if_exists {
            return Err(Error::InvalidArgument(
                "database directory already exists".into(),
            ));
        }

        let next_wal = scan_next_wal_file_number(&path);
        let max_levels = options.max_levels;
        let block_cache = Arc::new(BlockCache::new_with_stats(
            options.block_cache_size,
            Some(Arc::clone(&stats)),
        ));
        let cache_for_open = Some(Arc::clone(&block_cache));

        // 阶段 1: WAL 回放计时与物理字节累加
        let wal_replay_start = std::time::Instant::now();
        let recovery = WALManager::recover(&path, Arc::clone(&options))?;
        let memtable = MemTable::new_with_stats(Some(Arc::clone(&stats)));
        replay_entries(&memtable, &recovery.entries)?;
        let wal_replay_duration = wal_replay_start.elapsed().as_micros() as u64;
        let wal_replayed_bytes: u64 = recovery.recovered_files.iter().map(|f| f.file_size).sum();
        stats
            .recovery_wal_replay_duration_us
            .store(wal_replay_duration, AtomicOrdering::Relaxed);
        stats
            .recovery_wal_replayed_bytes
            .store(wal_replayed_bytes, AtomicOrdering::Relaxed);

        let mut last_sequence = recovery.max_sequence;
        last_sequence = last_sequence.max(max_sequence_in_memtable(&memtable));

        // 阶段 2: Manifest 加载与版本树重建计时
        let manifest_start = std::time::Instant::now();
        let version_set = if current_exists(&path) {
            VersionSet::recover(&path, max_levels, options.max_manifest_size)?
        } else {
            let edits = scan_version_edits_from_dir(
                &path,
                max_levels,
                cache_for_open.clone(),
                Some(Arc::clone(&stats)),
            )?;
            if edits.is_empty() {
                VersionSet::open_new(&path, max_levels, options.max_manifest_size)?
            } else {
                VersionSet::bootstrap_from_scan(
                    &path,
                    max_levels,
                    options.max_manifest_size,
                    edits,
                )?
            }
        };
        let manifest_duration = manifest_start.elapsed().as_micros() as u64;
        stats
            .recovery_manifest_duration_us
            .store(manifest_duration, AtomicOrdering::Relaxed);

        // 阶段 3: SSTable Reader 打开与元数据加载计时
        let sstable_open_start = std::time::Instant::now();
        let sstables = load_sstables_from_version(
            &path,
            version_set.current(),
            cache_for_open,
            Some(Arc::clone(&stats)),
        )?;
        let sstable_open_duration = sstable_open_start.elapsed().as_micros() as u64;
        stats
            .recovery_sstable_open_duration_us
            .store(sstable_open_duration, AtomicOrdering::Relaxed);

        remove_orphan_sstables(&path, version_set.current())?;
        last_sequence = last_sequence.max(max_sequence_in_sstables(&sstables));

        let next_sequence = last_sequence.saturating_add(1).max(1);
        let wal = WALManager::open(&path, next_wal, next_sequence, Arc::clone(&options))?;

        let compaction_picker = Arc::new(CompactionPicker::from_options(&options));
        let background_compaction = options.background_compaction;

        let num_threads = if background_compaction {
            options.compaction_threads.clamp(1, 4)
        } else {
            0
        };
        let (compaction_signals, compaction_receivers): (Vec<Sender<()>>, Vec<Receiver<()>>) = (0
            ..num_threads)
            .map(|_| crossbeam_channel::bounded(options.compaction_channel_size))
            .unzip();

        stats.sequence.store(last_sequence, AtomicOrdering::Relaxed);
        stats.block_cache_size.store(0, AtomicOrdering::Relaxed);
        let pending = version_set.pending_compaction_bytes(&compaction_picker);
        update_sstable_metrics(&sstables, &stats, pending);
        #[cfg(feature = "monitoring")]
        {
            crate::metrics::init();
        }

        let l0_sstable_count_init = sstables[0].len();

        let db = Arc::new(DB {
            path,
            options,
            memtable: RwLock::new(memtable),
            immutable_memtables: RwLock::new(Vec::new()),
            wal: RwLock::new(wal),
            sstables: RwLock::new(sstables),
            version_set: RwLock::new(version_set),
            compaction_picker,
            sequence: AtomicU64::new(last_sequence),
            committed_sequence: AtomicU64::new(last_sequence),
            write_lock: Mutex::new(()),
            flush_lock: Mutex::new(()),
            flush_shutdown: Arc::new(AtomicBool::new(false)),
            flush_handle: Mutex::new(None),
            compaction_shutdown: Arc::new(AtomicBool::new(false)),
            compaction_signals,
            compaction_handles: Mutex::new(Vec::new()),
            compacting: Mutex::new(HashSet::new()),
            closed: AtomicBool::new(false),
            compaction_filter: RwLock::new(None),
            compaction_removal_listener: RwLock::new(None),
            checkpoint_in_progress: AtomicBool::new(false),
            block_cache,
            snapshots: SnapshotList::new(),
            group_commit_lock: Mutex::new(()),
            group_commit_synced_seq: AtomicU64::new(0),
            l0_sstable_count: AtomicUsize::new(l0_sstable_count_init),
            stats: Arc::clone(&stats),
        });

        db.start_flush_thread();
        if background_compaction {
            db.start_compaction_threads(compaction_receivers);
        }
        tracing::info!(target: "db", "db.open.complete");
        Ok(db)
    }

    fn start_compaction_threads(self: &Arc<Self>, receivers: Vec<Receiver<()>>) {
        let mut handles = self.compaction_handles.lock();
        for (i, rx) in receivers.into_iter().enumerate() {
            let weak = Arc::downgrade(self);
            let shutdown = Arc::clone(&self.compaction_shutdown);
            let handle = std::thread::Builder::new()
                .name(format!("aidb-compaction-{i}"))
                .spawn(move || compaction::compaction_background_loop(weak, shutdown, rx))
                .expect("spawn compaction thread");
            handles.push(handle);
        }
    }

    fn start_flush_thread(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let shutdown = Arc::clone(&self.flush_shutdown);
        let flush_poll_ms = self.options.flush_poll_ms;
        let handle = std::thread::Builder::new()
            .name("aidb-flush".into())
            .spawn(move || loop {
                if shutdown.load(AtomicOrdering::Acquire) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(flush_poll_ms));
                if shutdown.load(AtomicOrdering::Acquire) {
                    break;
                }
                let Some(db) = weak.upgrade() else {
                    break;
                };
                if let Err(e) = db.flush_pending() {
                    tracing::warn!(target: "db", error = %e, "background flush failed");
                }
            })
            .expect("spawn flush thread");
        *self.flush_handle.lock() = Some(handle);
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn options(&self) -> &Arc<Options> {
        &self.options
    }

    /// 设置 compaction 过滤器. 在下次 compaction 时生效.
    pub fn set_compaction_filter(&self, filter: Option<Arc<dyn CompactionFilter>>) {
        *self.compaction_filter.write() = filter;
    }

    /// 设置 filter Remove (最新 Put) 监听器. 在下次 compaction 时生效.
    pub fn set_compaction_removal_listener(
        &self,
        listener: Option<Arc<dyn CompactionRemovalListener>>,
    ) {
        *self.compaction_removal_listener.write() = listener;
    }

    pub fn use_wal(&self) -> bool {
        self.options.use_wal
    }

    pub(crate) fn enter_checkpoint(&self) {
        self.checkpoint_in_progress
            .store(true, AtomicOrdering::Release);
    }

    pub(crate) fn leave_checkpoint(&self) {
        self.checkpoint_in_progress
            .store(false, AtomicOrdering::Release);
    }

    pub(crate) fn pin_sstables(&self) -> Vec<Vec<Arc<SSTableReader>>> {
        self.sstables.read().iter().cloned().collect()
    }

    pub(crate) fn collect_checkpoint_file_paths(&self) -> Result<Vec<PathBuf>> {
        use crate::engine::compaction::current_exists;
        use crate::engine::wal::manager::WALManager;

        const CURRENT_FILE: &str = "CURRENT";
        let mut paths = Vec::new();
        let db_path = &self.path;

        if current_exists(db_path) {
            paths.push(db_path.join(CURRENT_FILE));
        }

        let manifest = self.version_set.read().manifest_path().to_path_buf();
        if manifest.exists() {
            paths.push(manifest);
        }

        paths.extend(WALManager::scan_wal_file_paths(db_path));

        let version = self.version_set.read().current().clone();
        for (level, files) in version.levels.iter().enumerate() {
            for meta in files {
                let p = sstable_path(db_path, meta.file_number, level);
                if p.exists() {
                    paths.push(p);
                }
            }
        }

        Ok(paths)
    }

    /// Level 0 SSTable 文件数量 (测试/诊断).
    pub fn level0_sstable_count(&self) -> usize {
        self.sstables.read()[0].len()
    }

    /// 引擎条目估值: 已打开 SSTable 的 `num_entries` 加上活跃和不可变 MemTable 的 `entry_count`.
    /// 不扫描 key. `properties` 缺失的文件按 0.
    pub fn estimate_entry_count(&self) -> u64 {
        let sst_entries = {
            let levels = self.sstables.read();
            levels
                .iter()
                .flatten()
                .map(|reader| reader.properties().map(|p| p.num_entries).unwrap_or(0))
                .fold(0u64, u64::saturating_add)
        };
        let mem_entries = self.memtable.read().entry_count();
        let imm_entries = self
            .immutable_memtables
            .read()
            .iter()
            .fold(0u64, |sum, imm| sum.saturating_add(imm.entry_count()));
        sst_entries
            .saturating_add(mem_entries)
            .saturating_add(imm_entries)
    }

    /// 待 Compaction 积压量 (Pending Bytes) 纯内存现算.
    pub fn pending_compaction_bytes(&self) -> u64 {
        self.version_set
            .read()
            .pending_compaction_bytes(&self.compaction_picker)
    }

    /// Block cache 统计快照.
    pub fn cache_stats(&self) -> CacheStats {
        self.block_cache.stats()
    }

    /// 清零 block cache 统计计数器.
    pub fn reset_cache_stats(&self) {
        self.block_cache.reset_stats()
    }

    /// 清空 block cache 条目 (不重置统计).
    pub fn clear_cache(&self) {
        self.block_cache.clear()
    }

    /// 当前 block cache 占用字节.
    pub fn block_cache_size(&self) -> u64 {
        self.block_cache.size()
    }

    /// 配置的 block cache 容量上限.
    pub fn block_cache_capacity(&self) -> usize {
        self.block_cache.capacity()
    }

    /// 进程内近似热数据内存: active/frozen MemTable + Block Cache.
    pub fn approximate_memory_bytes(&self) -> u64 {
        let active = self.memtable.read().approximate_size() as u64;
        let frozen: u64 = self
            .immutable_memtables
            .read()
            .iter()
            .map(|t| t.inner().approximate_size() as u64)
            .sum();
        self.block_cache_size() + active + frozen
    }

    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(AtomicOrdering::SeqCst)
    }

    /// 等待 flush 的 Immutable MemTable 数量 (测试/诊断).
    pub fn immutable_memtable_count(&self) -> usize {
        self.immutable_memtables.read().len()
    }

    pub(crate) fn iter_at_sequence(
        &self,
        sequence: u64,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<DbIterGuard> {
        self.check_not_closed()?;
        let mem = self.memtable.read();
        let imm = self.immutable_memtables.read();
        let sst = self.sstables.read();
        Ok(super::iterator::DBIterator::new(
            &mem,
            &imm,
            &sst,
            sequence,
            start,
            end,
            Some(Arc::clone(&self.stats)),
        ))
    }

    fn check_not_closed(&self) -> Result<()> {
        if self.closed.load(AtomicOrdering::Acquire) {
            return Err(Error::InvalidState("database is closed".into()));
        }
        Ok(())
    }

    fn validate_user_key(key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(Error::InvalidArgument("empty key not allowed".into()));
        }
        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        if self.closed.load(AtomicOrdering::Acquire) {
            return Ok(());
        }
        self.compaction_shutdown
            .store(true, AtomicOrdering::Release);
        for s in &self.compaction_signals {
            let _ = s.try_send(());
        }
        for h in self.compaction_handles.lock().drain(..) {
            let _ = h.join();
        }
        self.flush_shutdown.store(true, AtomicOrdering::Release);
        if let Some(h) = self.flush_handle.lock().take() {
            let _ = h.join();
        }
        self.do_flush()?;
        *self.compaction_filter.write() = None;
        *self.compaction_removal_listener.write() = None;
        self.closed.store(true, AtomicOrdering::Release);
        self.wal.write().sync()?;
        let _ = self.wal.write().close();
        let _ = self.wal.write().cleanup(u64::MAX);
        tracing::info!(target: "db", "db.close");
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn replace_sstables_for_test(&self, levels: Vec<Vec<Arc<SSTableReader>>>) {
        *self.sstables.write() = levels;
    }
}

fn max_sequence_in_memtable(mt: &MemTable) -> u64 {
    let mut max = 0u64;
    for entry in mt.rep().inner_map().iter() {
        if let Ok(seq) = extract_sequence(entry.key().as_ref()) {
            max = max.max(seq);
        }
    }
    max
}

fn max_sequence_in_sstables(sstables: &[Vec<Arc<SSTableReader>>]) -> u64 {
    let mut max = 0u64;
    for level in sstables {
        for reader in level {
            let mut it = reader.iter();
            while it.valid() {
                if let Some(key) = it.key() {
                    if let Ok(seq) = extract_sequence(key) {
                        max = max.max(seq);
                    }
                }
                if !it.advance() {
                    break;
                }
            }
        }
    }
    max
}

pub(crate) fn update_sstable_metrics(
    sstables: &[Vec<Arc<SSTableReader>>],
    stats: &Statistics,
    pending_bytes: u64,
) {
    // live_bytes: 全部 SST 的有效 entry 载荷 (raw_key_size + raw_value_size) 之和,
    // 空间放大 SA 分母的 SST 侧; flush/compaction 改变 SST 集合后随本函数自然重算
    let live_total: u64 = sstables
        .iter()
        .flatten()
        .map(|r| {
            r.properties()
                .map(|p| p.raw_key_size + p.raw_value_size)
                .unwrap_or(0)
        })
        .sum();
    stats.live_bytes.store(live_total, AtomicOrdering::Relaxed);
    for (level, readers) in sstables.iter().enumerate() {
        if level < stats.sstable_count.len() {
            let total: u64 = readers.iter().map(|r| r.file_size()).sum();
            stats.sstable_count[level].store(readers.len() as u64, AtomicOrdering::Relaxed);
            stats.sstable_size_bytes[level].store(total, AtomicOrdering::Relaxed);
        }
    }
    stats
        .compaction_pending_bytes
        .store(pending_bytes, AtomicOrdering::Relaxed);
}

impl Drop for DB {
    fn drop(&mut self) {
        if self.closed.load(AtomicOrdering::Acquire) {
            return;
        }
        self.compaction_shutdown
            .store(true, AtomicOrdering::Release);
        for s in &self.compaction_signals {
            let _ = s.try_send(());
        }
        for h in self.compaction_handles.lock().drain(..) {
            let _ = h.join();
        }
        self.flush_shutdown.store(true, AtomicOrdering::Release);
        if let Some(h) = self.flush_handle.lock().take() {
            let _ = h.join();
        }
        let _ = self.wal.write().sync();
    }
}
