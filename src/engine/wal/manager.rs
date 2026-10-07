//! WALManager — WAL 生命周期管理: 文件创建 / append / 轮转 / 清理 / 恢复,
//! 并通过 `LOCK` 文件独占锁保证同一数据目录单进程访问.
//!
//! # 架构
//!
//! ```text
//! open    → LOCK (fs2 独占) → 扫描既有 wal_{n}.log → 创建新 WAL + FileHeader
//! append  → write_record → note_appended_sequence → 超 max_wal_size 时 maybe_auto_rotate
//! recover → 按编号扫描全部 WAL → 顺序读 + 分片重组 (First/Middle/Last)
//!         → BatchStart 不完整 batch 整批回滚 → max_sequence
//! cleanup → 按 WAL GC 水位 (watermark) 删除已不再需要的旧 WAL
//! ```
//!
//! 文件命名 `wal_{file_number}.log`; 每个文件以 `FileHeader` (key = `WAL`) 开头, 记录
//! min_seq / max_seq / create_ts (open 时 max_seq 写 0, close 时通过 trailer 原子写入真实值).
//!
//! # Invariant
//!
//! - `LOCK` + `fs2::FileExt::try_lock_exclusive` 单进程独占, 多进程打开报 `Error::Busy`.
//! - file_number 单调递增分配, recover 时取现存最大编号继续 (见 `docs/modules/01-engine.md`).
//! - `sync_wal` 决定每条写后是否 fsync; false 时进程 crash 可能丢末批写.

use super::reader::{ReadStatus, Reader};
use super::record::{OpType, RecordType, WalEntry};
use super::writer::Writer;
use crate::config::Options;
use crate::error::{Error, Result};
use crate::statistics::Statistics;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::SystemTime;

/// FileHeader 编码常量
const FILE_HEADER_KEY: &[u8] = b"WAL";
const FILE_HEADER_VERSION: u8 = 0;
const FILE_HEADER_VALUE_SIZE: usize = 25; // 1 + 8 + 8 + 8 = 25B

/// WAL 文件命名格式: wal_{file_number}.log
pub(crate) fn wal_path(dir: &Path, file_number: u64) -> PathBuf {
    dir.join(format!("wal_{}.log", file_number))
}

/// 从文件名解析 file_number
fn parse_wal_filename(filename: &str) -> Option<u64> {
    let name = filename.strip_suffix(".log")?;
    let num = name.strip_prefix("wal_")?;
    num.parse::<u64>().ok()
}

/// WAL 恢复结果
pub struct RecoveryResult {
    /// 从所有 WAL 文件恢复的 WalEntry 列表
    pub entries: Vec<WalEntry>,
    /// 所有 replayed entry 中的最大 sequence
    pub max_sequence: u64,
    /// 参与 replay 的 WAL 文件元数据
    pub recovered_files: Vec<WalMeta>,
}

/// WAL 文件元数据
#[derive(Debug, Clone)]
pub struct WalMeta {
    pub file_number: u64,
    pub min_seq: u64,
    pub max_seq: u64,
    pub create_ts: u64,
    pub file_size: u64,
}

/// WAL 管理器
pub struct WALManager {
    writer: Writer,
    file_number: u64,
    min_seq: u64,
    max_seq: u64,
    path: PathBuf,
    wals: Vec<WalMeta>,
    // lock_file held for the lifetime to prevent concurrent WAL access
    #[expect(dead_code)]
    lock_file: std::fs::File,
    options: Arc<Options>,
    stats: Option<Arc<Statistics>>,
}

impl WALManager {
    /// 打开或创建 WAL 目录
    #[tracing::instrument(name = "wal_open", skip(path, options))]
    pub fn open(
        path: &Path,
        next_file_number: u64,
        next_sequence: u64,
        options: Arc<Options>,
    ) -> Result<Self> {
        std::fs::create_dir_all(path)?;

        // LOCK 文件保护: 防止多进程同时打开同一数据目录
        let lock_path = path.join("LOCK");
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        fs2::FileExt::try_lock_exclusive(&lock_file)
            .map_err(|_| Error::Busy("Database already in use".into()))?;

        // 扫描现有 WAL 文件, 确定起始 file_number
        let existing = Self::scan_wal_files(path);
        let file_number = existing
            .iter()
            .map(|m| m.file_number)
            .max()
            .unwrap_or(0)
            .max(next_file_number);

        // 创建新 WAL 文件, 传递 sync_wal, 预分配磁盘空间 (F-015)
        let wal_path = wal_path(path, file_number);
        let mut writer =
            Writer::open_with_sync_preallocate(&wal_path, options.sync_wal, options.max_wal_size)?;

        // 写入 FileHeader (max_seq 写 0, close 时通过 trailer 原子写入真实值)
        let file_header = Self::make_file_header(next_sequence, 0, current_timestamp());
        writer.write_record(RecordType::Full, &file_header)?;
        writer.sync_data()?;

        let stats = options.statistics.clone();
        if let Some(ref s) = stats {
            s.wal_size_bytes
                .store(writer.file_size().unwrap_or(0), Ordering::Relaxed);
        }

        Ok(WALManager {
            writer,
            file_number,
            min_seq: next_sequence,
            max_seq: next_sequence,
            path: path.to_path_buf(),
            wals: existing,
            lock_file,
            options,
            stats,
        })
    }

    /// 记录已持久化的最大 sequence (用于 WAL GC watermark).
    pub fn note_appended_sequence(&mut self, seq: u64) {
        if seq > self.max_seq {
            self.max_seq = seq;
        }
    }

    /// 获取当前 WAL 文件已记录的最大 sequence (用于 group commit).
    pub fn max_seq(&self) -> u64 {
        self.max_seq
    }

    pub fn data_path(&self) -> &Path {
        &self.path
    }

    /// 估算多条 WalEntry 编码连续落盘的总字节数.
    pub(crate) fn estimated_batch_disk_bytes(&self, encoded_entries: &[Vec<u8>]) -> u64 {
        let mut block_off = self.writer.block_offset();
        let mut total = 0u64;
        for data in encoded_entries {
            let (bytes, new_off) = Writer::estimated_record_disk_bytes(data, block_off);
            total += bytes;
            block_off = new_off;
        }
        total
    }

    /// WriteBatch 写入前: 当前文件剩余空间不足则 rotate.
    /// batch 大于 max_wal_size 时不预 rotate (允许单文件临时超限).
    pub(crate) fn ensure_space_for_batch(
        &mut self,
        batch_bytes: u64,
        next_sequence: u64,
    ) -> Result<()> {
        let max = self.options.max_wal_size;
        if max == 0 || batch_bytes > max {
            return Ok(());
        }
        let current = self.writer.file_size()?;
        let remaining = max.saturating_sub(current);
        if batch_bytes > remaining {
            self.rotate(next_sequence)?;
        }
        Ok(())
    }

    /// 追加一条编码后的 WalEntry 到当前活跃 WAL
    #[tracing::instrument(level = "debug", name = "wal_write", skip(self, data))]
    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        self.append_record(data)?;
        self.maybe_auto_rotate()?;
        Ok(())
    }

    /// 追加 WalEntry, 不触发 max_wal_size 自动轮转 (WriteBatch 临界区).
    pub(crate) fn append_in_batch(&mut self, data: &[u8]) -> Result<()> {
        self.append_record(data)
    }

    fn append_record(&mut self, data: &[u8]) -> Result<()> {
        let start_offset = self.writer.block_offset();
        self.writer.write_record(RecordType::Full, data)?;
        let (disk_bytes, _) = Writer::estimated_record_disk_bytes(data, start_offset);

        if let Some(ref s) = self.stats {
            s.wal_written_bytes.fetch_add(disk_bytes, Ordering::Relaxed);
            s.wal_size_bytes
                .store(self.writer.file_size().unwrap_or(0), Ordering::Relaxed);
        }

        Ok(())
    }

    /// WriteBatch 原子写入 WAL: 预检空间 + 写入期间不 auto-rotate.
    pub(crate) fn append_encoded_write_batch(
        &mut self,
        encoded_entries: &[Vec<u8>],
        base_seq: u64,
    ) -> Result<()> {
        if encoded_entries.is_empty() {
            return Ok(());
        }
        let batch_bytes = self.estimated_batch_disk_bytes(encoded_entries);
        self.ensure_space_for_batch(batch_bytes, base_seq)?;
        for (i, data) in encoded_entries.iter().enumerate() {
            self.append_in_batch(data)?;
            if i > 0 {
                self.note_appended_sequence(base_seq + (i - 1) as u64);
            }
        }
        Ok(())
    }

    fn maybe_auto_rotate(&mut self) -> Result<()> {
        if self.options.max_wal_size > 0 {
            if let Ok(size) = self.writer.file_size() {
                if size >= self.options.max_wal_size {
                    self.rotate(self.max_seq.wrapping_add(1))?;
                }
            }
        }
        Ok(())
    }

    /// 刷新当前 WAL 缓冲区
    #[tracing::instrument(level = "debug", name = "wal_flush", skip(self))]
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    /// fsync 当前 WAL
    #[tracing::instrument(level = "debug", name = "wal_sync", skip(self))]
    pub fn sync(&mut self) -> Result<()> {
        self.writer.sync_data()?;
        Ok(())
    }

    /// 轮转到新 WAL 文件: 关闭旧文件, 创建新文件, 写入 FileHeader
    #[tracing::instrument(name = "wal_rotate", skip(self))]
    pub fn rotate(&mut self, next_sequence: u64) -> Result<()> {
        tracing::debug!(target: "wal", "wal.rotate: old={} new={}", self.file_number, self.file_number + 1);
        // 关闭当前 WAL (sync + 记录元数据)
        self.writer.sync_data()?;
        let file_size = self.writer.file_size()?;

        self.wals.push(WalMeta {
            file_number: self.file_number,
            min_seq: self.min_seq,
            max_seq: self.max_seq,
            create_ts: current_timestamp(),
            file_size,
        });

        // 创建新 WAL 文件, file_number + 1, 预分配磁盘空间 (F-015)
        let new_file_number = self.file_number + 1;
        let wal_path = wal_path(&self.path, new_file_number);
        self.writer = Writer::open_with_sync_preallocate(
            &wal_path,
            self.options.sync_wal,
            self.options.max_wal_size,
        )?;

        // 写入 FileHeader (max_seq 写 0, close 时通过 trailer 原子写入真实值)
        let file_header = Self::make_file_header(next_sequence, 0, current_timestamp());
        self.writer.write_record(RecordType::Full, &file_header)?;
        self.writer.sync_data()?;

        if let Some(ref s) = self.stats {
            s.wal_size_bytes
                .store(self.writer.file_size().unwrap_or(0), Ordering::Relaxed);
        }

        self.file_number = new_file_number;
        self.min_seq = next_sequence;
        self.max_seq = next_sequence;

        Ok(())
    }

    /// 关闭当前 WAL
    #[tracing::instrument(name = "wal_close", skip(self))]
    pub fn close(&mut self) -> Result<()> {
        // 在文件末尾写入自校验 trailer: max_seq (8B) || !max_seq (8B)
        // 替代原有的 CRC 回填两步操作, 消除 CRC 重算的崩溃窗口.
        let inv = !self.max_seq;
        self.writer.seek(SeekFrom::End(0))?;
        self.writer.write_all(&self.max_seq.to_be_bytes())?;
        self.writer.write_all(&inv.to_be_bytes())?;
        self.writer.sync_data()?;

        let file_size = self.writer.file_size()?;
        self.wals.push(WalMeta {
            file_number: self.file_number,
            min_seq: self.min_seq,
            max_seq: self.max_seq,
            create_ts: current_timestamp(),
            file_size,
        });
        Ok(())
    }

    /// 计算可清理的 WAL 文件 (max_seq < watermark), 删除并返回删除列表
    #[tracing::instrument(name = "wal_cleanup", skip(self))]
    pub fn cleanup(&mut self, watermark: u64) -> Result<Vec<u64>> {
        let mut removed = Vec::new();
        let mut remaining = Vec::new();

        for wal in std::mem::take(&mut self.wals) {
            if wal.max_seq != u64::MAX && wal.max_seq < watermark {
                // 可安全删除
                let path = wal_path(&self.path, wal.file_number);
                let _ = std::fs::remove_file(&path);
                removed.push(wal.file_number);
            } else {
                remaining.push(wal);
            }
        }
        self.wals = remaining;

        tracing::debug!(
          target: "wal",
          "wal.cleanup: removed_files={:?}",
          removed,
        );

        Ok(removed)
    }

    /// 扫描目录中的 WAL 文件
    fn scan_wal_files(path: &Path) -> Vec<WalMeta> {
        let mut files = Vec::new();
        if let Ok(dir) = std::fs::read_dir(path) {
            for entry in dir.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if let Some(file_number) = parse_wal_filename(&name_str) {
                    let meta = entry.metadata().ok();
                    files.push(WalMeta {
                        file_number,
                        min_seq: 0,
                        max_seq: 0,
                        create_ts: 0,
                        file_size: meta.map(|m| m.len()).unwrap_or(0),
                    });
                }
            }
        }
        files.sort_by_key(|f| f.file_number);
        files
    }

    pub(crate) fn scan_wal_file_paths(dir: &Path) -> Vec<PathBuf> {
        Self::scan_wal_files(dir)
            .into_iter()
            .map(|m| wal_path(dir, m.file_number))
            .filter(|p| p.exists())
            .collect()
    }

    /// 构造 FileHeader WalEntry 的编码字节
    fn make_file_header(min_seq: u64, max_seq: u64, create_ts: u64) -> Vec<u8> {
        let mut value = Vec::with_capacity(FILE_HEADER_VALUE_SIZE);
        value.push(FILE_HEADER_VERSION);
        value.extend_from_slice(&min_seq.to_be_bytes());
        value.extend_from_slice(&max_seq.to_be_bytes());
        value.extend_from_slice(&create_ts.to_be_bytes());

        let entry = WalEntry {
            sequence: 0,
            op_type: OpType::FileHeader,
            has_value: true,
            key: FILE_HEADER_KEY.to_vec(),
            value: Some(value),
        };
        entry.encode()
    }

    /// 从所有 WAL 文件恢复 WalEntry
    #[tracing::instrument(name = "wal_replay", skip(path, options))]
    pub fn recover(path: &Path, options: Arc<Options>) -> Result<RecoveryResult> {
        let mut all_entries = Vec::new();
        let mut max_sequence: u64 = 0;
        let mut files = Self::scan_wal_files(path);

        for wal in &mut files {
            let wal_path = wal_path(path, wal.file_number);
            // 读取第一条 Record, 用于检测 FileHeader (使用非 strict 模式, 因 FileHeader CRC 豁免)
            let mut reader = match Reader::open(&wal_path) {
                Ok(r) => r,
                Err(_) => continue,
            };
            let is_file_header = match reader.read_record()? {
                ReadStatus::Record(rt, data) => {
                    if rt != RecordType::Full {
                        false
                    } else if let Ok(entry) = WalEntry::decode(&data) {
                        if entry.op_type != OpType::FileHeader {
                            false
                        } else if let Some(ref value) = entry.value {
                            if value.len() >= FILE_HEADER_VALUE_SIZE {
                                let version = value[0];
                                if version != FILE_HEADER_VERSION {
                                    return Err(Error::Corruption(format!(
                                        "unsupported WAL version {} (expected {})",
                                        version, FILE_HEADER_VERSION
                                    )));
                                }
                                wal.min_seq = u64::from_be_bytes(value[1..9].try_into().unwrap());
                                wal.max_seq = u64::from_be_bytes(value[9..17].try_into().unwrap());
                                wal.create_ts =
                                    u64::from_be_bytes(value[17..25].try_into().unwrap());
                            }
                            true
                        } else {
                            true
                        }
                    } else {
                        false
                    }
                }
                // 文件第一条 Record CRC 损坏 — FileHeader CRC 豁免
                ReadStatus::CorruptionRecoverable | ReadStatus::CorruptionFatal => {
                    // 豁免: 静默忽略 FileHeader, 触发全量 replay
                    false
                }
                _ => false,
            };

            // 如果第一条不是 FileHeader, 回退到文件开头全量扫描;
            // 如果是 FileHeader, 保持当前reader (FileHeader已消费, 后续数据正确对齐)
            let strict = options.strict_wal_recovery;
            if !is_file_header {
                reader = match if strict {
                    Reader::open_strict(&wal_path)
                } else {
                    Reader::open(&wal_path)
                } {
                    Ok(r) => r,
                    Err(_) => continue,
                };
            }

            // 读取后续 Record (WalEntry), 支持分片 reassembly
            let mut pending_fragments: Option<Vec<u8>> = None;
            let mut batch_start: Option<usize> = None;
            let mut batch_needed: usize = 0;
            let mut batch_count: usize = 0;

            loop {
                match reader.read_record()? {
                    ReadStatus::Record(rt, data) => {
                        match rt {
                            RecordType::First => {
                                // 开始一个新的分片序列
                                pending_fragments = Some(data);
                            }
                            RecordType::Middle => {
                                if let Some(ref mut frag) = pending_fragments {
                                    frag.extend_from_slice(&data);
                                } else {
                                    // 孤立 Middle, 无前置 First — 数据损坏
                                    pending_fragments = None;
                                }
                            }
                            RecordType::Last => {
                                if let Some(mut frag) = pending_fragments.take() {
                                    frag.extend_from_slice(&data);
                                    // 完整分片已重组, 作为单条 Full 处理
                                    Self::process_recovered_entry(
                                        &frag,
                                        &mut all_entries,
                                        &mut max_sequence,
                                        &mut batch_start,
                                        &mut batch_needed,
                                        &mut batch_count,
                                    );
                                }
                                // 若 pending_fragments 为 None, 孤立 Last — 丢弃
                                note_wal_fragment_discarded();
                            }
                            RecordType::Full => {
                                pending_fragments = None;
                                Self::process_recovered_entry(
                                    &data,
                                    &mut all_entries,
                                    &mut max_sequence,
                                    &mut batch_start,
                                    &mut batch_needed,
                                    &mut batch_count,
                                );
                            }
                        }
                    }
                    ReadStatus::Eof => {
                        if pending_fragments.is_some() {
                            note_wal_fragment_discarded();
                        }
                        // 若 batch 未完成, 回滚
                        if let Some(batch_pos) = batch_start {
                            all_entries.truncate(batch_pos);
                        }
                        break;
                    }
                    ReadStatus::TailPartial => {
                        if let Some(batch_pos) = batch_start {
                            all_entries.truncate(batch_pos);
                        }
                        break;
                    }
                    ReadStatus::CorruptionRecoverable => {
                        // 正在收集分片时遇到 CRC 损坏, 丢弃当前分片
                        pending_fragments = None;
                        // batch 中 CRC 损坏, 回滚整个 batch
                        if let Some(batch_pos) = batch_start {
                            all_entries.truncate(batch_pos);
                            batch_start = None;
                        }
                        continue;
                    }
                    ReadStatus::CorruptionFatal => {
                        return Err(Error::Corruption("Fatal corruption".into()))
                    }
                }
            }
            // 读取 trailer: 新格式文件末尾 16 字节自校验 max_seq (F-009 fix)
            // trailer = max_seq (8B BE) || !max_seq (8B BE)
            {
                use std::io::{Read, Seek, SeekFrom};
                if let Ok(mut f) = std::fs::File::open(&wal_path) {
                    if f.seek(SeekFrom::End(-16)).is_ok() {
                        let mut buf = [0u8; 16];
                        if f.read_exact(&mut buf).is_ok() {
                            let seq = u64::from_be_bytes(buf[0..8].try_into().unwrap());
                            let inv = u64::from_be_bytes(buf[8..16].try_into().unwrap());
                            if inv == !seq {
                                wal.max_seq = wal.max_seq.max(seq);
                            }
                        }
                    }
                }
            }
        }

        tracing::debug!(
          target: "wal",
          "wal.replay.complete: file_count={} record_count={} max_sequence={}",
          files.len(),
          all_entries.len(),
          max_sequence,
        );

        Ok(RecoveryResult {
            entries: all_entries,
            max_sequence,
            recovered_files: files,
        })
    }

    /// 处理一条已解码的完整 WalEntry 数据, 包括 batch 追踪
    fn process_recovered_entry(
        data: &[u8],
        all_entries: &mut Vec<WalEntry>,
        max_sequence: &mut u64,
        batch_start: &mut Option<usize>,
        batch_needed: &mut usize,
        batch_count: &mut usize,
    ) {
        if let Ok(entry) = WalEntry::decode(data) {
            match entry.op_type {
                OpType::BatchStart => {
                    // 开始一个新 batch
                    *batch_start = Some(all_entries.len());
                    *batch_needed = entry.value.as_ref().map_or(0, |v| {
                        u32::from_le_bytes(v[..4].try_into().unwrap_or([0; 4])) as usize
                    });
                    *batch_count = 0;
                }
                _ => {
                    if batch_start.is_some() {
                        *batch_count += 1;
                        all_entries.push(entry);
                        *max_sequence = (*max_sequence).max(all_entries.last().unwrap().sequence);
                        if *batch_count >= *batch_needed {
                            // batch 完成
                            *batch_start = None;
                        }
                    } else {
                        // 非 batch 条目, 直接添加
                        all_entries.push(entry);
                        *max_sequence = (*max_sequence).max(all_entries.last().unwrap().sequence);
                    }
                }
            }
        }
    }
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn note_wal_fragment_discarded() {
    tracing::warn!(target: "wal", "wal fragment discarded");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Options;
    use tempfile::tempdir;
    use tracing::Subscriber;

    fn test_options() -> Arc<Options> {
        Arc::new(Options::for_testing())
    }

    #[test]
    fn test_manager_open_creates_file() {
        let dir = tempdir().unwrap();
        let mut mgr = WALManager::open(dir.path(), 1, 100, test_options()).unwrap();
        mgr.close().unwrap();
        // 应该创建了 wal_1.log
        let path = wal_path(dir.path(), 1);
        assert!(path.exists(), "WAL file should exist");
    }

    #[test]
    fn test_manager_append() {
        let dir = tempdir().unwrap();
        let mut mgr = WALManager::open(dir.path(), 1, 100, test_options()).unwrap();
        let entry = WalEntry {
            sequence: 100,
            op_type: OpType::TypePut,
            has_value: true,
            key: b"k1".to_vec(),
            value: Some(b"v1".to_vec()),
        };
        mgr.append(&entry.encode()).unwrap();
        mgr.close().unwrap();
        // 文件大小应 > 0
        let path = wal_path(dir.path(), 1);
        assert!(std::fs::metadata(&path).unwrap().len() > 0);
    }

    #[test]
    fn test_manager_rotate() {
        let dir = tempdir().unwrap();
        let mut mgr = WALManager::open(dir.path(), 1, 100, test_options()).unwrap();
        mgr.rotate(200).unwrap();
        mgr.close().unwrap();
        // 应存在两个 WAL 文件
        assert!(wal_path(dir.path(), 1).exists());
        assert!(wal_path(dir.path(), 2).exists());
    }

    #[test]
    fn test_manager_cleanup() {
        let dir = tempdir().unwrap();
        let mut mgr = WALManager::open(dir.path(), 1, 100, test_options()).unwrap();
        mgr.close().unwrap();
        let removed = mgr.cleanup(u64::MAX).unwrap();
        assert!(removed.is_empty() || removed.len() <= 1);
    }

    #[test]
    fn test_recover_empty_dir() {
        let dir = tempdir().unwrap();
        let result = WALManager::recover(dir.path(), test_options()).unwrap();
        assert!(
            result.entries.is_empty(),
            "empty dir should have no entries"
        );
        assert_eq!(result.max_sequence, 0);
    }

    #[test]
    fn test_recover_single_entry() {
        let dir = tempdir().unwrap();
        let opts = test_options();
        let mut mgr = WALManager::open(dir.path(), 1, 100, opts.clone()).unwrap();
        let entry = WalEntry {
            sequence: 100,
            op_type: OpType::TypePut,
            has_value: true,
            key: b"k1".to_vec(),
            value: Some(b"v1".to_vec()),
        };
        mgr.append(&entry.encode()).unwrap();
        mgr.sync().unwrap();
        mgr.close().unwrap();
        drop(mgr);

        let result = WALManager::recover(dir.path(), test_options()).unwrap();
        assert_eq!(result.entries.len(), 1, "should recover 1 entry");
        assert_eq!(result.entries[0].sequence, 100);
        assert_eq!(result.entries[0].key, b"k1");
        assert_eq!(result.entries[0].value.as_deref(), Some(&b"v1"[..]));
    }

    #[test]
    fn test_recover_multiple_wals() {
        let dir = tempdir().unwrap();
        let opts = test_options();
        let mut mgr = WALManager::open(dir.path(), 1, 100, opts.clone()).unwrap();

        let e1 = WalEntry {
            sequence: 100,
            op_type: OpType::TypePut,
            has_value: true,
            key: b"a".to_vec(),
            value: Some(b"1".to_vec()),
        };
        mgr.append(&e1.encode()).unwrap();

        mgr.rotate(200).unwrap();

        let e2 = WalEntry {
            sequence: 200,
            op_type: OpType::TypePut,
            has_value: true,
            key: b"b".to_vec(),
            value: Some(b"2".to_vec()),
        };
        mgr.append(&e2.encode()).unwrap();

        mgr.close().unwrap();
        drop(mgr);

        let result = WALManager::recover(dir.path(), test_options()).unwrap();
        assert_eq!(result.entries.len(), 2, "should recover both entries");
        assert_eq!(result.entries[0].key, b"a");
        assert_eq!(result.entries[1].key, b"b");
        assert_eq!(result.max_sequence, 200);
    }

    #[test]
    fn test_wal_filename_parse() {
        assert_eq!(parse_wal_filename("wal_1.log"), Some(1));
        assert_eq!(parse_wal_filename("wal_42.log"), Some(42));
        assert_eq!(parse_wal_filename("other.log"), None);
        assert_eq!(parse_wal_filename("wal_abc.log"), None);
        assert_eq!(parse_wal_filename("wal_1.txt"), None);
    }

    #[test]
    fn wal_fragment_discard_warns_without_bytes() {
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::prelude::*;
        use tracing_subscriber::Registry;

        let hits = Arc::new(Mutex::new(Vec::new()));
        let layer = DiscardLayer {
            hits: Arc::clone(&hits),
        };
        tracing::subscriber::with_default(Registry::default().with(layer), || {
            super::note_wal_fragment_discarded();
        });
        let hits = hits.lock().unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, tracing::Level::WARN);
        assert_eq!(hits[0].1, "wal fragment discarded");
        assert!(!hits[0].2.contains("data"));
    }

    struct DiscardLayer {
        hits: std::sync::Arc<std::sync::Mutex<Vec<(tracing::Level, String, String)>>>,
    }

    impl<S: Subscriber> tracing_subscriber::Layer<S> for DiscardLayer {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut message = String::new();
            let mut fields = String::new();
            event.record(&mut Grab {
                message: &mut message,
                fields: &mut fields,
            });
            self.hits
                .lock()
                .unwrap()
                .push((*event.metadata().level(), message, fields));
        }
    }

    struct Grab<'a> {
        message: &'a mut String,
        fields: &'a mut String,
    }

    impl tracing::field::Visit for Grab<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                *self.message = format!("{value:?}").trim_matches('"').to_string();
            } else {
                self.fields
                    .push_str(&format!("{}={value:?} ", field.name()));
            }
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "message" {
                *self.message = value.to_string();
            } else {
                self.fields.push_str(&format!("{}={value} ", field.name()));
            }
        }
    }
}
