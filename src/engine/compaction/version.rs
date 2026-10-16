//! Version / MANIFEST / CURRENT 管理: 维护当前 `Version` (各层 SSTable 元数据列表),
//! 所有文件增删以 `VersionEdit` 追加写入 MANIFEST, 并用 `CURRENT` 原子指向活跃 MANIFEST.
//!
//! # 架构
//!
//! ```text
//! open_new            → 新库: MANIFEST-000001 + CURRENT
//! recover             → 读 CURRENT → replay MANIFEST → 重建 Version
//! bootstrap_from_scan → 无 CURRENT 遗留库: 目录扫描生成 edits 后重建
//! apply_edit          → MANIFEST append (crc32 + len + postcard payload) → 更新内存 Version
//!                     → 超过 max_manifest_size 时 rotate_manifest
//! rotate_manifest     → 当前 Version 全量快照写入新 MANIFEST, 原子写 CURRENT
//! ```
//!
//! # Invariant
//!
//! - `CURRENT` 始终指向活跃 `MANIFEST-*`, 经 `CURRENT.tmp` + rename 原子切换 (见
//!   `docs/modules/02-engine-storage.md`).
//! - `VersionEdit` 先写 MANIFEST 并 sync 落盘, 后才更新内存 Version.
//! - 遗留库扫描 / 加载损坏文件时降级跳过 (warn), 不阻断 open.

use super::helpers::user_key_from_internal;
use super::picker::CompactionPicker;
use crate::engine::sstable::{parse_sstable_filename, sstable_path, SSTableReader};
use crate::error::{Error, Result};
use crate::statistics::Statistics;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const CURRENT_FILE: &str = "CURRENT";
const CURRENT_TMP: &str = "CURRENT.tmp";
const MANIFEST_PREFIX: &str = "MANIFEST-";

/// 一次版本变更.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum VersionEdit {
    AddFile {
        level: usize,
        file_number: u64,
        file_size: u64,
        smallest_key: Vec<u8>,
        largest_key: Vec<u8>,
    },
    DeleteFile {
        level: usize,
        file_number: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMetaData {
    pub file_number: u64,
    pub file_size: u64,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Version {
    pub levels: Vec<Vec<FileMetaData>>,
}

impl Version {
    pub fn new(max_levels: usize) -> Self {
        Self {
            levels: vec![Vec::new(); max_levels],
        }
    }

    pub fn apply(&self, edit: &VersionEdit) -> Self {
        let mut v = self.clone();
        match edit {
            VersionEdit::AddFile {
                level,
                file_number,
                file_size,
                smallest_key,
                largest_key,
            } => {
                if *level < v.levels.len() {
                    v.levels[*level].push(FileMetaData {
                        file_number: *file_number,
                        file_size: *file_size,
                        smallest_key: smallest_key.clone(),
                        largest_key: largest_key.clone(),
                    });
                }
            }
            VersionEdit::DeleteFile { level, file_number } => {
                if *level < v.levels.len() {
                    v.levels[*level].retain(|f| f.file_number != *file_number);
                }
            }
        }
        v
    }

    pub fn num_files(&self) -> usize {
        self.levels.iter().map(|l| l.len()).sum()
    }

    pub fn total_size(&self) -> u64 {
        self.levels
            .iter()
            .flat_map(|l| l.iter())
            .map(|f| f.file_size)
            .sum()
    }

    pub fn contains_file(&self, file_number: u64) -> bool {
        self.levels
            .iter()
            .any(|level| level.iter().any(|f| f.file_number == file_number))
    }
}

pub struct VersionSet {
    current: Version,
    next_file_number: AtomicU64,
    db_path: PathBuf,
    manifest_path: PathBuf,
    manifest_file_number: u64,
    manifest_file: BufWriter<File>,
    max_manifest_size: usize,
    _max_levels: usize,
}

impl VersionSet {
    /// 新库: 创建 MANIFEST-000001 + CURRENT.
    pub fn open_new(db_path: &Path, max_levels: usize, max_manifest_size: usize) -> Result<Self> {
        let manifest_file_number = 1u64;
        let manifest_path = db_path.join(format!("{MANIFEST_PREFIX}{manifest_file_number:06}"));
        let mut file = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&manifest_path)?,
        );
        file.flush()?;
        file.get_ref().sync_all()?;
        write_current_atomic(db_path, &manifest_path)?;

        let manifest_path_clone = manifest_path.clone();
        Ok(Self {
            current: Version::new(max_levels),
            next_file_number: AtomicU64::new(2),
            db_path: db_path.to_path_buf(),
            manifest_path: manifest_path_clone.clone(),
            manifest_file_number,
            manifest_file: BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&manifest_path_clone)?,
            ),
            max_manifest_size,
            _max_levels: max_levels,
        })
    }

    /// 从 CURRENT + MANIFEST replay.
    pub fn recover(db_path: &Path, max_levels: usize, max_manifest_size: usize) -> Result<Self> {
        let current_path = db_path.join(CURRENT_FILE);
        let manifest_name = fs::read_to_string(&current_path)
            .map_err(|e| Error::Corruption(format!("read CURRENT: {e}")))?
            .trim()
            .to_string();
        if !manifest_name.starts_with(MANIFEST_PREFIX) {
            return Err(Error::Corruption("invalid CURRENT manifest name".into()));
        }
        let manifest_path = db_path.join(&manifest_name);
        if !manifest_path.exists() {
            return Err(Error::Corruption(format!(
                "manifest missing: {manifest_name}"
            )));
        }
        let manifest_file_number = parse_manifest_number(&manifest_name)?;
        let mut current = Version::new(max_levels);
        let mut max_file_num = manifest_file_number;

        let file = File::open(&manifest_path)?;
        let mut reader = BufReader::new(file);
        loop {
            match read_manifest_record(&mut reader) {
                Ok(Some(edit)) => {
                    if let VersionEdit::AddFile { file_number, .. } = &edit {
                        max_file_num = max_file_num.max(*file_number);
                    }
                    current = current.apply(&edit);
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!(target: "cmp", error = %e, "truncated MANIFEST tail, stop replay");
                    break;
                }
            }
        }

        let next = max_file_num.saturating_add(1).max(2);
        Ok(Self {
            current,
            next_file_number: AtomicU64::new(next),
            db_path: db_path.to_path_buf(),
            manifest_path: manifest_path.clone(),
            manifest_file_number,
            manifest_file: BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&manifest_path)?,
            ),
            max_manifest_size,
            _max_levels: max_levels,
        })
    }

    /// 无 CURRENT: 从扫描元数据 bootstrap 首版 MANIFEST.
    pub fn bootstrap_from_scan(
        db_path: &Path,
        max_levels: usize,
        max_manifest_size: usize,
        edits: Vec<VersionEdit>,
    ) -> Result<Self> {
        let mut vs = Self::open_new(db_path, max_levels, max_manifest_size)?;
        let mut max_num = 1u64;
        for edit in edits {
            if let VersionEdit::AddFile { file_number, .. } = &edit {
                max_num = max_num.max(*file_number);
            }
            vs.apply_edit(&edit)?;
        }
        vs.next_file_number
            .store(max_num.saturating_add(1).max(2), Ordering::SeqCst);
        Ok(vs)
    }

    pub fn current(&self) -> &Version {
        &self.current
    }

    pub fn pending_compaction_bytes(&self, picker: &CompactionPicker) -> u64 {
        let levels = &self.current.levels;
        if levels.is_empty() {
            return 0;
        }

        // 1. L0 层: Version.levels[0] 为 push 追加 (升序, 最老在前)
        // 注: 与 self.sstables[0] (运行时 reader, 降序, 最新在前) 顺序相反!
        let l0_files = &levels[0];
        let mut pending_l0 = 0u64;
        let excess = l0_files
            .len()
            .saturating_sub(picker.level0_compaction_trigger());
        if excess > 0 {
            // 最老文件在索引 0, 直接零分配切片取前 excess 个文件求和
            pending_l0 = l0_files[..excess].iter().map(|f| f.file_size).sum();
        }

        // 2. L1+ 层: 复用 picker.target_size_for_level 杜绝公式漂移
        let mut pending_levels = 0u64;
        let num_levels = levels.len();
        if num_levels > 2 {
            for (level, level_files) in levels.iter().enumerate().take(num_levels - 1).skip(1) {
                let actual: u64 = level_files.iter().map(|f| f.file_size).sum();
                let target = picker.target_size_for_level(level);
                pending_levels = pending_levels.saturating_add(actual.saturating_sub(target));
            }
        }

        pending_l0.saturating_add(pending_levels)
    }

    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    pub fn allocate_file_number(&self) -> u64 {
        self.next_file_number.fetch_add(1, Ordering::SeqCst)
    }

    pub fn apply_edit(&mut self, edit: &VersionEdit) -> Result<()> {
        write_manifest_record(&mut self.manifest_file, edit)?;
        self.manifest_file.flush()?;
        self.manifest_file.get_ref().sync_all()?;
        self.current = self.current.apply(edit);
        tracing::debug!(target: "cmp", ?edit, "cmp.apply");

        if self.manifest_file.get_ref().metadata()?.len() as usize > self.max_manifest_size {
            self.rotate_manifest()?;
        }
        Ok(())
    }

    fn rotate_manifest(&mut self) -> Result<()> {
        let snapshot_edits: Vec<VersionEdit> = self
            .current
            .levels
            .iter()
            .enumerate()
            .flat_map(|(level, files)| {
                files.iter().map(move |f| VersionEdit::AddFile {
                    level,
                    file_number: f.file_number,
                    file_size: f.file_size,
                    smallest_key: f.smallest_key.clone(),
                    largest_key: f.largest_key.clone(),
                })
            })
            .collect();

        self.manifest_file.flush()?;
        let new_num = self.manifest_file_number + 1;
        let new_path = self.db_path.join(format!("{MANIFEST_PREFIX}{new_num:06}"));
        let mut new_file = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&new_path)?,
        );
        for edit in &snapshot_edits {
            write_manifest_record(&mut new_file, edit)?;
        }
        new_file.flush()?;
        new_file.get_ref().sync_all()?;
        write_current_atomic(&self.db_path, &new_path)?;

        self.manifest_path = new_path;
        self.manifest_file_number = new_num;
        self.manifest_file = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.manifest_path)?,
        );
        Ok(())
    }
}

pub fn current_exists(db_path: &Path) -> bool {
    db_path.join(CURRENT_FILE).exists()
}

/// 从 Version 元数据打开 SSTable 列表; 跳过损坏文件.
pub fn load_sstables_from_version(
    db_path: &Path,
    version: &Version,
    block_cache: Option<Arc<crate::engine::cache::BlockCache>>,
    stats: Option<Arc<Statistics>>,
) -> Result<Vec<Vec<Arc<SSTableReader>>>> {
    let mut sstables = vec![Vec::new(); version.levels.len()];
    for (level, files) in version.levels.iter().enumerate() {
        let mut opened: Vec<(u64, Arc<SSTableReader>)> = Vec::new();
        for meta in files {
            let path = sstable_path(db_path, meta.file_number, level);
            match SSTableReader::open_with_stats(&path, block_cache.clone(), stats.clone()) {
                Ok(reader) => opened.push((meta.file_number, Arc::new(reader))),
                Err(e) => {
                    tracing::warn!(
                      target: "db",
                      file = %path.display(),
                      error = %e,
                      "skip sstable from manifest"
                    );
                }
            }
        }
        opened.sort_by_key(|a| a.0);
        if level == 0 {
            for (_, r) in opened.into_iter().rev() {
                sstables[level].push(r);
            }
        } else {
            for (_, r) in opened {
                sstables[level].push(r);
            }
        }
    }
    Ok(sstables)
}

/// 目录扫描收集 bootstrap 用 VersionEdit (Phase5 遗留库).
pub fn scan_version_edits_from_dir(
    db_path: &Path,
    max_levels: usize,
    block_cache: Option<Arc<crate::engine::cache::BlockCache>>,
    stats: Option<Arc<Statistics>>,
) -> Result<Vec<VersionEdit>> {
    let mut edits = Vec::new();
    if let Ok(dir) = fs::read_dir(db_path) {
        for entry in dir.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some((num, level)) = parse_sstable_filename(&name_str) {
                if level >= max_levels {
                    continue;
                }
                let path = entry.path();
                match SSTableReader::open_with_stats(&path, block_cache.clone(), stats.clone()) {
                    Ok(reader) => {
                        let _ = user_key_from_internal(reader.smallest_key())?;
                        let _ = user_key_from_internal(reader.largest_key())?;
                        edits.push(VersionEdit::AddFile {
                            level,
                            file_number: num,
                            file_size: reader.file_size(),
                            smallest_key: reader.smallest_key().to_vec(),
                            largest_key: reader.largest_key().to_vec(),
                        });
                    }
                    Err(e) => {
                        tracing::warn!(
                          target: "db",
                          file = %path.display(),
                          error = %e,
                          "skip file during bootstrap scan"
                        );
                    }
                }
            }
        }
    }
    Ok(edits)
}

/// 删除不在 Version 中的孤儿 `.sst`.
pub fn remove_orphan_sstables(db_path: &Path, version: &Version) -> Result<()> {
    if let Ok(dir) = fs::read_dir(db_path) {
        for entry in dir.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some((num, _level)) = parse_sstable_filename(&name_str) {
                if !version.contains_file(num) {
                    let _ = fs::remove_file(entry.path());
                    tracing::debug!(
                      target: "db",
                      file = %entry.path().display(),
                      "removed orphan sstable"
                    );
                }
            }
        }
    }
    Ok(())
}

fn write_current_atomic(db_path: &Path, manifest_path: &Path) -> Result<()> {
    let name = manifest_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| Error::Corruption("bad manifest path".into()))?;
    let tmp = db_path.join(CURRENT_TMP);
    let final_path = db_path.join(CURRENT_FILE);
    {
        let mut f = File::create(&tmp)?;
        writeln!(f, "{name}")?;
        f.sync_all()?;
    }
    fs::rename(&tmp, &final_path)?;
    Ok(())
}

fn parse_manifest_number(name: &str) -> Result<u64> {
    let num_str = name
        .strip_prefix(MANIFEST_PREFIX)
        .ok_or_else(|| Error::Corruption("bad manifest filename".into()))?;
    num_str
        .parse()
        .map_err(|_| Error::Corruption("bad manifest number".into()))
}

fn write_manifest_record(writer: &mut impl Write, edit: &VersionEdit) -> Result<()> {
    let payload = postcard::to_allocvec(edit)
        .map_err(|e| Error::Corruption(format!("serialize VersionEdit: {e}")))?;
    let len = payload.len() as u32;
    let mut crc_input = Vec::with_capacity(4 + payload.len());
    crc_input.extend_from_slice(&len.to_le_bytes());
    crc_input.extend_from_slice(&payload);
    let crc = crc32fast::hash(&crc_input);
    writer.write_all(&crc.to_le_bytes())?;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(&payload)?;
    Ok(())
}

fn read_manifest_record(reader: &mut impl Read) -> Result<Option<VersionEdit>> {
    let mut crc_buf = [0u8; 4];
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut crc_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(Error::Io(e)),
    }
    reader.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > 64 * 1024 * 1024 {
        return Err(Error::Corruption("MANIFEST record too large".into()));
    }
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload)?;
    let mut crc_input = Vec::with_capacity(4 + len);
    crc_input.extend_from_slice(&len_buf);
    crc_input.extend_from_slice(&payload);
    let expected = crc32fast::hash(&crc_input);
    let actual = u32::from_le_bytes(crc_buf);
    if expected != actual {
        return Err(Error::Corruption("MANIFEST CRC mismatch".into()));
    }
    let edit: VersionEdit = postcard::from_bytes(&payload)
        .map_err(|e| Error::Corruption(format!("deserialize VersionEdit: {e}")))?;
    Ok(Some(edit))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::memtable::{encode_internal_key, ValueType};
    use crate::engine::sstable::{SSTableBuilder, SSTableReader};
    use tempfile::tempdir;

    fn make_sst(path: &Path, entries: &[(&[u8], &[u8])]) -> (u64, u64, Vec<u8>, Vec<u8>) {
        let mut b =
            SSTableBuilder::new(path, 512, 16, crate::config::CompressionType::None, 0.0).unwrap();
        for (k, v) in entries {
            b.add(k, v).unwrap();
        }
        let size = b.finish().unwrap();
        let r = SSTableReader::open(path, None).unwrap();
        (
            r.file_number(),
            size,
            r.smallest_key().to_vec(),
            r.largest_key().to_vec(),
        )
    }

    #[test]
    fn test_version_apply_add_and_delete() {
        let v = Version::new(3);
        let e1 = VersionEdit::AddFile {
            level: 0,
            file_number: 1,
            file_size: 100,
            smallest_key: vec![1],
            largest_key: vec![9],
        };
        let v2 = v.apply(&e1);
        assert_eq!(v2.levels[0].len(), 1);
        let e2 = VersionEdit::DeleteFile {
            level: 0,
            file_number: 1,
        };
        let v3 = v2.apply(&e2);
        assert!(v3.levels[0].is_empty());
    }

    #[test]
    fn test_version_set_allocate_monotonic() {
        let dir = tempdir().unwrap();
        let vs = VersionSet::open_new(dir.path(), 7, 1024 * 1024).unwrap();
        let a = vs.allocate_file_number();
        let b = vs.allocate_file_number();
        assert!(b > a);
    }

    #[test]
    fn test_manifest_recover_after_edits() {
        let dir = tempdir().unwrap();
        let k1 = encode_internal_key(b"a", 1, ValueType::TypePut);
        let k2 = encode_internal_key(b"z", 1, ValueType::TypePut);
        {
            let mut vs = VersionSet::open_new(dir.path(), 7, 1024 * 1024).unwrap();
            vs.apply_edit(&VersionEdit::AddFile {
                level: 0,
                file_number: 5,
                file_size: 123,
                smallest_key: k1.clone(),
                largest_key: k2.clone(),
            })
            .unwrap();
        }
        let vs2 = VersionSet::recover(dir.path(), 7, 1024 * 1024).unwrap();
        assert_eq!(vs2.current().levels[0].len(), 1);
        assert_eq!(vs2.current().levels[0][0].file_number, 5);
    }

    #[test]
    fn test_bootstrap_from_scan() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000003_L0.sst");
        let (_n, size, sm, lg) = make_sst(&path, &[(b"x", b"1")]);
        let edits = vec![VersionEdit::AddFile {
            level: 0,
            file_number: 3,
            file_size: size,
            smallest_key: sm,
            largest_key: lg,
        }];
        let vs = VersionSet::bootstrap_from_scan(dir.path(), 7, 1024 * 1024, edits).unwrap();
        assert!(dir.path().join("CURRENT").exists());
        assert_eq!(vs.current().num_files(), 1);
        let vs2 = VersionSet::recover(dir.path(), 7, 1024 * 1024).unwrap();
        assert_eq!(vs2.current().num_files(), 1);
    }
}
