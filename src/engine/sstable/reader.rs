//! SSTableReader — 单个 SSTable 的只读访问: Footer → Index → Bloom → Data Block.
//!
//! # 点查流程
//!
//! ```text
//! get(seek_key) / value_type(seek_key)
//!   ├─ Bloom: may_contain(extract_user_key)      miss → 直接 None (免 I/O)
//!   ├─ Index: find_block_handle 二分定位 Data Block
//!   ├─ BlockCache hit → Block; miss → 文件读取 + CRC 校验
//!   └─ BlockIterator::seek: user_key 匹配且 seq <= seek_seq
//!        → get: 返回 (value, ValueType); value_type: 仅返回 ValueType
//! ```
//!
//! # Invariant
//!
//! - Bloom 仅索引 user_key; block 内仍按 `seq <= seek_seq` 过滤.
//! - Bloom 缺失或解码失败 → warn 降级为无 filter, 不影响读正确性.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::Arc;

use bytes::Bytes;

use crate::engine::cache::BlockCache;
use crate::engine::filter::{bloom::record_bloom_false_positive, BloomFilter, Filter};
use crate::engine::memtable::{
    encode_internal_key, extract_sequence, extract_user_key, extract_value_type, range_covers,
    PointState, ValueType,
};
use crate::error::{Error, Result};
use crate::statistics::Statistics;

use super::block::Block;
use super::block_io::{read_block_cached, read_block_from_file};
use super::footer::{Footer, FOOTER_SIZE};
use super::handle::BlockHandle;
use super::index::{find_block_handle, load_index_entries, IndexBlock};
use super::iterator::SSTableIterator;
use super::meta::{find_meta_block_handle, read_raw_bytes, BLOOM_META_NAME, PROPERTIES_META_NAME};
use super::properties::SstProperties;

#[derive(Clone, Debug)]
struct RangeTombstoneEntry {
    start: Vec<u8>,
    end: Vec<u8>,
    sequence: u64,
}

pub struct SSTableReader {
    file: Arc<File>,
    file_number: u64,
    level: usize,
    index_entries: Arc<Vec<(Vec<u8>, BlockHandle)>>,
    file_size: u64,
    smallest_key: Vec<u8>,
    largest_key: Vec<u8>,
    bloom_filter: Option<BloomFilter>,
    properties: Option<SstProperties>,
    block_cache: Option<Arc<BlockCache>>,
    range_tombstones: Arc<Vec<RangeTombstoneEntry>>,
    stats: Option<Arc<Statistics>>,
}

impl SSTableReader {
    pub fn open(path: &Path, block_cache: Option<Arc<BlockCache>>) -> Result<Self> {
        Self::open_with_stats(path, block_cache, None)
    }

    pub fn open_with_stats(
        path: &Path,
        block_cache: Option<Arc<BlockCache>>,
        stats: Option<Arc<Statistics>>,
    ) -> Result<Self> {
        let file = Arc::new(File::open(path)?);
        let file_size = file.metadata()?.len();
        if file_size < FOOTER_SIZE as u64 {
            return Err(Error::Corruption("SSTable file too small".into()));
        }

        let (file_number, level) = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(super::filename::parse_sstable_filename)
            .unwrap_or((0, 0));

        let footer = read_footer(&file, file_size)?;
        let index_bytes = read_block_from_file(&file, &footer.index_handle)?;
        let index_block = Block::new(Bytes::from(index_bytes))?;
        let index_entries = Arc::new(load_index_entries(&index_block)?);

        let meta_index_bytes = read_block_from_file(&file, &footer.meta_index_handle)?;
        let meta_index = IndexBlock::new(Bytes::from(meta_index_bytes))?;
        let bloom_filter = load_bloom_filter(&file, &meta_index);
        let properties = load_properties(&file, &meta_index);

        let (smallest_key, largest_key) =
            read_key_range(&file, file_number, &index_entries, block_cache.as_ref())?;
        let range_tombstones = Arc::new(load_range_tombstones(
            &file,
            file_number,
            Arc::clone(&index_entries),
        )?);

        Ok(Self {
            file,
            file_number,
            level,
            index_entries,
            file_size,
            smallest_key,
            largest_key,
            bloom_filter,
            properties,
            block_cache,
            range_tombstones,
            stats,
        })
    }

    pub fn file_number(&self) -> u64 {
        self.file_number
    }

    pub fn level(&self) -> usize {
        self.level
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn smallest_key(&self) -> &[u8] {
        &self.smallest_key
    }

    pub fn largest_key(&self) -> &[u8] {
        &self.largest_key
    }

    pub fn has_bloom_filter(&self) -> bool {
        self.bloom_filter.is_some()
    }

    pub fn properties(&self) -> Option<&SstProperties> {
        self.properties.as_ref()
    }

    #[tracing::instrument(
    level = "debug",
    name = "sst_seek",
    skip(self, seek_key),
    fields(file_number = self.file_number, level = self.level)
  )]
    pub fn get(&self, seek_key: &[u8]) -> Result<Option<(Vec<u8>, ValueType)>> {
        let target_user = extract_user_key(seek_key);

        let bloom_passed = if let Some(ref filter) = self.bloom_filter {
            let hit = filter.may_contain(target_user);
            tracing::debug!(
              target: "sst",
              file_number = self.file_number,
              hit,
              "bloom_check"
            );
            let _span =
                tracing::trace_span!("bloom_check", file_number = self.file_number, hit).entered();
            if !hit {
                if let Some(ref s) = self.stats {
                    s.bloom_useful.fetch_add(1, AtomicOrdering::Relaxed);
                }
                tracing::debug!(target: "sst", found = false, "sst.seek.result");
                return Ok(None);
            }
            true
        } else {
            false
        };

        let handle = find_block_handle(&self.index_entries, seek_key)?;
        let block_data = read_block_cached(
            &self.file,
            self.file_number,
            &handle,
            self.block_cache.as_ref(),
            self.stats.as_ref(),
        )?;
        let block = Block::new(block_data)?;
        let mut it = block.iter();
        it.seek(seek_key);
        let seek_seq = extract_sequence(seek_key)?;

        while it.valid() {
            let key = it.key();
            if extract_user_key(key) != target_user {
                break;
            }
            let seq = extract_sequence(key)?;
            if seq <= seek_seq {
                let ty = extract_value_type(key)?;
                let val = it.value().to_vec();
                tracing::debug!(target: "sst", found = true, "sst.seek.result");
                return Ok(Some((val, ty)));
            }
            if !it.advance() {
                break;
            }
        }
        tracing::debug!(target: "sst", found = false, "sst.seek.result");
        if bloom_passed {
            if let Some(ref s) = self.stats {
                s.bloom_false_positive
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            record_bloom_false_positive();
        }
        Ok(None)
    }

    /// 点查只返回 `ValueType`, 不分配 value `Vec`.
    #[tracing::instrument(
    level = "debug",
    name = "sst_value_type",
    skip(self, seek_key),
    fields(file_number = self.file_number, level = self.level)
  )]
    pub fn value_type(&self, seek_key: &[u8]) -> Result<Option<ValueType>> {
        self.value_type_opt(seek_key, true)
    }

    /// 点查只返回 `ValueType`, 可选是否累加 stats 并经过 BlockCache.
    /// 当 `record_stats == false` 时 (写路径内部存在性检查), 绕过 BlockCache 且不累加 block_read_bytes / bloom 指标.
    pub fn value_type_opt(&self, seek_key: &[u8], record_stats: bool) -> Result<Option<ValueType>> {
        let target_user = extract_user_key(seek_key);

        let bloom_passed = if let Some(ref filter) = self.bloom_filter {
            let hit = filter.may_contain(target_user);
            tracing::debug!(
              target: "sst",
              file_number = self.file_number,
              hit,
              "bloom_check"
            );
            if !hit {
                if record_stats {
                    if let Some(ref s) = self.stats {
                        s.bloom_useful.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                }
                tracing::debug!(target: "sst", found = false, "sst.value_type.result");
                return Ok(None);
            }
            true
        } else {
            false
        };

        let handle = find_block_handle(&self.index_entries, seek_key)?;
        let (cache, stats) = if record_stats {
            (self.block_cache.as_ref(), self.stats.as_ref())
        } else {
            (None, None)
        };
        let block_data = read_block_cached(&self.file, self.file_number, &handle, cache, stats)?;
        let block = Block::new(block_data)?;
        let mut it = block.iter();
        it.seek(seek_key);
        let seek_seq = extract_sequence(seek_key)?;

        while it.valid() {
            let key = it.key();
            if extract_user_key(key) != target_user {
                break;
            }
            let seq = extract_sequence(key)?;
            if seq <= seek_seq {
                let ty = extract_value_type(key)?;
                tracing::debug!(target: "sst", found = true, "sst.value_type.result");
                return Ok(Some(ty));
            }
            if !it.advance() {
                break;
            }
        }
        tracing::debug!(target: "sst", found = false, "sst.value_type.result");
        if bloom_passed && record_stats {
            if let Some(ref s) = self.stats {
                s.bloom_false_positive
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            record_bloom_false_positive();
        }
        Ok(None)
    }

    /// 同 user_key 在 `max_seq` 下的 point 状态 (单文件内最新).
    pub fn point_state(&self, key: &[u8], max_seq: u64) -> Result<PointState> {
        self.point_state_opt(key, max_seq, true)
    }

    /// 同 user_key 在 `max_seq` 下的 point 状态, 可选是否累加 stats 并经过 BlockCache.
    pub fn point_state_opt(
        &self,
        key: &[u8],
        max_seq: u64,
        record_stats: bool,
    ) -> Result<PointState> {
        let seek_key = encode_internal_key(key, max_seq, ValueType::TypePut);
        let target_user = extract_user_key(&seek_key);
        let seek_seq = extract_sequence(&seek_key)?;

        let handle = match find_block_handle(&self.index_entries, &seek_key) {
            Ok(h) => h,
            Err(_) => return Ok(PointState::Absent),
        };
        let (cache, stats) = if record_stats {
            (self.block_cache.as_ref(), self.stats.as_ref())
        } else {
            (None, None)
        };
        let block_data = read_block_cached(&self.file, self.file_number, &handle, cache, stats)?;
        let block = Block::new(block_data)?;
        let mut it = block.iter();
        it.seek(&seek_key);
        let mut best = PointState::Absent;

        while it.valid() {
            let ik = it.key();
            if extract_user_key(ik) != target_user {
                break;
            }
            let seq = extract_sequence(ik)?;
            if seq <= seek_seq {
                let ty = extract_value_type(ik)?;
                let state = match ty {
                    ValueType::TypePut => PointState::Put(it.value().to_vec(), seq),
                    ValueType::TypeDelete => PointState::Delete(seq),
                    ValueType::TypeRangeDelete => PointState::Absent,
                };
                if let PointState::Put(_, s) | PointState::Delete(s) = &state {
                    let replace = match &best {
                        PointState::Put(_, bs) | PointState::Delete(bs) => s > bs,
                        PointState::Absent => true,
                    };
                    if replace {
                        best = state;
                    }
                }
            }
            if !it.advance() {
                break;
            }
        }
        Ok(best)
    }

    pub fn max_range_tombstone_seq(&self, user_key: &[u8], max_seq: u64) -> Option<u64> {
        self.range_tombstones
            .iter()
            .filter(|r| r.sequence <= max_seq && range_covers(&r.start, &r.end, user_key))
            .map(|r| r.sequence)
            .max()
    }

    pub fn has_range_tombstones(&self) -> bool {
        !self.range_tombstones.is_empty()
    }

    pub(crate) fn collect_range_tombstones(&self) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
        self.range_tombstones
            .iter()
            .map(|r| (r.start.clone(), r.end.clone(), r.sequence))
            .collect()
    }

    pub fn iter(&self) -> SSTableIterator {
        SSTableIterator::new(
            Arc::clone(&self.file),
            self.file_number,
            Arc::clone(&self.index_entries),
            self.block_cache.clone(),
            self.stats.clone(),
        )
    }

    pub fn iter_uncached(&self) -> SSTableIterator {
        SSTableIterator::new(
            Arc::clone(&self.file),
            self.file_number,
            Arc::clone(&self.index_entries),
            None,
            None,
        )
    }
}

fn load_bloom_filter(file: &File, meta_index: &IndexBlock) -> Option<BloomFilter> {
    let handle = match find_meta_block_handle(meta_index, BLOOM_META_NAME) {
        Ok(Some(h)) => h,
        Ok(None) => return None,
        Err(e) => {
            tracing::warn!(error = %e, "bloom meta index read failed, degraded to no-filter");
            return None;
        }
    };
    let raw = match read_raw_bytes(file, handle.offset, handle.size) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(error = %e, "bloom meta block read failed, degraded to no-filter");
            return None;
        }
    };
    match BloomFilter::decode(&raw) {
        Ok(f) => Some(f),
        Err(e) => {
            tracing::warn!(error = %e, "bloom decode failed, degraded to no-filter");
            None
        }
    }
}

fn load_properties(file: &File, meta_index: &IndexBlock) -> Option<SstProperties> {
    let handle = match find_meta_block_handle(meta_index, PROPERTIES_META_NAME) {
        Ok(Some(h)) => h,
        Ok(None) => return None,
        Err(e) => {
            tracing::warn!(error = %e, "properties meta index read failed");
            return None;
        }
    };
    let raw = match read_raw_bytes(file, handle.offset, handle.size) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(error = %e, "properties block read failed");
            return None;
        }
    };
    match SstProperties::decode(&raw) {
        Ok(p) => Some(p),
        Err(e) => {
            tracing::warn!(error = %e, "properties decode failed");
            None
        }
    }
}

fn read_footer(file: &File, file_size: u64) -> Result<Footer> {
    let mut buf = [0u8; FOOTER_SIZE];
    let mut f = file.try_clone()?;
    f.seek(SeekFrom::Start(file_size - FOOTER_SIZE as u64))?;
    f.read_exact(&mut buf)?;
    Footer::decode(&buf)
}

fn read_key_range(
    file: &File,
    file_number: u64,
    entries: &[(Vec<u8>, BlockHandle)],
    block_cache: Option<&Arc<BlockCache>>,
) -> Result<(Vec<u8>, Vec<u8>)> {
    if entries.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let first = read_block_cached(file, file_number, &entries[0].1, block_cache, None)?;
    let first_it = Block::new(first)?.iter();
    let smallest = if first_it.valid() {
        first_it.key().to_vec()
    } else {
        Vec::new()
    };

    let last = read_block_cached(
        file,
        file_number,
        &entries[entries.len() - 1].1,
        block_cache,
        None,
    )?;
    let block = Block::new(last)?;
    let mut it = block.iter();
    let mut largest = Vec::new();
    while it.valid() {
        largest = it.key().to_vec();
        if !it.advance() {
            break;
        }
    }
    Ok((smallest, largest))
}

fn load_range_tombstones(
    file: &File,
    file_number: u64,
    index_entries: Arc<Vec<(Vec<u8>, BlockHandle)>>,
) -> Result<Vec<RangeTombstoneEntry>> {
    let mut it = SSTableIterator::new(
        Arc::new(file.try_clone()?),
        file_number,
        index_entries,
        None,
        None,
    );
    let mut out = Vec::new();
    while it.valid() {
        let Some(ik) = it.key() else {
            break;
        };
        let Ok(value_type) = extract_value_type(ik) else {
            if !it.advance() {
                break;
            }
            continue;
        };
        if value_type == ValueType::TypeRangeDelete {
            let Some(end) = it.value() else {
                if !it.advance() {
                    break;
                }
                continue;
            };
            out.push(RangeTombstoneEntry {
                start: extract_user_key(ik).to_vec(),
                end: end.to_vec(),
                sequence: extract_sequence(ik)?,
            });
        }
        if !it.advance() {
            break;
        }
    }
    Ok(out)
}
