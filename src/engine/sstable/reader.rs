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

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use parking_lot::Mutex;

use crate::engine::cache::{BlockCache, TableCache};
use crate::engine::filter::{bloom::record_bloom_false_positive, BloomFilter, Filter};
use crate::engine::memtable::{
    encode_internal_key, extract_sequence, extract_user_key, extract_value_type, range_covers,
    range_overlaps, PointState, ValueType,
};
use crate::error::{Error, Result};
use crate::statistics::Statistics;

use super::block::Block;
use super::block_io::{read_block_cached_with_id, read_block_from_file};
use super::footer::{Footer, FOOTER_SIZE};
use super::handle::BlockHandle;
use super::index::{find_block_handle, load_index_entries, IndexBlock};
use super::iterator::SSTableIterator;
use super::meta::{
    decode_range_tombstones, find_meta_block_handle, read_raw_bytes, BLOOM_META_NAME,
    PROPERTIES_META_NAME, RANGE_TOMBSTONES_META_NAME,
};
use super::properties::SstProperties;

#[derive(Clone, Debug)]
struct RangeTombstoneEntry {
    start: Vec<u8>,
    end: Vec<u8>,
    sequence: u64,
}

fn cache_id_for_dir(dir: &Path) -> u64 {
    static NEXT_CACHE_ID: AtomicU64 = AtomicU64::new(1);
    static DIR_IDS: OnceLock<Mutex<HashMap<PathBuf, u64>>> = OnceLock::new();
    let map = DIR_IDS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock();
    if let Some(&id) = guard.get(dir) {
        return id;
    }
    let id = NEXT_CACHE_ID.fetch_add(1, AtomicOrdering::Relaxed);
    guard.insert(dir.to_path_buf(), id);
    id
}

pub struct SSTableReader {
    file: Option<Arc<File>>,
    path: PathBuf,
    cache_id: u64,
    table_cache: Option<Arc<TableCache>>,
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
        Self::open_with_caches(path, block_cache, None, stats)
    }

    pub fn open_with_caches(
        path: &Path,
        block_cache: Option<Arc<BlockCache>>,
        table_cache: Option<Arc<TableCache>>,
        stats: Option<Arc<Statistics>>,
    ) -> Result<Self> {
        let (file_number, level) = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(super::filename::parse_sstable_filename)
            .unwrap_or((0, 0));
        let cache_id = path.parent().map(cache_id_for_dir).unwrap_or(0);
        let file = if let Some(ref tc) = table_cache {
            tc.get_or_open(cache_id, file_number, path)?
        } else {
            Arc::new(File::open(path)?)
        };
        let file_size = file.metadata()?.len();
        if file_size < FOOTER_SIZE as u64 {
            return Err(Error::Corruption("SSTable file too small".into()));
        }

        let footer = read_footer(&file, file_size)?;
        let index_bytes = read_block_from_file(&file, &footer.index_handle)?;
        let index_block = Block::new(Bytes::from(index_bytes))?;
        let index_entries = Arc::new(load_index_entries(&index_block)?);

        let meta_index_bytes = read_block_from_file(&file, &footer.meta_index_handle)?;
        let meta_index = IndexBlock::new(Bytes::from(meta_index_bytes))?;
        let bloom_filter = load_bloom_filter(&file, &meta_index);
        let properties = load_properties_required(&file, &meta_index)?;

        let smallest_key = properties.smallest_key.clone();
        let largest_key = properties.largest_key.clone();

        let rt_handle = find_meta_block_handle(&meta_index, RANGE_TOMBSTONES_META_NAME)?
            .ok_or_else(|| Error::Corruption("missing range_tombstones meta block".into()))?;
        let rt_raw = read_raw_bytes(&file, rt_handle.offset, rt_handle.size)?;
        let range_tombstones_vec: Vec<RangeTombstoneEntry> = decode_range_tombstones(&rt_raw)?
            .into_iter()
            .map(|(start, end, sequence)| RangeTombstoneEntry {
                start,
                end,
                sequence,
            })
            .collect();
        let range_tombstones = Arc::new(range_tombstones_vec);
        let stored_file = if table_cache.is_some() {
            None
        } else {
            Some(file)
        };

        Ok(Self {
            file: stored_file,
            path: path.to_path_buf(),
            cache_id,
            table_cache,
            file_number,
            level,
            index_entries,
            file_size,
            smallest_key,
            largest_key,
            bloom_filter,
            properties: Some(properties),
            block_cache,
            range_tombstones,
            stats,
        })
    }

    pub fn index_size(&self) -> u64 {
        self.properties.as_ref().map_or(0, |p| p.index_size)
    }

    #[inline]
    fn acquire_file(&self) -> Result<Arc<File>> {
        if let Some(ref f) = self.file {
            return Ok(Arc::clone(f));
        }
        if let Some(ref tc) = self.table_cache {
            return tc.get_or_open(self.cache_id, self.file_number, &self.path);
        }
        Ok(Arc::new(File::open(&self.path)?))
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

    pub fn get(&self, seek_key: &[u8]) -> Result<Option<(Vec<u8>, ValueType)>> {
        let target_user = extract_user_key(seek_key);

        let bloom_passed = if let Some(ref filter) = self.bloom_filter {
            let hit = filter.may_contain(target_user);
            if !hit {
                if let Some(ref s) = self.stats {
                    s.bloom_useful.fetch_add(1, AtomicOrdering::Relaxed);
                }
                return Ok(None);
            }
            true
        } else {
            false
        };

        let handle = find_block_handle(&self.index_entries, seek_key)?;
        let file = self.acquire_file()?;
        let block_data = read_block_cached_with_id(
            self.cache_id,
            &file,
            self.file_number,
            &handle,
            self.block_cache.as_ref(),
            self.stats.as_ref(),
        )?;
        let block = Block::new(block_data)?;
        let mut it = block.iter();
        it.seek_point(seek_key);
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
                return Ok(Some((val, ty)));
            }
            if !it.advance() {
                break;
            }
        }
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
    pub fn value_type(&self, seek_key: &[u8]) -> Result<Option<ValueType>> {
        self.value_type_opt(seek_key, true)
    }

    /// 点查只返回 `ValueType`, 可选是否累加 stats 并经过 BlockCache.
    /// 当 `record_stats == false` 时 (写路径内部存在性检查), 绕过 BlockCache 且不累加 block_read_bytes / bloom 指标.
    pub fn value_type_opt(&self, seek_key: &[u8], record_stats: bool) -> Result<Option<ValueType>> {
        let target_user = extract_user_key(seek_key);

        let bloom_passed = if let Some(ref filter) = self.bloom_filter {
            let hit = filter.may_contain(target_user);
            if !hit {
                if record_stats {
                    if let Some(ref s) = self.stats {
                        s.bloom_useful.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                }
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
        let file = self.acquire_file()?;
        let block_data = read_block_cached_with_id(
            self.cache_id,
            &file,
            self.file_number,
            &handle,
            cache,
            stats,
        )?;
        let block = Block::new(block_data)?;
        let mut it = block.iter();
        it.seek_point(seek_key);
        let seek_seq = extract_sequence(seek_key)?;

        while it.valid() {
            let key = it.key();
            if extract_user_key(key) != target_user {
                break;
            }
            let seq = extract_sequence(key)?;
            if seq <= seek_seq {
                let ty = extract_value_type(key)?;
                return Ok(Some(ty));
            }
            if !it.advance() {
                break;
            }
        }
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
        let file = self.acquire_file()?;
        let block_data = read_block_cached_with_id(
            self.cache_id,
            &file,
            self.file_number,
            &handle,
            cache,
            stats,
        )?;
        let block = Block::new(block_data)?;
        let mut it = block.iter();
        it.seek_point(&seek_key);
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

    /// 只克隆与 `[start, end)` 相交的 range tombstone (`None` 表示无界).
    pub(crate) fn collect_range_tombstones_overlapping(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
        self.range_tombstones
            .iter()
            .filter(|r| range_overlaps(&r.start, &r.end, start, end))
            .map(|r| (r.start.clone(), r.end.clone(), r.sequence))
            .collect()
    }

    pub fn iter(&self) -> SSTableIterator {
        let file = self
            .acquire_file()
            .expect("SSTableReader::iter failed to open SSTable file");
        SSTableIterator::new(
            file,
            self.file_number,
            Arc::clone(&self.index_entries),
            self.block_cache.clone(),
            self.stats.clone(),
        )
    }

    /// 未定位的迭代器, 调用方必须先 seek (见 `SSTableIterator::new_unpositioned`).
    pub(crate) fn iter_unpositioned(&self) -> SSTableIterator {
        let file = self
            .acquire_file()
            .expect("SSTableReader::iter_unpositioned failed to open SSTable file");
        SSTableIterator::new_unpositioned(
            file,
            self.file_number,
            Arc::clone(&self.index_entries),
            self.block_cache.clone(),
            self.stats.clone(),
        )
    }

    pub fn iter_uncached(&self) -> SSTableIterator {
        let file = self
            .acquire_file()
            .expect("SSTableReader::iter_uncached failed to open SSTable file");
        SSTableIterator::new(
            file,
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

fn load_properties_required(file: &File, meta_index: &IndexBlock) -> Result<SstProperties> {
    let handle = find_meta_block_handle(meta_index, PROPERTIES_META_NAME)?
        .ok_or_else(|| Error::Corruption("missing properties meta block".into()))?;
    let raw = read_raw_bytes(file, handle.offset, handle.size)?;
    SstProperties::decode(&raw)
}

fn read_footer(file: &File, file_size: u64) -> Result<Footer> {
    let mut buf = [0u8; FOOTER_SIZE];
    super::block_io::read_exact_at(file, &mut buf, file_size - FOOTER_SIZE as u64)?;
    Footer::decode(&buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompressionType;
    use crate::engine::sstable::{sstable_path, SSTableBuilder};
    use tempfile::tempdir;

    #[test]
    fn open_with_stats_reads_zero_data_blocks() {
        let dir = tempdir().unwrap();
        let path = sstable_path(dir.path(), 1, 0);
        let mut b = SSTableBuilder::new(&path, 256, 4, CompressionType::None, 0.01).unwrap();
        for i in 0..50u32 {
            let k = encode_internal_key(format!("key-{i:04}").as_bytes(), 1, ValueType::TypePut);
            b.add(&k, b"value-payload-1234567890").unwrap();
        }
        b.finish().unwrap();

        let stats = Arc::new(Statistics::default());
        let reader = SSTableReader::open_with_stats(&path, None, Some(Arc::clone(&stats))).unwrap();
        assert_eq!(stats.block_read_bytes.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(
            reader.smallest_key(),
            &encode_internal_key(b"key-0000", 1, ValueType::TypePut)
        );
        assert_eq!(
            reader.largest_key(),
            &encode_internal_key(b"key-0049", 1, ValueType::TypePut)
        );
        assert!(reader.index_size() > 0);
    }
}
