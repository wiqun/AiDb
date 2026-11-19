//! SSTableIterator — 单文件全量顺序迭代, 以 Data Block 为粒度 seek.
//!
//! # 迭代方式
//!
//! - 内部持有 Index entries (`(max_key, BlockHandle)`); 块内用 `BlockIterator` 扫描.
//! - `seek_to_target`: 先在 Index 上二分定位目标 block, 再在 block 内 `seek`.
//! - 跨块: 当前块耗尽后按序加载下一 block (经 `read_block_cached`), 支持 prev / seek_to_last.
//!
//! 仅做单文件遍历; 跨文件归并见 `db/iterator.rs` (读路径) 与 `compaction/merge.rs` (compaction).

use std::cmp::Ordering;
use std::fs::File;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::Arc;

use bytes::Bytes;

use crate::engine::cache::BlockCache;
use crate::engine::memtable::compare_internal_key;
use crate::statistics::Statistics;

use super::block::{Block, BlockIterator};
use super::block_io::{parse_block_bytes, read_block_cached, read_exact_at};
use super::handle::BlockHandle;

const INITIAL_READAHEAD_SIZE: usize = 16 * 1024;
const MAX_READAHEAD_SIZE: usize = 256 * 1024;
const READAHEAD_TRIGGER_BLOCKS: u32 = 2;

#[derive(Default)]
struct ReadaheadState {
    consecutive_forward_blocks: u32,
    readahead_size: usize,
    buf_offset: u64,
    buf: Vec<u8>,
}

impl ReadaheadState {
    fn reset(&mut self) {
        self.consecutive_forward_blocks = 0;
        self.readahead_size = 0;
        self.buf_offset = 0;
        self.buf.clear();
    }

    fn slice(&self, offset: u64, size: u64) -> Option<&[u8]> {
        if offset < self.buf_offset {
            return None;
        }
        let rel = (offset - self.buf_offset) as usize;
        let end = rel.checked_add(size as usize)?;
        if end <= self.buf.len() {
            Some(&self.buf[rel..end])
        } else {
            None
        }
    }
}

pub struct SSTableIterator {
    file: Arc<File>,
    file_number: u64,
    index_entries: Arc<Vec<(Vec<u8>, BlockHandle)>>,
    block_cache: Option<Arc<BlockCache>>,
    stats: Option<Arc<Statistics>>,
    block_index: usize,
    block_iter: Option<BlockIterator>,
    valid: bool,
    readahead: ReadaheadState,
}

impl SSTableIterator {
    pub(crate) fn new(
        file: Arc<File>,
        file_number: u64,
        index_entries: Arc<Vec<(Vec<u8>, BlockHandle)>>,
        block_cache: Option<Arc<BlockCache>>,
        stats: Option<Arc<Statistics>>,
    ) -> Self {
        let mut it = Self::new_unpositioned(file, file_number, index_entries, block_cache, stats);
        it.seek_to_first();
        it
    }

    /// 不加载任何 block 的迭代器 (`valid() == false`), 调用方需先 seek 再使用.
    /// 用于读路径惰性构造: 避免为随后会被 seek 掉的位置读取首块.
    pub(crate) fn new_unpositioned(
        file: Arc<File>,
        file_number: u64,
        index_entries: Arc<Vec<(Vec<u8>, BlockHandle)>>,
        block_cache: Option<Arc<BlockCache>>,
        stats: Option<Arc<Statistics>>,
    ) -> Self {
        Self {
            file,
            file_number,
            index_entries,
            block_cache,
            stats,
            block_index: 0,
            block_iter: None,
            valid: false,
            readahead: ReadaheadState::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn readahead_size_for_test(&self) -> usize {
        self.readahead.readahead_size
    }

    pub fn seek_to_first(&mut self) {
        self.readahead.reset();
        self.block_index = 0;
        self.load_block();
    }

    pub fn seek_to_target(&mut self, target: &[u8]) {
        self.readahead.reset();
        if self.index_entries.is_empty() {
            self.valid = false;
            return;
        }
        let last_key = &self.index_entries[self.index_entries.len() - 1].0;
        if compare_internal_key(target, last_key) == Ordering::Greater {
            self.valid = false;
            self.block_iter = None;
            return;
        }

        let mut lo = 0usize;
        let mut hi = self.index_entries.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if compare_internal_key(&self.index_entries[mid].0, target) == Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        self.block_index = if lo < self.index_entries.len() {
            lo
        } else {
            self.index_entries.len() - 1
        };
        self.load_block();
        if let Some(ref mut bit) = self.block_iter {
            bit.seek(target);
            self.valid = bit.valid();
        }
    }

    pub fn advance(&mut self) -> bool {
        if let Some(ref mut bit) = self.block_iter {
            if bit.advance() {
                self.valid = true;
                return true;
            }
        }
        if self.block_index + 1 >= self.index_entries.len() {
            self.valid = false;
            return false;
        }
        self.readahead.consecutive_forward_blocks =
            self.readahead.consecutive_forward_blocks.saturating_add(1);
        self.block_index += 1;
        self.load_block();
        if let Some(ref mut bit) = self.block_iter {
            if bit.valid() {
                self.valid = true;
                return true;
            }
            return self.advance();
        }
        self.valid = false;
        false
    }

    /// 反向移动一步.
    pub fn prev(&mut self) -> bool {
        if !self.valid {
            return false;
        }

        // Try prev in current block
        if let Some(ref mut bit) = self.block_iter {
            if bit.prev() {
                self.valid = true;
                return true;
            }
        }

        // Move to previous block and seek to its last entry
        if self.block_index == 0 {
            self.valid = false;
            return false;
        }
        self.readahead.reset();
        self.block_index -= 1;
        self.load_block();
        if let Some(ref mut bit) = self.block_iter {
            bit.seek_to_last();
            self.valid = bit.valid();
            return self.valid;
        }
        self.valid = false;
        false
    }

    /// 定位到最后一个 entry.
    pub fn seek_to_last(&mut self) {
        self.readahead.reset();
        if self.index_entries.is_empty() {
            self.valid = false;
            return;
        }
        self.block_index = self.index_entries.len() - 1;
        self.load_block();
        if let Some(ref mut bit) = self.block_iter {
            bit.seek_to_last();
            self.valid = bit.valid();
        }
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.block_iter
            .as_ref()
            .filter(|_| self.valid)
            .map(|b| b.key())
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.block_iter
            .as_ref()
            .filter(|_| self.valid)
            .map(|b| b.value())
    }

    fn load_block(&mut self) {
        if self.block_index >= self.index_entries.len() {
            self.valid = false;
            self.block_iter = None;
            return;
        }
        let handle = self.index_entries[self.block_index].1;
        let data = match self.read_block_with_readahead(&handle) {
            Ok(d) => d,
            Err(_) => {
                self.valid = false;
                self.block_iter = None;
                return;
            }
        };
        let block = match Block::new(data) {
            Ok(b) => b,
            Err(_) => {
                self.valid = false;
                self.block_iter = None;
                return;
            }
        };
        let bit = block.iter();
        let ok = bit.valid();
        self.block_iter = Some(bit);
        self.valid = ok;
    }

    fn read_block_with_readahead(&mut self, handle: &BlockHandle) -> crate::error::Result<Bytes> {
        if let Some(cache) = self.block_cache.as_ref() {
            if let Some(bytes) = cache.get_scoped(0, self.file_number, handle.offset) {
                if let Some(ref s) = self.stats {
                    s.block_seek_count.fetch_add(1, AtomicOrdering::Relaxed);
                }
                return Ok(bytes);
            }
        }

        if let Some(raw_slice) = self.readahead.slice(handle.offset, handle.size) {
            if let Some(ref s) = self.stats {
                s.block_seek_count.fetch_add(1, AtomicOrdering::Relaxed);
            }
            let bytes = Bytes::from(parse_block_bytes(raw_slice, handle)?);
            if let Some(cache) = self.block_cache.as_ref() {
                cache.insert_scoped(0, self.file_number, handle.offset, bytes.clone());
            }
            return Ok(bytes);
        }

        if self.readahead.consecutive_forward_blocks >= READAHEAD_TRIGGER_BLOCKS {
            let next_ra = if self.readahead.readahead_size == 0 {
                INITIAL_READAHEAD_SIZE
            } else {
                (self.readahead.readahead_size * 2).min(MAX_READAHEAD_SIZE)
            };
            self.readahead.readahead_size = next_ra;

            let target_len = next_ra.max(handle.size as usize);
            let start_offset = handle.offset;
            let mut end_offset = handle.offset + handle.size;
            for (_, h) in self.index_entries[self.block_index + 1..].iter() {
                if h.offset != end_offset {
                    break;
                }
                let candidate_len = (h.offset + h.size - start_offset) as usize;
                if candidate_len > target_len {
                    break;
                }
                end_offset = h.offset + h.size;
            }
            let total_len = (end_offset - start_offset) as usize;
            self.readahead.buf.resize(total_len, 0);
            read_exact_at(&self.file, &mut self.readahead.buf, start_offset)?;
            self.readahead.buf_offset = start_offset;
            if let Some(ref s) = self.stats {
                s.block_seek_count.fetch_add(1, AtomicOrdering::Relaxed);
                s.block_read_bytes
                    .fetch_add(total_len as u64, AtomicOrdering::Relaxed);
            }
            let bytes = Bytes::from(parse_block_bytes(
                &self.readahead.buf[..handle.size as usize],
                handle,
            )?);
            if let Some(cache) = self.block_cache.as_ref() {
                cache.insert_scoped(0, self.file_number, handle.offset, bytes.clone());
            }
            return Ok(bytes);
        }

        read_block_cached(
            &self.file,
            self.file_number,
            handle,
            self.block_cache.as_ref(),
            self.stats.as_ref(),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::config::CompressionType;
    use crate::engine::memtable::{encode_internal_key, ValueType};
    use crate::engine::sstable::{sstable_path, SSTableBuilder, SSTableReader};
    use tempfile::tempdir;

    #[test]
    fn iterator_sequential_readahead_and_seek_reset() {
        let dir = tempdir().unwrap();
        let path = sstable_path(dir.path(), 1, 0);
        let mut b = SSTableBuilder::new(&path, 256, 2, CompressionType::None, 0.0).unwrap();
        for i in 0..120u32 {
            let k = encode_internal_key(format!("k-{i:04}").as_bytes(), 1, ValueType::TypePut);
            b.add(&k, &[b'x'; 64]).unwrap();
        }
        b.finish().unwrap();

        let reader = SSTableReader::open(&path, None).unwrap();
        let mut it = reader.iter_uncached();
        let mut count = 0u32;
        while it.valid() {
            let expected =
                encode_internal_key(format!("k-{count:04}").as_bytes(), 1, ValueType::TypePut);
            assert_eq!(it.key().unwrap(), expected.as_slice());
            count += 1;
            if !it.advance() {
                break;
            }
        }
        assert_eq!(count, 120);
        assert!(it.readahead_size_for_test() >= 16 * 1024);

        let target = encode_internal_key(b"k-0055", 1, ValueType::TypePut);
        it.seek_to_target(&target);
        assert_eq!(it.readahead_size_for_test(), 0);
        assert!(it.valid());
        assert_eq!(it.key().unwrap(), target.as_slice());
    }
}
