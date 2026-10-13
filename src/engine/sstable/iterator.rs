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
use std::sync::Arc;

use crate::engine::cache::BlockCache;
use crate::engine::memtable::compare_internal_key;
use crate::statistics::Statistics;

use super::block::{Block, BlockIterator};
use super::block_io::read_block_cached;
use super::handle::BlockHandle;

pub struct SSTableIterator {
    file: Arc<File>,
    file_number: u64,
    index_entries: Arc<Vec<(Vec<u8>, BlockHandle)>>,
    block_cache: Option<Arc<BlockCache>>,
    stats: Option<Arc<Statistics>>,
    block_index: usize,
    block_iter: Option<BlockIterator>,
    valid: bool,
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
        }
    }

    pub fn seek_to_first(&mut self) {
        self.block_index = 0;
        self.load_block();
    }

    pub fn seek_to_target(&mut self, target: &[u8]) {
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
        let data = match read_block_cached(
            &self.file,
            self.file_number,
            &handle,
            self.block_cache.as_ref(),
            self.stats.as_ref(),
        ) {
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
}
