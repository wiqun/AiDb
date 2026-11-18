//! TableCache: 分片 LRU SSTable 文件描述符 (`Arc<File>`) 缓存.
//!
//! 落地 `Options::max_open_files` 限制, 防止多 Group 集群打开大量 SST 时耗尽系统 fd.
//! 索引/Bloom 元数据仍驻留在 `SSTableReader` 中, 仅淘汰底层 `Arc<File>`; 淘汰后
//! 再次访问由 `TableCache` 按原路径透明重开.

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::error::Result;

const NUM_SHARDS: usize = 16;
const SHARD_MASK: usize = NUM_SHARDS - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TableKey {
    cache_id: u64,
    file_number: u64,
}

#[inline]
fn shard_index(key: &TableKey) -> usize {
    let mut hasher = rustc_hash::FxHasher::default();
    key.hash(&mut hasher);
    (hasher.finish() as usize) & SHARD_MASK
}

struct ShardInner {
    map: HashMap<TableKey, (Arc<File>, u64)>,
    lru: VecDeque<(TableKey, u64)>,
    next_counter: u64,
    max_entries: usize,
}

impl ShardInner {
    fn new(max_entries: usize) -> Self {
        Self {
            map: HashMap::new(),
            lru: VecDeque::new(),
            next_counter: 0,
            max_entries,
        }
    }

    fn evict_if_needed(&mut self, open_count: &AtomicUsize) {
        while self.map.len() > self.max_entries {
            let Some((key, counter)) = self.lru.pop_front() else {
                break;
            };
            if let Some((_, cur_counter)) = self.map.get(&key) {
                if *cur_counter == counter {
                    self.map.remove(&key);
                    open_count.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
        if self.lru.len() >= 128 && self.lru.len() > self.map.len() * 4 {
            let mut compacted = VecDeque::with_capacity(self.map.len());
            for (k, c) in self.lru.drain(..) {
                if let Some((_, cur)) = self.map.get(&k) {
                    if *cur == c {
                        compacted.push_back((k, c));
                    }
                }
            }
            self.lru = compacted;
        }
    }
}

/// 分片 LRU 打开文件句柄缓存.
pub struct TableCache {
    shards: Vec<Mutex<ShardInner>>,
    max_open_files: usize,
    cached_count: AtomicUsize,
}

impl std::fmt::Debug for TableCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableCache")
            .field("max_open_files", &self.max_open_files)
            .field("cached_files", &self.cached_files())
            .finish()
    }
}

impl TableCache {
    /// 创建容量为 `max_open_files` 的 `TableCache` (`0` 表示至少保留 1 个或禁用缓存直接打开).
    pub fn new(max_open_files: usize) -> Self {
        let per_shard = if max_open_files == 0 {
            0
        } else {
            max_open_files.div_ceil(NUM_SHARDS).max(1)
        };
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(Mutex::new(ShardInner::new(per_shard)));
        }
        Self {
            shards,
            max_open_files,
            cached_count: AtomicUsize::new(0),
        }
    }

    /// 获取或打开指定 SST 文件的 `Arc<File>`.
    pub fn get_or_open(&self, cache_id: u64, file_number: u64, path: &Path) -> Result<Arc<File>> {
        if self.max_open_files == 0 {
            return Ok(Arc::new(File::open(path)?));
        }
        let key = TableKey {
            cache_id,
            file_number,
        };
        let idx = shard_index(&key);
        {
            let mut shard = self.shards[idx].lock();
            if let Some((file, _)) = shard.map.get(&key) {
                let file = Arc::clone(file);
                shard.next_counter += 1;
                let c = shard.next_counter;
                shard.map.get_mut(&key).unwrap().1 = c;
                shard.lru.push_back((key, c));
                return Ok(file);
            }
        }

        let file = Arc::new(File::open(path)?);
        let mut shard = self.shards[idx].lock();
        if let Some((existing, _)) = shard.map.get(&key) {
            return Ok(Arc::clone(existing));
        }
        shard.next_counter += 1;
        let c = shard.next_counter;
        shard.map.insert(key, (Arc::clone(&file), c));
        shard.lru.push_back((key, c));
        self.cached_count.fetch_add(1, Ordering::Relaxed);
        shard.evict_if_needed(&self.cached_count);
        Ok(file)
    }

    /// 主动驱逐指定 SST 的缓存句柄 (例如文件被删除时).
    pub fn evict(&self, cache_id: u64, file_number: u64) {
        if self.max_open_files == 0 {
            return;
        }
        let key = TableKey {
            cache_id,
            file_number,
        };
        let idx = shard_index(&key);
        let mut shard = self.shards[idx].lock();
        if shard.map.remove(&key).is_some() {
            self.cached_count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// 当前缓存中保留的文件句柄总数.
    pub fn cached_files(&self) -> usize {
        self.cached_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn table_cache_evicts_handles_and_reopens_transparently() {
        let dir = tempdir().unwrap();
        // max_open_files = 16 -> 1 entry per shard across 16 shards
        let tc = TableCache::new(16);
        let mut paths = Vec::new();
        for i in 0..48u64 {
            let p = dir.path().join(format!("{i:06}.sst"));
            let mut f = File::create(&p).unwrap();
            writeln!(f, "data-{i}").unwrap();
            paths.push(p);
        }
        for (i, p) in paths.iter().enumerate() {
            let _ = tc.get_or_open(1, i as u64, p).unwrap();
        }
        assert!(tc.cached_files() <= 16, "cached {}", tc.cached_files());

        // Re-opening an evicted file succeeds transparently
        let f0 = tc.get_or_open(1, 0, &paths[0]).unwrap();
        assert!(f0.metadata().unwrap().len() > 0);
    }
}
