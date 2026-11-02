//! RowCache: 基于分片并发 LRU 的行级缓存 (ADR-007).
//! 为高频点查提供数十纳秒级极速响应, 绕过 MemTable / BlockCache / SST 解码.

use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;

use super::block_cache::CacheStats;

const NUM_SHARDS: usize = 32;
const SHARD_MASK: usize = NUM_SHARDS - 1;
const LRU_QUEUE_COMPACT_MULTIPLIER: usize = 4;
const LRU_QUEUE_COMPACT_MIN: usize = 256;

#[inline]
fn shard_index(key: &[u8]) -> usize {
    let mut hasher = rustc_hash::FxHasher::default();
    key.hash(&mut hasher);
    (hasher.finish() as usize) & SHARD_MASK
}

struct ShardInner {
    cache: HashMap<Vec<u8>, (Bytes, u64, u64)>, // (value, seq, counter)
    lru_queue: VecDeque<(Vec<u8>, u64)>,        // (key, counter)
    next_counter: u64,
    total_bytes: usize,
    max_bytes: usize,
}

impl ShardInner {
    fn new(max_bytes: usize) -> Self {
        Self {
            cache: HashMap::new(),
            lru_queue: VecDeque::new(),
            next_counter: 0,
            total_bytes: 0,
            max_bytes,
        }
    }

    fn maybe_compact_queue(&mut self) {
        let alive = self.cache.len();
        if self.lru_queue.len() >= LRU_QUEUE_COMPACT_MIN
            && self.lru_queue.len() > alive * LRU_QUEUE_COMPACT_MULTIPLIER
        {
            let mut new_queue = VecDeque::with_capacity(alive);
            for (key, counter) in self.lru_queue.drain(..) {
                if let Some((_, _, cur_counter)) = self.cache.get(&key) {
                    if *cur_counter == counter {
                        new_queue.push_back((key, counter));
                    }
                }
            }
            self.lru_queue = new_queue;
        }
    }

    fn evict_excess(&mut self, stats_evictions: &AtomicU64) {
        while self.total_bytes > self.max_bytes && !self.cache.is_empty() {
            if let Some((key, counter)) = self.lru_queue.pop_front() {
                if let Some((_, _, cur_counter)) = self.cache.get(&key) {
                    if *cur_counter == counter {
                        if let Some((val, _, _)) = self.cache.remove(&key) {
                            let entry_size = key.len() + val.len() + 64;
                            self.total_bytes = self.total_bytes.saturating_sub(entry_size);
                            stats_evictions.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            } else {
                break;
            }
        }
    }
}

/// 分片并发 RowCache
pub struct RowCache {
    shards: Vec<Mutex<ShardInner>>,
    capacity: usize,
    stats_lookups: AtomicU64,
    stats_hits: AtomicU64,
    stats_misses: AtomicU64,
    stats_insertions: AtomicU64,
    stats_evictions: AtomicU64,
}

impl RowCache {
    /// 创建指定容量 (字节) 的分片 RowCache (0 表示禁用)
    pub fn new(capacity: usize) -> Self {
        let shard_cap = if capacity == 0 {
            0
        } else {
            capacity.div_ceil(NUM_SHARDS)
        };
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(Mutex::new(ShardInner::new(shard_cap)));
        }

        Self {
            shards,
            capacity,
            stats_lookups: AtomicU64::new(0),
            stats_hits: AtomicU64::new(0),
            stats_misses: AtomicU64::new(0),
            stats_insertions: AtomicU64::new(0),
            stats_evictions: AtomicU64::new(0),
        }
    }

    /// 查询行级缓存 (仅当缓存版本 seq <= max_seq 时可见并命中)
    pub fn get(&self, key: &[u8], max_seq: u64) -> Option<Bytes> {
        if self.capacity == 0 {
            return None;
        }
        self.stats_lookups.fetch_add(1, Ordering::Relaxed);
        let idx = shard_index(key);
        let mut shard = self.shards[idx].lock();

        let hit = if let Some((val, seq, _)) = shard.cache.get(key) {
            if *seq <= max_seq {
                Some(val.clone())
            } else {
                None
            }
        } else {
            None
        };

        if let Some(val) = hit {
            self.stats_hits.fetch_add(1, Ordering::Relaxed);
            shard.next_counter += 1;
            let c = shard.next_counter;
            if let Some((_, _, counter)) = shard.cache.get_mut(key) {
                *counter = c;
            }
            shard.lru_queue.push_back((key.to_vec(), c));
            shard.maybe_compact_queue();
            return Some(val);
        }

        self.stats_misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// 插入或回填缓存条目
    pub fn insert(&self, key: &[u8], value: Bytes, seq: u64) {
        if self.capacity == 0 {
            return;
        }
        let entry_size = key.len() + value.len() + 64;
        let idx = shard_index(key);
        let mut shard = self.shards[idx].lock();

        if entry_size > shard.max_bytes {
            return;
        }

        if let Some((old_val, _, _)) = shard.cache.remove(key) {
            let old_size = key.len() + old_val.len() + 64;
            shard.total_bytes = shard.total_bytes.saturating_sub(old_size);
        }

        shard.total_bytes += entry_size;
        shard.next_counter += 1;
        let c = shard.next_counter;
        shard.cache.insert(key.to_vec(), (value, seq, c));
        shard.lru_queue.push_back((key.to_vec(), c));
        shard.evict_excess(&self.stats_evictions);
        shard.maybe_compact_queue();
        self.stats_insertions.fetch_add(1, Ordering::Relaxed);
    }

    /// 使指定 key 的行级缓存失效 (写路径调用)
    pub fn invalidate(&self, key: &[u8]) {
        if self.capacity == 0 {
            return;
        }
        let idx = shard_index(key);
        let mut shard = self.shards[idx].lock();
        if let Some((old_val, _, _)) = shard.cache.remove(key) {
            let entry_size = key.len() + old_val.len() + 64;
            shard.total_bytes = shard.total_bytes.saturating_sub(entry_size);
        }
    }

    /// 全量清空 RowCache (delete_range 等大范围清空时调用)
    pub fn clear(&self) {
        for shard in &self.shards {
            let mut s = shard.lock();
            s.cache.clear();
            s.lru_queue.clear();
            s.total_bytes = 0;
        }
    }

    /// 获取缓存性能指标快照
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            lookups: self.stats_lookups.load(Ordering::Relaxed),
            hits: self.stats_hits.load(Ordering::Relaxed),
            misses: self.stats_misses.load(Ordering::Relaxed),
            insertions: self.stats_insertions.load(Ordering::Relaxed),
            evictions: self.stats_evictions.load(Ordering::Relaxed),
        }
    }
}
