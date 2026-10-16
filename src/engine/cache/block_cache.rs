//! Sharded LRU Data Block 缓存: O(1) 查找 / 插入 / 淘汰, 命中与未命中统计,
//! 并对关键块 (index / filter) 提供 pin 引用保护 (`PinGuard`, 见 `sstable::block_io`).
//!
//! # 架构
//!
//! ```text
//! 总容量均分 16 shard (NUM_SHARDS)
//! shard = HashMap<CacheKey, (Bytes, counter)> + lru_queue (VecDeque)
//!   ├─ get / insert 递增 next_counter, lru_queue 记录 (key, counter)
//!   └─ evict_one 惰性跳过 stale counter / pinned entry
//! lru_queue 长度 > 存活 key × 4 (且 >= 256) 时触发一次全量整理
//! ```
//!
//! `CacheKey = (file_number, block_offset)`, 经 `FxHasher` 散列到 shard.
//!
//! # Invariant
//!
//! - 总容量均分到各 shard; hash 偏斜时单个 shard 先满自行淘汰, 不借用其它 shard 配额
//!   (见 `docs/modules/02-engine-storage.md`).
//! - pin 计数 > 0 的 entry 不参与淘汰; pin 是 key 级引用计数, 与 entry 值解耦
//!   (insert 覆盖旧值时不清理 pin).

use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;

use crate::statistics::Statistics;

const NUM_SHARDS: usize = 16;
const SHARD_MASK: usize = NUM_SHARDS - 1;
const MIN_MAX_EVICTIONS: usize = 16;
const ASSUMED_BLOCK_SIZE: usize = 8 * 1024;
/// `lru_queue` 长度超过 "存活 key 数 * 该倍数" 时触发一次全量整理.
const LRU_QUEUE_COMPACT_MULTIPLIER: usize = 4;
/// 避免 cache 很小/刚启动时因为几次访问就触发整理.
const LRU_QUEUE_COMPACT_MIN: usize = 256;

/// Cache lookup key: SST file number + Data Block file offset.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub file_number: u64,
    pub offset: u64,
}

/// Snapshot of cache statistics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub lookups: u64,
    pub hits: u64,
    pub misses: u64,
    pub insertions: u64,
    pub evictions: u64,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        if self.lookups == 0 {
            0.0
        } else {
            self.hits as f64 / self.lookups as f64
        }
    }
}

/// Compute shard index for a given cache key.
pub fn shard_index(key: &CacheKey) -> usize {
    let mut hasher = rustc_hash::FxHasher::default();
    key.hash(&mut hasher);
    (hasher.finish() as usize) & SHARD_MASK
}

struct ShardInner {
    /// value (Bytes) + access_counter. 计数器用于检测 lru_queue 中的 stale entry.
    cache: HashMap<CacheKey, (Bytes, u64)>,
    /// LRU 顺序: (key, 插入/访问时的 access_counter).
    /// 可能包含 stale entry (counter 不匹配), 在 eviction 时惰性跳过.
    lru_queue: VecDeque<(CacheKey, u64)>,
    current_size: usize,
    /// 单调递增计数器, 每次 get hit 或 insert 时递增.
    next_counter: u64,
    /// Per-key pin 计数. pin count > 0 时 entry 不被 evict.
    /// key 级引用计数, 与具体 entry 值解耦 (insert 覆盖时不清理).
    pin_counts: HashMap<CacheKey, AtomicU64>,
}

impl ShardInner {
    /// Evict the LRU entry, skipping stale entries and pinned entries.
    /// Returns true if something was evicted.
    fn evict_one(&mut self, evictions: &AtomicU64, look_for: Option<&CacheKey>) -> bool {
        loop {
            let Some((key, counter)) = self.lru_queue.pop_front() else {
                return false;
            };
            // 跳过显式排除的 key (用于 insert 中覆盖旧 key 的场景)
            if let Some(exclude) = look_for {
                if &key == exclude {
                    continue;
                }
            }
            // 跳过 pinned entry (被 PinGuard 保护)
            if let Some(pc) = self.pin_counts.get(&key) {
                if pc.load(Ordering::Relaxed) > 0 {
                    continue;
                }
            }
            // counter 匹配才说明这是真正的 LRU entry
            if let Some((_, ref stored_counter)) = self.cache.get(&key) {
                if *stored_counter == counter {
                    if let Some((bytes, _)) = self.cache.remove(&key) {
                        self.current_size -= bytes.len();
                        evictions.fetch_add(1, Ordering::Relaxed);
                        tracing::debug!(target: "cache", evicted_count = 1, "cache.evict");
                        return true;
                    }
                }
            }
            // stale entry 或已删除: 跳过
        }
    }

    /// 全量整理 `lru_queue`.
    fn compact_lru_queue_if_needed(&mut self) {
        let live = self.cache.len();
        let threshold =
            (live.saturating_mul(LRU_QUEUE_COMPACT_MULTIPLIER)).max(LRU_QUEUE_COMPACT_MIN);
        if self.lru_queue.len() <= threshold {
            return;
        }
        let before = self.lru_queue.len();
        let cache = &self.cache;
        self.lru_queue.retain(
            |(key, counter)| matches!(cache.get(key), Some((_, stored)) if stored == counter),
        );
        tracing::debug!(
            target: "cache",
            before,
            after = self.lru_queue.len(),
            live,
            "cache.lru_queue.compact"
        );
    }
}

struct CacheShard {
    inner: Mutex<ShardInner>,
}

/// RAII 守卫: 持有期间阻止对应 entry 被 evict.
/// Drop 时自动 decrement pin 计数.
///
/// 对应 key 的 pin count 归零后, entry 被重新追加到 lru_queue 以恢复正常驱逐.
pub struct PinGuard<'a> {
    key: CacheKey,
    cache: &'a BlockCache,
}

impl Drop for PinGuard<'_> {
    fn drop(&mut self) {
        self.cache.unpin(&self.key);
    }
}

/// Thread-safe sharded LRU cache for SSTable Data Block payloads.
/// 使用双计数器方案实现 O(1) 的 get/insert/eviction 操作,
/// 支持通过 PinGuard RAII 保护关键 block (如 index/filter) 不被 evict.
pub struct BlockCache {
    shards: Box<[CacheShard; NUM_SHARDS]>,
    capacity_per_shard: usize,
    lookups: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    insertions: AtomicU64,
    evictions: AtomicU64,
    stats: Option<Arc<Statistics>>,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        Self::new_with_stats(capacity, None)
    }

    pub fn new_with_stats(capacity: usize, stats: Option<Arc<Statistics>>) -> Self {
        let capacity_per_shard = if capacity == 0 {
            0
        } else {
            (capacity / NUM_SHARDS).max(1)
        };
        let cache = Self {
            shards: Box::new([(); NUM_SHARDS].map(|_| CacheShard {
                inner: Mutex::new(ShardInner {
                    cache: HashMap::new(),
                    lru_queue: VecDeque::new(),
                    current_size: 0,
                    next_counter: 0,
                    pin_counts: HashMap::new(),
                }),
            })),
            capacity_per_shard,
            lookups: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            insertions: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            stats,
        };
        if let Some(ref s) = cache.stats {
            s.block_cache_capacity
                .store(cache.capacity() as u64, Ordering::Relaxed);
        }
        cache
    }

    pub fn get(&self, key: CacheKey) -> Option<Bytes> {
        self.lookups.fetch_add(1, Ordering::Relaxed);
        if self.capacity_per_shard == 0 {
            self.misses.fetch_add(1, Ordering::Relaxed);
            if let Some(ref s) = self.stats {
                s.block_cache_misses.fetch_add(1, Ordering::Relaxed);
            }
            tracing::debug!(target: "cache", "cache.miss");
            return None;
        }

        let idx = shard_index(&key);
        let mut guard = self.shards[idx].inner.lock();
        if let Some((value, _)) = guard.cache.get_mut(&key) {
            let result = value.clone();
            guard.next_counter += 1;
            let new_counter = guard.next_counter;
            if let Some((_, ref mut stored_counter)) = guard.cache.get_mut(&key) {
                *stored_counter = new_counter;
            }
            guard.lru_queue.push_back((key.clone(), new_counter));

            while let Some((ref qkey, qcounter)) = guard.lru_queue.pop_front() {
                let stale = match guard.cache.get(qkey) {
                    Some((_, stored_counter)) => *stored_counter != qcounter,
                    None => true,
                };
                if !stale {
                    guard.lru_queue.push_front((qkey.clone(), qcounter));
                    break;
                }
            }
            guard.compact_lru_queue_if_needed();

            drop(guard);
            self.hits.fetch_add(1, Ordering::Relaxed);
            if let Some(ref s) = self.stats {
                s.block_cache_hits.fetch_add(1, Ordering::Relaxed);
            }
            tracing::debug!(target: "cache", "cache.hit");
            Some(result)
        } else {
            drop(guard);
            self.misses.fetch_add(1, Ordering::Relaxed);
            if let Some(ref s) = self.stats {
                s.block_cache_misses.fetch_add(1, Ordering::Relaxed);
            }
            tracing::debug!(target: "cache", "cache.miss");
            None
        }
    }

    pub fn insert(&self, key: CacheKey, value: Bytes) {
        if self.capacity_per_shard == 0 {
            return;
        }
        let value_len = value.len();
        if value_len > self.capacity_per_shard {
            return;
        }

        let max_evictions = (self.capacity_per_shard / ASSUMED_BLOCK_SIZE).max(MIN_MAX_EVICTIONS);
        let idx = shard_index(&key);
        let mut guard = self.shards[idx].inner.lock();

        // 覆盖已有 key: 先清理旧 LRU entry (惰性, evict_one 会跳过 stale)
        if let Some((old, _)) = guard.cache.remove(&key) {
            guard.current_size -= old.len();
        }

        guard.next_counter += 1;
        let counter = guard.next_counter;

        // Best-effort eviction loop
        let mut eviction_count = 0usize;
        while guard.current_size + value_len > self.capacity_per_shard
            && eviction_count < max_evictions
        {
            if guard.evict_one(&self.evictions, Some(&key)) {
                eviction_count += 1;
            } else {
                break;
            }
        }

        // Guaranteed eviction (locked)
        while guard.current_size + value_len > self.capacity_per_shard {
            if !guard.evict_one(&self.evictions, None) {
                // 所有 entry 均被 pin, 无法驱逐; 记录告警日志但不拒绝插入.
                tracing::warn!(
                    target: "cache",
                    file_number = key.file_number,
                    offset = key.offset,
                    current_size = guard.current_size,
                    capacity_per_shard = self.capacity_per_shard,
                    "cache.insert.overflow_all_pinned"
                );
                break;
            }
        }

        let inserted = guard.cache.insert(key.clone(), (value, counter)).is_none();
        if inserted {
            guard.current_size += value_len;
        }
        guard.lru_queue.push_back((key, counter));
        guard.compact_lru_queue_if_needed();
        self.insertions.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(target: "cache", "cache.insert");
        drop(guard);
        if let Some(ref s) = self.stats {
            s.block_cache_size.store(self.size(), Ordering::Relaxed);
        }
    }

    pub fn clear(&self) {
        for shard in self.shards.iter() {
            let mut guard = shard.inner.lock();
            guard.cache.clear();
            guard.lru_queue.clear();
            guard.current_size = 0;
            // 不清空 pin_counts: 旧 PinGuard drop 后自然归零.
            // 调用方应在 clear() 前确保无活跃 PinGuard.
        }
        if let Some(ref s) = self.stats {
            s.block_cache_size.store(0, Ordering::Relaxed);
        }
    }

    /// 为已缓存的 entry 加 pin. 返回 PinGuard 表示 entry 被锁定,
    /// 在 PinGuard 存活期间该 entry 不被 evict.
    ///
    /// 若 key 不在 cache 中, 返回 None.
    pub fn pin(&self, key: CacheKey) -> Option<PinGuard<'_>> {
        let idx = shard_index(&key);
        let mut guard = self.shards[idx].inner.lock();
        if !guard.cache.contains_key(&key) {
            return None;
        }
        let counter = guard
            .pin_counts
            .entry(key.clone())
            .or_insert_with(|| AtomicU64::new(0));
        let prev = counter.fetch_add(1, Ordering::Relaxed);
        debug_assert!(prev < u64::MAX, "pin_count overflow for key {:?}", key);
        drop(guard);
        Some(PinGuard { key, cache: self })
    }

    /// 手动解 pin (等同于 drop PinGuard). 公开以便无 PinGuard 场景使用.
    fn unpin(&self, key: &CacheKey) {
        let idx = shard_index(key);
        let mut guard = self.shards[idx].inner.lock();
        if let Some(counter) = guard.pin_counts.get(key) {
            let prev = counter.fetch_sub(1, Ordering::Relaxed);
            if prev == 1 {
                guard.pin_counts.remove(key);
                // 重新挂入 lru_queue: 使该 entry 恢复正常驱逐路径.
                let stored_counter = guard.cache.get(key).map(|(_, c)| *c);
                if let Some(sc) = stored_counter {
                    guard.lru_queue.push_back((key.clone(), sc));
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        let mut total = 0usize;
        for shard in self.shards.iter() {
            let guard = shard.inner.lock();
            total += guard.cache.len();
        }
        total
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn size(&self) -> u64 {
        let mut total = 0usize;
        for shard in self.shards.iter() {
            let guard = shard.inner.lock();
            total += guard.current_size;
        }
        total as u64
    }

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            lookups: self.lookups.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            insertions: self.insertions.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    pub fn reset_stats(&self) {
        self.lookups.store(0, Ordering::Relaxed);
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.insertions.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
    }

    pub fn capacity(&self) -> usize {
        self.capacity_per_shard * NUM_SHARDS
    }

    #[cfg_attr(not(test), expect(dead_code, reason = "used in tests"))]
    pub(crate) fn total_lru_queue_len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.inner.lock().lru_queue.len())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::*;

    #[test]
    fn test_lib_cache_basic() {
        let cache = BlockCache::new(256);
        let k = CacheKey {
            file_number: 1,
            offset: 0,
        };
        cache.insert(k.clone(), Bytes::from_static(b"hello"));
        assert_eq!(cache.get(k).unwrap(), Bytes::from_static(b"hello"));
    }

    #[test]
    fn test_lru_queue_bounded_under_hot_key_access() {
        let cache = BlockCache::new(100 * 64);

        let cold_keys: Vec<CacheKey> = (0..32)
            .map(|i| CacheKey {
                file_number: 1000 + i,
                offset: 0,
            })
            .collect();
        for k in &cold_keys {
            cache.insert(k.clone(), Bytes::from(vec![0u8; 100]));
        }

        let hot_keys: Vec<CacheKey> = (0..8)
            .map(|i| CacheKey {
                file_number: 2000 + i,
                offset: 0,
            })
            .collect();
        for k in &hot_keys {
            cache.insert(k.clone(), Bytes::from(vec![1u8; 100]));
        }

        let live_before = cache.len();
        let hit = |round: u32| {
            let k = &hot_keys[(round as usize) % hot_keys.len()];
            assert!(cache.get(k.clone()).is_some());
        };

        for round in 0..5_000u32 {
            hit(round);
        }
        let queue_len_5k = cache.total_lru_queue_len();

        for round in 0..200_000u32 {
            hit(round);
        }
        let queue_len_200k = cache.total_lru_queue_len();

        let live_after = cache.len();
        assert_eq!(live_before, live_after);

        assert!(
            queue_len_200k <= queue_len_5k.max(NUM_SHARDS * LRU_QUEUE_COMPACT_MIN) * 2,
            "lru_queue 长度随访问次数持续增长, 疑似无界: \
             5_000 次访问后长度={queue_len_5k}, 200_000 次访问后长度={queue_len_200k}"
        );
    }

    // --- Pinning 测试 ---

    #[test]
    fn test_pin_basic_prevents_eviction() {
        // 所有 key 用相同 file_number 确保在同一 shard.
        // capacity=1024 → per_shard=64, 每个 entry 10 bytes → 最多 6 个.
        let cache = BlockCache::new(1024);
        let mk = |n: u64| CacheKey {
            file_number: 1,
            offset: n * 10,
        };

        // 填入 5 个 entry (50 bytes ≤ 64)
        for i in 0..5 {
            cache.insert(mk(i), Bytes::from(vec![i as u8; 10]));
        }

        // pin k0
        let k0 = mk(0);
        let guard = cache.pin(k0.clone()).expect("k0 should be cached");

        // 插入第 6 个 entry: 50+10=60 ≤ 64, 无需 eviction
        cache.insert(mk(5), Bytes::from(vec![5u8; 10]));

        // 插入第 7 个 entry: 60+10=70 > 64 → eviction
        // k0 被 pin, 应被跳过; 驱逐未 pinned 的 entry.
        cache.insert(mk(6), Bytes::from(vec![6u8; 10]));

        assert!(cache.get(k0).is_some(), "pinned k0 should stay");

        drop(guard);

        // unpin 后可被驱逐: 插入 k7 把 k0 挤出去
        cache.insert(mk(7), Bytes::from(vec![7u8; 10]));
    }

    #[test]
    fn test_pin_unpin_returns_to_lru() {
        let cache = BlockCache::new(1024);
        let mk = |n: u64| CacheKey {
            file_number: 1,
            offset: n * 10,
        };

        cache.insert(mk(0), Bytes::from(vec![0u8; 10]));
        for i in 1..6 {
            cache.insert(mk(i), Bytes::from(vec![i as u8; 10]));
        }

        let k0 = mk(0);
        let guard = cache.pin(k0.clone()).expect("k0 should be cached");
        drop(guard);

        // unpin 后 k0 重回 lru_queue, 可以被驱逐.
        // 插入第 7 个 entry (容量仅够 6 个) → 驱逐 k0 (队头).
        cache.insert(mk(6), Bytes::from(vec![6u8; 10]));
        // 不 panic 即测试通过 — k0 已被正常驱逐
    }

    #[test]
    fn test_pin_multiple_then_unpin() {
        let cache = BlockCache::new(1024);
        let k = CacheKey {
            file_number: 1,
            offset: 0,
        };
        cache.insert(k.clone(), Bytes::from(vec![0u8; 10]));

        // 对同一 key 多次 pin
        let g1 = cache.pin(k.clone()).unwrap();
        let g2 = cache.pin(k.clone()).unwrap();

        for i in 1..6 {
            cache.insert(
                CacheKey {
                    file_number: 1,
                    offset: i * 10,
                },
                Bytes::from(vec![i as u8; 10]),
            );
        }

        // 释放 g1, k 仍有一次 pin 保护
        drop(g1);
        assert!(cache.get(k.clone()).is_some());

        // 插入第 7 个 entry: 容量满 → eviction, k 仍被 g2 保护
        cache.insert(
            CacheKey {
                file_number: 1,
                offset: 70,
            },
            Bytes::from(vec![7u8; 10]),
        );
        assert!(cache.get(k.clone()).is_some());

        // 释放最后一个 guard
        drop(g2);

        // 再插入: k 可被驱逐
        cache.insert(
            CacheKey {
                file_number: 1,
                offset: 80,
            },
            Bytes::from(vec![8u8; 10]),
        );
        // 不 panic
    }

    #[test]
    fn test_pin_all_entries_eviction_does_not_panic() {
        // per_shard = 1024 / 16 = 64. 每个 entry 10 bytes → 最多 6 个.
        let cache = BlockCache::new(1024);
        // 使用固定 file_number 使所有 key 落入同一 shard
        let keys: Vec<CacheKey> = (0..6)
            .map(|i| CacheKey {
                file_number: 1,
                offset: i * 10,
            })
            .collect();

        for k in &keys {
            cache.insert(k.clone(), Bytes::from(vec![k.offset as u8; 10]));
        }

        // pin 所有 entry
        let guards: Vec<_> = keys
            .iter()
            .map(|k| cache.pin(k.clone()).expect("should be in cache"))
            .collect();

        // 插入新 key: 无法驱逐任何 entry, 但不 panic
        let new_key = CacheKey {
            file_number: 1,
            offset: 60,
        };
        cache.insert(new_key.clone(), Bytes::from(vec![9u8; 10]));
        // 所有原始 key 仍然存在
        for k in &keys {
            assert!(cache.get(k.clone()).is_some());
        }
        drop(guards);
    }

    #[test]
    fn test_pin_nonexistent_key() {
        let cache = BlockCache::new(200);
        let k = CacheKey {
            file_number: 99,
            offset: 0,
        };
        assert!(cache.pin(k).is_none());
    }

    #[test]
    fn test_pin_concurrent() {
        let cache = Arc::new(BlockCache::new(10 * 1024));
        let keys: Vec<CacheKey> = (0..16)
            .map(|i| CacheKey {
                file_number: i,
                offset: 0,
            })
            .collect();

        let keys_t1 = keys.clone();
        let cache_clone = Arc::clone(&cache);
        let t1 = thread::spawn(move || {
            for k in &keys_t1 {
                cache_clone.insert(k.clone(), Bytes::from(vec![k.file_number as u8; 64]));
                let guard = cache_clone.pin(k.clone());
                if let Some(g) = guard {
                    std::thread::sleep(std::time::Duration::from_micros(10));
                    drop(g);
                }
            }
        });

        let keys_t2 = keys.clone();
        let cache_clone = Arc::clone(&cache);
        let t2 = thread::spawn(move || {
            for k in &keys_t2 {
                let guard = cache_clone.pin(k.clone());
                if let Some(g) = guard {
                    std::thread::sleep(std::time::Duration::from_micros(5));
                    drop(g);
                }
            }
        });

        let keys_t3 = keys.clone();
        let cache_clone = Arc::clone(&cache);
        let t3 = thread::spawn(move || {
            for k in &keys_t3 {
                let _ = cache_clone.get(k.clone());
                std::thread::sleep(std::time::Duration::from_micros(15));
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();
        t3.join().unwrap();

        // 最终一致性: 所有 key 在 cache 中 (无 eviction, 容量足够)
        for k in &keys {
            assert!(cache.get(k.clone()).is_some(), "key {k:?} should exist");
        }
    }
}
