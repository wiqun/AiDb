//! Data Block 存储格式: prefix compression + restart points + 块内 Hash Index (Data / Index Block 共用).
//!
//! # 布局
//!
//! ```text
//! Block = [entry × N][restart_offset: 4B × K][num_restarts: 4B][hash_buckets: 1B × B][num_buckets: 4B]
//! entry = [shared: varint32][unshared: varint32][value_len: varint32][key_delta][value]
//! ```
//!
//! `key_delta` = key 去掉与上一 key 公共前缀 (`shared`) 后的部分; 尾部 restart array 记录
//! 每个 restart point 的 entry 偏移, 供二分定位后线性扫描; Data Block 尾部附带 Hash Index
//! (`K_NO_ENTRY=255`, `K_COLLISION=254`, 其他值表示唯一 restart 索引) 加速点查.
//!
//! # Invariant
//!
//! - restart point 存完整 key (`shared=0`), 保证 key 链可从任意 restart point 重建.
//! - `add` 要求 key 严格递增, 否则 `Error::InvalidArgument`.

use bytes::{Bytes, BytesMut};
use std::cmp::Ordering;

use crate::engine::memtable::compare_internal_key;
use crate::error::{Error, Result};

const K_COLLISION: u8 = 254;
const K_NO_ENTRY: u8 = 255;
const MAX_HASH_INDEX_RESTARTS: usize = 253;

#[inline]
fn put_varint32(buf: &mut BytesMut, mut v: u32) {
    while v >= 0x80 {
        buf.extend_from_slice(&[((v as u8) & 0x7f) | 0x80]);
        v >>= 7;
    }
    buf.extend_from_slice(&[v as u8]);
}

#[inline]
fn get_varint32(data: &[u8], cur: &mut usize) -> Result<u32> {
    let mut result = 0u32;
    let mut shift = 0u32;
    while shift <= 28 {
        if *cur >= data.len() {
            return Err(Error::Corruption("truncated varint32 in block".into()));
        }
        let b = data[*cur];
        *cur += 1;
        if b < 0x80 {
            return Ok(result | ((b as u32) << shift));
        }
        result |= ((b & 0x7f) as u32) << shift;
        shift += 7;
    }
    Err(Error::Corruption("malformed varint32 in block".into()))
}

#[inline]
fn block_hash64(key: &[u8]) -> u64 {
    let mut z: u64 = 0xcbf29ce484222325;
    for &b in key {
        z ^= b as u64;
        z = z.wrapping_mul(0x00000100000001b3);
    }
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

/// 构建 Data Block (或 Index Block).
pub struct BlockBuilder {
    buffer: BytesMut,
    restarts: Vec<u32>,
    counter: usize,
    restart_interval: usize,
    last_key: Vec<u8>,
    hash_index_enabled: bool,
    hash_entries: Vec<(u64, u8)>,
}

impl BlockBuilder {
    pub fn new(restart_interval: usize) -> Self {
        let interval = restart_interval.max(1);
        Self {
            buffer: BytesMut::new(),
            restarts: Vec::new(),
            counter: 0,
            restart_interval: interval,
            last_key: Vec::new(),
            hash_index_enabled: interval > 1,
            hash_entries: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn current_size(&self) -> usize {
        let hash_bytes = if self.hash_index_enabled && !self.hash_entries.is_empty() {
            ((self.hash_entries.len() as f64 / 0.75).ceil() as usize | 1) + 4
        } else {
            4
        };
        self.buffer.len() + self.restarts.len() * 4 + 4 + hash_bytes
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.last_key.is_empty()
            && compare_internal_key(key, self.last_key.as_slice()) != Ordering::Greater
        {
            return Err(Error::InvalidArgument(
                "BlockBuilder: keys must be strictly increasing".into(),
            ));
        }

        let is_restart = self.counter == 0;
        if is_restart {
            self.restarts.push(self.buffer.len() as u32);
            if self.restarts.len() > MAX_HASH_INDEX_RESTARTS {
                self.hash_index_enabled = false;
                self.hash_entries.clear();
            }
        }

        // Restart points must store full key (shared=0), matching LevelDB behavior.
        let shared = if is_restart {
            0
        } else {
            shared_prefix_len(&self.last_key, key)
        };
        let unshared = key.len() - shared;
        put_varint32(&mut self.buffer, shared as u32);
        put_varint32(&mut self.buffer, unshared as u32);
        put_varint32(&mut self.buffer, value.len() as u32);
        self.buffer.extend_from_slice(&key[shared..]);
        self.buffer.extend_from_slice(value);

        if self.hash_index_enabled {
            let user_key = if key.len() >= 8 {
                &key[..key.len() - 8]
            } else {
                key
            };
            let restart_idx = (self.restarts.len() - 1) as u8;
            self.hash_entries
                .push((block_hash64(user_key), restart_idx));
        }

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.counter += 1;
        if self.counter >= self.restart_interval {
            self.counter = 0;
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Bytes {
        for r in &self.restarts {
            self.buffer.extend_from_slice(&r.to_le_bytes());
        }
        self.buffer
            .extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());

        if self.hash_index_enabled && !self.hash_entries.is_empty() {
            let num_buckets = ((self.hash_entries.len() as f64 / 0.75).ceil() as u32) | 1;
            let start = self.buffer.len();
            self.buffer.resize(start + num_buckets as usize, K_NO_ENTRY);
            let buckets = &mut self.buffer[start..];
            for &(h, restart_idx) in &self.hash_entries {
                let idx = (h as usize) % (num_buckets as usize);
                let cur = buckets[idx];
                if cur == K_NO_ENTRY {
                    buckets[idx] = restart_idx;
                } else if cur != restart_idx {
                    buckets[idx] = K_COLLISION;
                }
            }
            self.buffer.extend_from_slice(&num_buckets.to_le_bytes());
        } else {
            self.buffer.extend_from_slice(&0u32.to_le_bytes());
        }
        self.hash_entries.clear();
        std::mem::take(&mut self.buffer).freeze()
    }
}

fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

/// 只读 Block.
#[derive(Clone)]
pub struct Block {
    data: Bytes,
    restart_offset: usize,
    num_restarts: u32,
    hash_offset: usize,
    num_buckets: u32,
}

impl Block {
    pub fn new(data: Bytes) -> Result<Self> {
        if data.len() < 8 {
            return Err(Error::Corruption("block too small".into()));
        }
        let num_buckets = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap());
        let hash_bytes = num_buckets as usize;
        if hash_bytes + 8 > data.len() {
            return Err(Error::Corruption(format!(
                "num_buckets {num_buckets} out of range for block len {}",
                data.len()
            )));
        }
        let hash_offset = data.len() - 4 - hash_bytes;
        let num_restarts =
            u32::from_le_bytes(data[hash_offset - 4..hash_offset].try_into().unwrap());
        let restart_bytes = num_restarts as usize * 4;
        if restart_bytes + 4 > hash_offset {
            return Err(Error::Corruption(format!(
                "num_restarts {num_restarts} out of range for block len {}",
                data.len()
            )));
        }
        let restart_offset = hash_offset - 4 - restart_bytes;
        Ok(Self {
            data,
            restart_offset,
            num_restarts,
            hash_offset,
            num_buckets,
        })
    }

    pub fn num_restarts(&self) -> u32 {
        self.num_restarts
    }

    fn restart_offset_at(&self, index: u32) -> u32 {
        let base = self.restart_offset + index as usize * 4;
        u32::from_le_bytes(self.data[base..base + 4].try_into().unwrap())
    }

    pub fn iter(&self) -> BlockIterator {
        BlockIterator::new(self.clone())
    }
}

/// Block 内前向迭代.
pub struct BlockIterator {
    block: Block,
    current: usize,
    entry_end: usize,
    restart_index: u32,
    key: Vec<u8>,
    value: Vec<u8>,
    valid: bool,
}

impl BlockIterator {
    pub fn new(block: Block) -> Self {
        let mut it = Self {
            block,
            current: 0,
            entry_end: 0,
            restart_index: 0,
            key: Vec::new(),
            value: Vec::new(),
            valid: false,
        };
        it.seek_to_first();
        it
    }

    pub fn seek_to_first(&mut self) {
        self.current = 0;
        self.restart_index = 0;
        self.key.clear();
        self.value.clear();
        self.valid = self.decode_current();
    }

    pub fn seek_to_restart_point(&mut self, index: u32) {
        if index >= self.block.num_restarts {
            self.valid = false;
            return;
        }
        self.restart_index = index;
        self.current = self.block.restart_offset_at(index) as usize;
        self.key.clear();
        self.value.clear();
        self.valid = self.decode_current();
    }

    /// 线性扫描直到 `key >= target` (InternalKey 比较).
    pub fn seek(&mut self, target: &[u8]) {
        if self.block.num_restarts == 0 {
            self.valid = false;
            return;
        }
        let mut left = 0i32;
        let mut right = self.block.num_restarts as i32 - 1;
        let mut best = 0u32;
        while left <= right {
            let mid = left + (right - left) / 2;
            self.seek_to_restart_point(mid as u32);
            if !self.valid {
                break;
            }
            if compare_internal_key(self.key(), target) != Ordering::Greater {
                best = mid as u32;
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        self.seek_to_restart_point(best);
        while self.valid() && compare_internal_key(self.key(), target) == Ordering::Less {
            if !self.advance() {
                break;
            }
        }
    }

    /// 点查优先通过块内 Hash Index 直接定位 restart 区间, 冲突或无桶时回退二分查找.
    pub fn seek_point(&mut self, target: &[u8]) {
        if self.block.num_buckets == 0 || target.len() < 8 {
            self.seek(target);
            return;
        }
        let user_key = &target[..target.len() - 8];
        let bucket_idx = (block_hash64(user_key) as usize) % (self.block.num_buckets as usize);
        let entry = self.block.data[self.block.hash_offset + bucket_idx];
        match entry {
            K_NO_ENTRY => {
                self.valid = false;
            }
            K_COLLISION => {
                self.seek(target);
            }
            restart_idx => {
                let r = restart_idx as u32;
                let next_restart_off = if r + 1 < self.block.num_restarts {
                    self.block.restart_offset_at(r + 1) as usize
                } else {
                    self.block.restart_offset
                };
                self.seek_to_restart_point(r);
                while self.valid()
                    && self.current < next_restart_off
                    && compare_internal_key(self.key(), target) == Ordering::Less
                {
                    if !self.advance() {
                        break;
                    }
                }
                if self.current >= next_restart_off {
                    self.valid = false;
                }
            }
        }
    }

    pub fn advance(&mut self) -> bool {
        if !self.valid {
            return false;
        }
        self.current = self.entry_end;
        self.valid = self.decode_entry();
        self.valid
    }

    /// 反向移动一步: walk from nearest restart point, maintain key chain.
    pub fn prev(&mut self) -> bool {
        if !self.valid || self.current == 0 {
            self.valid = false;
            return false;
        }

        // Find restart point STRICTLY BEFORE current
        let num = self.block.num_restarts;
        let mut lo = 0i32;
        let mut hi = num as i32 - 1;
        let mut restart_idx: i32 = -1;
        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            let ro = self.block.restart_offset_at(mid as u32) as usize;
            if ro < self.current {
                restart_idx = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }

        let start_off = if restart_idx >= 0 {
            self.block.restart_offset_at(restart_idx as u32) as usize
        } else {
            0usize
        };

        let original_current = self.current;

        // Walk from restart point, reconstructing keys via prefix chain
        let mut scan = start_off;
        let mut prev_entry_start = start_off;
        let mut found = false;
        self.key.clear();
        self.value.clear();

        while scan < self.block.restart_offset && scan < original_current {
            prev_entry_start = scan;
            found = true;

            let mut cur = scan;
            let Ok(shared) = get_varint32(&self.block.data, &mut cur) else {
                self.valid = false;
                return false;
            };
            let Ok(unshared) = get_varint32(&self.block.data, &mut cur) else {
                self.valid = false;
                return false;
            };
            let Ok(value_len) = get_varint32(&self.block.data, &mut cur) else {
                self.valid = false;
                return false;
            };
            let key_end = cur + unshared as usize;
            let val_end = key_end + value_len as usize;
            if val_end > self.block.restart_offset {
                break;
            }

            // Reconstruct key using prefix chain
            if shared == 0 {
                self.key.clear();
            } else if shared as usize > self.key.len() {
                // Key chain broken — should not happen with valid data
                break;
            } else {
                self.key.truncate(shared as usize);
            }
            self.key.extend_from_slice(&self.block.data[cur..key_end]);
            self.value.clear();
            self.value
                .extend_from_slice(&self.block.data[key_end..val_end]);

            scan = val_end;
        }

        if !found {
            self.valid = false;
            return false;
        }

        // Key and value already set by the walk; just update position metadata
        self.current = prev_entry_start;
        self.entry_end = scan; // scan is the end of prev entry (= start of current entry in walk)
        self.valid = true;
        true
    }

    /// 定位到 block 内最后一个 entry (walk from last restart point).
    pub fn seek_to_last(&mut self) {
        let num = self.block.num_restarts;
        if num == 0 {
            self.valid = false;
            return;
        }

        let last_restart = num - 1;

        // Walk from last restart point to the final entry, maintaining key chain
        let start_off = self.block.restart_offset_at(last_restart) as usize;
        let mut scan = start_off;
        let mut last_entry = start_off;
        self.key.clear();
        self.value.clear();

        while scan < self.block.restart_offset {
            last_entry = scan;
            let mut cur = scan;
            let Ok(shared) = get_varint32(&self.block.data, &mut cur) else {
                break;
            };
            let Ok(unshared) = get_varint32(&self.block.data, &mut cur) else {
                break;
            };
            let Ok(value_len) = get_varint32(&self.block.data, &mut cur) else {
                break;
            };
            let key_end = cur + unshared as usize;
            let val_end = key_end + value_len as usize;
            if val_end > self.block.restart_offset {
                break;
            }

            // Reconstruct key using prefix chain
            if shared == 0 {
                self.key.clear();
            } else if shared as usize > self.key.len() {
                break;
            } else {
                self.key.truncate(shared as usize);
            }
            self.key.extend_from_slice(&self.block.data[cur..key_end]);
            self.value.clear();
            self.value
                .extend_from_slice(&self.block.data[key_end..val_end]);

            scan = val_end;
        }

        self.current = last_entry;
        self.entry_end = scan;
        self.restart_index = last_restart;
        self.valid = true;
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    fn decode_current(&mut self) -> bool {
        self.decode_entry()
    }

    fn decode_entry(&mut self) -> bool {
        if self.current >= self.block.restart_offset {
            self.valid = false;
            return false;
        }
        let offset = self.current;
        let mut cur = offset;
        let shared = match get_varint32(&self.block.data, &mut cur) {
            Ok(v) => v,
            Err(_) => {
                self.valid = false;
                return false;
            }
        };
        let unshared = match get_varint32(&self.block.data, &mut cur) {
            Ok(v) => v,
            Err(_) => {
                self.valid = false;
                return false;
            }
        };
        let value_len = match get_varint32(&self.block.data, &mut cur) {
            Ok(v) => v,
            Err(_) => {
                self.valid = false;
                return false;
            }
        };
        let key_end = cur + unshared as usize;
        let val_end = key_end + value_len as usize;
        if val_end > self.block.restart_offset {
            self.valid = false;
            return false;
        }

        if shared == 0 {
            self.key.clear();
        } else if shared as usize > self.key.len() {
            self.valid = false;
            return false;
        } else {
            self.key.truncate(shared as usize);
        }
        self.key.extend_from_slice(&self.block.data[cur..key_end]);
        self.value.clear();
        self.value
            .extend_from_slice(&self.block.data[key_end..val_end]);
        self.entry_end = val_end;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::memtable::{encode_internal_key, ValueType};

    #[test]
    fn block_varint_and_hash_index_point_lookup_and_collision() {
        let mut b = BlockBuilder::new(2);
        b.add(&encode_internal_key(b"k1", 10, ValueType::TypePut), b"v1")
            .unwrap();
        b.add(
            &encode_internal_key(b"k2", 20, ValueType::TypePut),
            b"v2_new",
        )
        .unwrap();
        b.add(
            &encode_internal_key(b"k2", 10, ValueType::TypePut),
            b"v2_old",
        )
        .unwrap();
        b.add(&encode_internal_key(b"k4", 10, ValueType::TypePut), b"v4")
            .unwrap();
        let raw = b.finish();
        let block = Block::new(raw).unwrap();

        let mut it = block.iter();
        it.seek_point(&encode_internal_key(b"k1", 15, ValueType::TypePut));
        assert!(it.valid());
        assert_eq!(it.value(), b"v1");

        it.seek_point(&encode_internal_key(b"k2", 15, ValueType::TypePut));
        assert!(it.valid());
        assert_eq!(
            it.key(),
            &encode_internal_key(b"k2", 10, ValueType::TypePut)
        );

        it.seek_point(&encode_internal_key(b"missing_key", 15, ValueType::TypePut));
        if it.valid() {
            assert_ne!(&it.key()[..it.key().len() - 8], b"missing_key");
        }
    }
}
