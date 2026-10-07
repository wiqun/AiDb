//! Data Block 存储格式: prefix compression + restart points (Data / Index Block 共用).
//!
//! # 布局
//!
//! ```text
//! Block = [entry × N][restart_offset: 4B × K][num_restarts: 4B]
//! entry = [shared:4][unshared:4][value_len:4][key_delta][value]
//! ```
//!
//! `key_delta` = key 去掉与上一 key 公共前缀 (`shared`) 后的部分; 尾部 restart array 记录
//! 每个 restart point 的 entry 偏移, 供二分定位后线性扫描.
//!
//! # Invariant
//!
//! - restart point 存完整 key (`shared=0`), 保证 key 链可从任意 restart point 重建.
//! - `add` 要求 key 严格递增, 否则 `Error::InvalidArgument`.

use bytes::{Bytes, BytesMut};
use std::cmp::Ordering;

use crate::engine::memtable::compare_internal_key;
use crate::error::{Error, Result};

/// 构建 Data Block (或 Index Block).
pub struct BlockBuilder {
    buffer: BytesMut,
    restarts: Vec<u32>,
    counter: usize,
    restart_interval: usize,
    last_key: Vec<u8>,
}

impl BlockBuilder {
    pub fn new(restart_interval: usize) -> Self {
        Self {
            buffer: BytesMut::new(),
            restarts: Vec::new(),
            counter: 0,
            restart_interval: restart_interval.max(1),
            last_key: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn current_size(&self) -> usize {
        self.buffer.len() + self.restarts.len() * 4 + 4
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
        }

        // Restart points must store full key (shared=0), matching LevelDB behavior.
        let shared = if is_restart {
            0
        } else {
            shared_prefix_len(&self.last_key, key)
        };
        let unshared = key.len() - shared;
        self.buffer
            .extend_from_slice(&(shared as u32).to_le_bytes());
        self.buffer
            .extend_from_slice(&(unshared as u32).to_le_bytes());
        self.buffer
            .extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.buffer.extend_from_slice(&key[shared..]);
        self.buffer.extend_from_slice(value);

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
}

impl Block {
    pub fn new(data: Bytes) -> Result<Self> {
        if data.len() < 4 {
            return Err(Error::Corruption("block too small".into()));
        }
        let num_restarts = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap());
        let restart_bytes = num_restarts as usize * 4;
        if restart_bytes + 4 > data.len() {
            return Err(Error::Corruption(format!(
                "num_restarts {num_restarts} out of range for block len {}",
                data.len()
            )));
        }
        let restart_offset = data.len() - 4 - restart_bytes;
        Ok(Self {
            data,
            restart_offset,
            num_restarts,
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
            let Ok(shared) = read_u32(&self.block.data, &mut cur) else {
                self.valid = false;
                return false;
            };
            let Ok(unshared) = read_u32(&self.block.data, &mut cur) else {
                self.valid = false;
                return false;
            };
            let Ok(value_len) = read_u32(&self.block.data, &mut cur) else {
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
            let Ok(shared) = read_u32(&self.block.data, &mut cur) else {
                break;
            };
            let Ok(unshared) = read_u32(&self.block.data, &mut cur) else {
                break;
            };
            let Ok(value_len) = read_u32(&self.block.data, &mut cur) else {
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
        let shared = match read_u32(&self.block.data, &mut cur) {
            Ok(v) => v,
            Err(_) => {
                self.valid = false;
                return false;
            }
        };
        let unshared = match read_u32(&self.block.data, &mut cur) {
            Ok(v) => v,
            Err(_) => {
                self.valid = false;
                return false;
            }
        };
        let value_len = match read_u32(&self.block.data, &mut cur) {
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

fn read_u32(data: &Bytes, cur: &mut usize) -> Result<u32> {
    if *cur + 4 > data.len() {
        return Err(Error::Corruption("truncated block entry".into()));
    }
    let v = u32::from_le_bytes(data[*cur..*cur + 4].try_into().unwrap());
    *cur += 4;
    Ok(v)
}
