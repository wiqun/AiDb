//! IndexBlock — Data Block 的最大 InternalKey → BlockHandle 的映射, 用于定位目标 block.
//!
//! # 组织方式
//!
//! - 构建复用 `BlockBuilder`, restart interval 固定为 1 (每条 entry 都存完整 key, 不依赖
//!   block 内前缀链), value 为 `BlockHandle::encode()` 字节.
//! - 查询: `find_block_handle` 在 entries 上二分, 返回最后一个 `key <= seek_key` 的
//!   Data Block 的 handle.

use std::cmp::Ordering;

use bytes::Bytes;

use crate::engine::memtable::{
    compare_internal_key, encode_internal_key, extract_user_key, ValueType,
};
use crate::error::{Error, Result};

use super::block::{Block, BlockBuilder};
use super::handle::BlockHandle;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    pub key: Vec<u8>,
    pub handle: BlockHandle,
}

pub struct IndexBlockBuilder {
    builder: BlockBuilder,
}

impl Default for IndexBlockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexBlockBuilder {
    pub fn new() -> Self {
        Self {
            builder: BlockBuilder::new(1),
        }
    }

    pub fn add_entry(&mut self, entry: &IndexEntry) -> Result<()> {
        let value = entry.handle.encode();
        self.builder.add(&entry.key, &value)
    }

    pub fn finish(&mut self) -> Bytes {
        self.builder.finish()
    }

    pub fn is_empty(&self) -> bool {
        self.builder.is_empty()
    }
}

pub struct IndexBlock {
    block: Block,
}

impl IndexBlock {
    pub fn new(data: Bytes) -> Result<Self> {
        Ok(Self {
            block: Block::new(data)?,
        })
    }

    pub fn entries(&self) -> Result<Vec<IndexEntry>> {
        let mut out = Vec::new();
        let mut it = self.block.iter();
        while it.valid() {
            let key = it.key().to_vec();
            let handle = BlockHandle::decode(it.value())?;
            out.push(IndexEntry { key, handle });
            if !it.advance() {
                break;
            }
        }
        Ok(out)
    }

    /// 定位可能包含 `seek_key` 的 Data Block.
    pub fn find_block(&self, seek_key: &[u8]) -> Result<BlockHandle> {
        let entries = self.entries()?;
        let tuples: Vec<_> = entries.into_iter().map(|e| (e.key, e.handle)).collect();
        find_block_handle(&tuples, seek_key)
    }
}

/// 在 Index 条目上二分定位 Data Block.
pub fn find_block_handle(
    entries: &[(Vec<u8>, BlockHandle)],
    seek_key: &[u8],
) -> Result<BlockHandle> {
    if entries.is_empty() {
        return Err(Error::Corruption("empty index block".into()));
    }
    let mut lo = 0usize;
    let mut hi = entries.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if compare_internal_key(&entries[mid].0, seek_key) == Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    let idx = if lo < entries.len() {
        lo
    } else {
        entries.len() - 1
    };
    Ok(entries[idx].1)
}

/// 从 Index Block 解析 entries (供 Iterator 复用).
pub fn load_index_entries(block: &Block) -> Result<Vec<(Vec<u8>, BlockHandle)>> {
    let mut out = Vec::new();
    let mut it = block.iter();
    while it.valid() {
        out.push((it.key().to_vec(), BlockHandle::decode(it.value())?));
        if !it.advance() {
            break;
        }
    }
    Ok(out)
}

const MAX_SEQUENCE_NUMBER: u64 = (1u64 << 56) - 1;

/// 计算满足 `last_key < sep < next_first_key` 的最短 InternalKey 分隔符.
pub fn find_shortest_internal_key_separator(last_key: &[u8], next_first_key: &[u8]) -> Vec<u8> {
    if last_key.len() < 8 || next_first_key.len() < 8 {
        return last_key.to_vec();
    }
    let last_user = extract_user_key(last_key);
    let next_user = extract_user_key(next_first_key);
    if last_user >= next_user {
        return last_key.to_vec();
    }
    let prefix_len = last_user
        .iter()
        .zip(next_user.iter())
        .take_while(|(a, b)| a == b)
        .count();
    if prefix_len < last_user.len()
        && last_user[prefix_len] < 0xff
        && last_user[prefix_len] + 1 < next_user[prefix_len]
    {
        let mut sep_user = last_user[..=prefix_len].to_vec();
        sep_user[prefix_len] += 1;
        let sep_internal = encode_internal_key(&sep_user, MAX_SEQUENCE_NUMBER, ValueType::TypePut);
        if sep_internal.len() < last_key.len()
            && compare_internal_key(last_key, &sep_internal) == Ordering::Less
            && compare_internal_key(&sep_internal, next_first_key) == Ordering::Less
        {
            return sep_internal;
        }
    }
    last_key.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shortest_internal_key_separator_shortens_and_preserves_order() {
        let last = encode_internal_key(b"user:1000:profile", 50, ValueType::TypePut);
        let next = encode_internal_key(b"user:3000:profile", 40, ValueType::TypePut);
        let sep = find_shortest_internal_key_separator(&last, &next);
        assert!(sep.len() < last.len());
        assert_eq!(compare_internal_key(&last, &sep), Ordering::Less);
        assert_eq!(compare_internal_key(&sep, &next), Ordering::Less);

        let adj1 = encode_internal_key(b"ab", 10, ValueType::TypePut);
        let adj2 = encode_internal_key(b"ac", 10, ValueType::TypePut);
        assert_eq!(find_shortest_internal_key_separator(&adj1, &adj2), adj1);
    }
}
