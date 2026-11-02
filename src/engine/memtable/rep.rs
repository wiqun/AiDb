//! MemTableRep trait — 抽象 MemTable 底层存储.
//!
//! 当前唯一实现为 SkipMapRep. 当需要替代实现时, 将 MemTable 字段类型泛型化即可.

use std::ops::Bound;
use std::sync::Arc;

use super::key_bytes::{InternalKeyBytes, InternalKeySlice};

/// 抽象 MemTable 底层存储 (对应 RocksDB MemTableRepFactory).
pub(crate) trait MemTableRep {
    /// Entry 引用: key=InternalKeyBytes, value=Arc<[u8]>.
    type EntryRef<'a>
    where
        Self: 'a;

    fn insert(&self, key: InternalKeyBytes, value: Arc<[u8]>);
    fn lower_bound(&self, bound: Bound<&InternalKeySlice>) -> Option<Self::EntryRef<'_>>;
    fn front(&self) -> Option<Self::EntryRef<'_>>;
    fn back(&self) -> Option<Self::EntryRef<'_>>;

    /// 从 entry 获取 key 引用.
    #[expect(dead_code)]
    fn entry_key<'a>(entry: &'a Self::EntryRef<'a>) -> &'a InternalKeyBytes;

    /// 从 entry 获取 value 的 Arc clone (O(1) ref bump).
    #[expect(dead_code)]
    fn entry_value(entry: &Self::EntryRef<'_>) -> Arc<[u8]>;

    /// 移动到下一个 entry.
    #[expect(dead_code)]
    fn entry_next<'a>(entry: &'a Self::EntryRef<'a>) -> Option<Self::EntryRef<'a>>;

    /// 移动到上一个 entry.
    #[expect(dead_code)]
    fn entry_prev<'a>(entry: &'a Self::EntryRef<'a>) -> Option<Self::EntryRef<'a>>;

    /// 近似内存占用 (bytes).
    fn approximate_size(&self) -> usize;
}
