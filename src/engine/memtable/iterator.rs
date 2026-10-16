//! MemTable 迭代器: seek / seek_to_first / next / prev.

use super::internal_key::{encode_internal_key, ValueType, K_MAX_SEQUENCE};
use super::key_bytes::InternalKeyBytes;
use super::rep::MemTableRep;
use super::skiplist_rep::SkipMapRep;
use super::MemTable;
use crossbeam_skiplist::map::Entry;
use std::ops::Bound;
use std::sync::Arc;

/// MemTable 前向迭代器.
pub struct MemTableIterator<'a> {
    rep: &'a SkipMapRep,
    current: Option<Entry<'a, InternalKeyBytes, Arc<[u8]>>>,
}

impl<'a> MemTableIterator<'a> {
    pub(crate) fn new(table: &'a MemTable) -> Self {
        Self {
            rep: table.rep(),
            current: None,
        }
    }

    /// 定位到 >= target_user_key 的第一个 entry (含所有版本).
    pub fn seek(&mut self, target_user_key: &[u8]) {
        let encoded = encode_internal_key(target_user_key, K_MAX_SEQUENCE, ValueType::TypePut);
        let seek = InternalKeyBytes::from_slice(&encoded);
        self.current = self.rep.lower_bound(Bound::Included(&seek));
    }

    pub fn seek_to_first(&mut self) {
        self.current = self.rep.front();
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> bool {
        self.current = match self.current.take() {
            None => self.rep.front(),
            Some(entry) => entry.next(),
        };
        self.valid()
    }

    pub fn key(&self) -> &[u8] {
        self.current
            .as_ref()
            .expect("iterator not valid")
            .key()
            .as_ref()
    }

    pub fn value(&self) -> &[u8] {
        // value_arc lazily resolved here for &[u8] compatibility
        // (returning &[u8] requires a persistent allocation that the iterator doesn't hold)
        // This is an existing limitation — caller expected to use value_arc() for O(1).
        unreachable!("use value_arc() instead")
    }

    /// 返回原始 Arc<[u8]> (value 的 Arc 引用, clone 为 O(1) ref bump).
    pub fn value_arc(&self) -> &Arc<[u8]> {
        // value_arc returns a reference to the Arc stored in the SkipMap entry,
        // which is valid for the lifetime of the iterator.
        // We need to unsafely extend the lifetime since Entry holds the Arc internally.
        let entry = self.current.as_ref().expect("iterator not valid");
        // Entry::value() returns &Arc<[u8]> valid for 'a
        entry.value()
    }

    pub fn valid(&self) -> bool {
        self.current.is_some()
    }

    #[allow(clippy::should_implement_trait)]
    pub fn prev(&mut self) -> bool {
        self.current = match self.current.take() {
            None => self.rep.back(),
            Some(entry) => entry.prev(),
        };
        self.valid()
    }
}
