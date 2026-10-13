//! MemTable — 内存写入缓冲.

mod internal_key;
mod iterator;
mod key_bytes;
mod range_tombstone;
mod rep;
mod skiplist_rep;
mod table;

pub use internal_key::{
    check_sequence, compare_internal_key, decode_internal_key, encode_internal_key,
    encode_internal_key_buffered, extract_sequence, extract_user_key, extract_value_type,
    ValueType, K_MAX_SEQUENCE, K_TYPE_SEEK, SEQUENCE_LIMIT,
};
pub use iterator::MemTableIterator;
pub(crate) use key_bytes::InternalKeyBytes;
pub use range_tombstone::{
    max_covering_range_tombstone_seq, range_covers, range_overlaps, RangeTombstoneRecord,
};
pub(crate) use skiplist_rep::SharedSkipMap;
pub use table::{ImmutableMemTable, MemTable, PointState};
