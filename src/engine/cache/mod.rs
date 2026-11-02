//! Block Cache: LRU 缓存 SSTable Data Block payload.

pub mod block_cache;
pub mod row_cache;

pub use block_cache::{shard_index, BlockCache, CacheKey, CacheStats, PinGuard};
pub use row_cache::RowCache;
