//! aidb: LSM-Tree 存储引擎
//!
//! 公共 API 不超过 30 个函数. 内部模块通过 `pub(crate)` 隔离, 对外不可见.

pub mod config;

#[cfg(feature = "backup")]
pub mod backup;

pub mod engine;
pub mod error;

#[cfg(feature = "monitoring")]
pub mod metrics;

pub use engine::cache::{BlockCache, CacheStats};
pub use engine::checkpoint::Checkpoint;
pub use engine::db::{DbIterGuard, Snapshot, WriteBatch, WriteOp, DB};
pub use error::{Error, Result};

pub mod statistics;
pub use statistics::Statistics;

#[cfg(feature = "cluster")]
pub mod cluster;
