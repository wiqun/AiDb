//! 引擎与存储模块集成测试统一入口
//! @component aidb-engine

mod common;

#[path = "modules/compaction/mod.rs"]
mod modules_compaction;

#[path = "modules/db/mod.rs"]
mod modules_db;

#[path = "modules/snapshot/mod.rs"]
mod modules_snapshot;

#[path = "engine/scenarios.rs"]
mod scenarios;

#[path = "engine/crash_recovery.rs"]
mod crash_recovery;

#[path = "engine/wal_write_batch_boundary.rs"]
mod wal_write_batch_boundary;

#[path = "engine/compaction.rs"]
mod compaction;

#[path = "engine/dataflow.rs"]
mod dataflow;

#[path = "engine/large_flush_repro.rs"]
mod large_flush_repro;

#[path = "engine/recovery_compaction.rs"]
mod recovery_compaction;

#[path = "engine/write_thread.rs"]
mod write_thread;

#[path = "engine/pinnable_slice.rs"]
mod pinnable_slice;

#[cfg(feature = "backup")]
#[path = "engine/backup.rs"]
mod backup;

#[path = "pipeline/wal_memtable.rs"]
mod pipeline_wal_memtable;

#[path = "regression/empty_value_compaction.rs"]
mod regression_empty_value_compaction;

#[path = "regression/bloom.rs"]
mod regression_bloom;

#[path = "regression/reverse_scan.rs"]
mod regression_reverse_scan;

#[path = "regression/concurrent_visibility.rs"]
mod regression_concurrent_visibility;

#[path = "regression/batch_empty_key.rs"]
mod regression_batch_empty_key;

#[path = "modules/cache/block_cache.rs"]
mod cache_block_cache;

#[path = "modules/cache/dataflow.rs"]
mod cache_dataflow;

#[path = "modules/cache/row_cache.rs"]
mod cache_row_cache;

#[path = "modules/filter/bloom.rs"]
mod filter_bloom;

#[path = "modules/filter/dataflow.rs"]
mod filter_dataflow;

#[path = "modules/memtable/dataflow.rs"]
mod memtable_dataflow;

#[path = "modules/memtable/function.rs"]
mod memtable_function;

#[path = "modules/sstable/bloom.rs"]
mod sstable_bloom;

#[path = "modules/sstable/cache.rs"]
mod sstable_cache;

#[path = "modules/sstable/dataflow.rs"]
mod sstable_dataflow;

#[path = "modules/sstable/function.rs"]
mod sstable_function;

#[path = "modules/wal/dataflow.rs"]
pub mod wal_dataflow;

#[path = "modules/wal/function.rs"]
pub mod wal_function;

#[path = "modules/wal/write_batch_boundary.rs"]
pub mod wal_write_batch_boundary_module;
