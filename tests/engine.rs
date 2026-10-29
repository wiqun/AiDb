//! 引擎黑盒: 经 `DB` 公共 API 的场景测试
//! @component aidb-engine
//!
//! ```bash
//! cargo test --test engine -- --test-threads=1
//! cargo test --test engine scenarios
//! cargo test --test engine crash_recovery
//! cargo test --test engine compaction -- --test-threads=1
//! ```

mod common;

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
