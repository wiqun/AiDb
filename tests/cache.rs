//! @component aidb-cache
//! Block Cache 模块验收测试
//!
//!   cargo test --test cache -- --test-threads=1

#[path = "modules/cache/block_cache.rs"]
mod block_cache;
mod common;
#[path = "modules/cache/dataflow.rs"]
mod dataflow;
#[path = "modules/cache/row_cache.rs"]
mod row_cache;
