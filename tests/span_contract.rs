//! @component aidb-observability
//! 热路径 span 级别契约测试 (源码扫描, 无需运行服务器)
//!
//! 验证只允许 `db_flush`, `cmp_run`, `raft_propose` 三个 span.
//!
//! ```bash
//! cargo test --test span_contract -- --test-threads=1
//! ```

#[path = "modules/span_contract.rs"]
mod span_contract;
