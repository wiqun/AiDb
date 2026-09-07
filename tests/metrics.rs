//! @component aidb-engine
//! Prometheus metrics 验收测试 (需要 `monitoring` feature)
//!
//!   cargo test --test metrics --features monitoring -- --test-threads=1

#[cfg(feature = "monitoring")]
#[path = "modules/metrics/prometheus.rs"]
mod prometheus;

#[cfg(feature = "monitoring")]
#[path = "modules/metrics/statistics_sync_test.rs"]
mod statistics_sync_test;

#[cfg(not(feature = "monitoring"))]
#[path = "modules/metrics/placeholder.rs"]
mod placeholder;

#[path = "modules/metrics/statistics_test.rs"]
mod statistics_test;

#[path = "modules/metrics/lifecycle_test.rs"]
mod lifecycle_test;

#[path = "modules/metrics/engine_stats_test.rs"]
mod engine_stats_test;

#[path = "modules/metrics/wa_bytes_test.rs"]
mod wa_bytes_test;

#[path = "modules/metrics/ra_bytes_test.rs"]
mod ra_bytes_test;

#[path = "modules/metrics/write_stall_test.rs"]
mod write_stall_test;

#[path = "modules/metrics/pending_compaction_test.rs"]
mod pending_compaction_test;

#[cfg(feature = "monitoring")]
#[path = "modules/metrics/otel_export_test.rs"]
mod otel_export_test;

#[path = "modules/metrics/recovery_test.rs"]
mod recovery_test;
