//! OTel 指标 wiring 验证
//! @component aidb-metrics

use std::sync::atomic::Ordering;
use std::sync::Arc;

use aidb::config::Options;
use aidb::engine::cache::{BlockCache, CacheKey};
use aidb::engine::filter::bloom::bloom_false_positive_count;
use aidb::metrics::testutil;
use aidb::statistics::Statistics;
use aidb::DB;
use bytes::Bytes;
use tempfile::tempdir;

fn cache_key(file_number: u64, offset: u64) -> CacheKey {
    CacheKey {
        file_number,
        offset,
    }
}

#[test]
#[serial_test::serial]
fn test_block_cache_otel_counters_and_size() {
    let exporter = testutil::init_in_memory();
    let stats = Arc::new(Statistics::default());
    let cache = BlockCache::new_with_stats(1024, Some(stats.clone()));
    let k = cache_key(1, 0);

    let hits_before = testutil::counter_sum(&exporter, "aidb_block_cache_hits_total");
    let misses_before = testutil::counter_sum(&exporter, "aidb_block_cache_misses_total");

    cache.insert(k.clone(), Bytes::from_static(b"hello"));
    aidb::metrics::sync_to_otel(&stats);
    assert!(
        (testutil::gauge_value(&exporter, "aidb_block_cache_size_bytes") - cache.size() as f64)
            .abs()
            < f64::EPSILON
    );
    assert_eq!(
        testutil::gauge_value(&exporter, "aidb_block_cache_capacity_bytes"),
        cache.capacity() as f64
    );

    cache.get(k.clone());
    aidb::metrics::sync_to_otel(&stats);
    assert_eq!(
        testutil::counter_sum(&exporter, "aidb_block_cache_hits_total"),
        hits_before + 1
    );

    cache.get(cache_key(2, 0));
    aidb::metrics::sync_to_otel(&stats);
    assert_eq!(
        testutil::counter_sum(&exporter, "aidb_block_cache_misses_total"),
        misses_before + 1
    );

    cache.clear();
    aidb::metrics::sync_to_otel(&stats);
    assert!(testutil::gauge_value(&exporter, "aidb_block_cache_size_bytes").abs() < f64::EPSILON);
}

#[test]
#[serial_test::serial]
fn test_bloom_false_positive_otel_counter() {
    let exporter = testutil::init_in_memory();
    let dir = tempdir().unwrap();
    let mut opts = Options::for_testing();
    opts.bloom_false_positive_rate = 0.01;
    opts.use_wal = false;
    let db = DB::open(dir.path(), opts).unwrap();

    for i in 0..500 {
        db.put(format!("key_{i:04x}").as_bytes(), b"v").unwrap();
    }
    db.flush().unwrap();

    let counter_before = bloom_false_positive_count();
    let prom_before = testutil::counter_sum(&exporter, "aidb_bloom_false_positive_total");
    let stats_before = db.statistics().bloom_false_positive.load(Ordering::Relaxed);
    for i in 0..500 {
        let _ = db.get(format!("absent_{i:04x}").as_bytes());
    }

    aidb::metrics::sync_to_otel(&db.statistics());

    let delta_otel =
        testutil::counter_sum(&exporter, "aidb_bloom_false_positive_total") - prom_before;
    let delta_global = bloom_false_positive_count() - counter_before;
    let delta_stats = db.statistics().bloom_false_positive.load(Ordering::Relaxed) - stats_before;

    // 三方对照断言: OTel == Atomic stats == 全局计数器
    assert_eq!(
        delta_otel, delta_stats,
        "otel bloom counter should track internal atomic stats"
    );
    assert_eq!(
        delta_otel, delta_global,
        "otel bloom counter should track global bloom counter"
    );
    db.close().unwrap();
}

#[test]
#[serial_test::serial]
fn test_db_operation_and_flush_duration_histograms() {
    let exporter = testutil::init_in_memory();
    let dir = tempdir().unwrap();
    let mut opts = Options::for_testing();
    opts.memtable_size = 4096;
    opts.background_compaction = false;
    let db = DB::open(dir.path(), opts).unwrap();

    let client_before = testutil::counter_sum(&exporter, "db.client.operations");

    db.put(b"k", b"v").unwrap();
    aidb::metrics::sync_to_otel(&db.statistics());
    assert_eq!(
        testutil::histogram_count(&exporter, "aidb_operation_duration_seconds"),
        0
    );
    assert!(testutil::counter_sum(&exporter, "db.client.operations") > client_before);

    db.get(b"k").unwrap();
    aidb::metrics::sync_to_otel(&db.statistics());
    assert_eq!(
        testutil::histogram_count(&exporter, "aidb_operation_duration_seconds"),
        0
    );

    db.flush().unwrap();
    aidb::metrics::sync_to_otel(&db.statistics());
    assert!(testutil::gauge_value(&exporter, "aidb_flush_last_duration_seconds") >= 0.0);
    assert_eq!(
        testutil::histogram_count(&exporter, "aidb_flush_duration_seconds"),
        0
    );

    db.close().unwrap();
}
