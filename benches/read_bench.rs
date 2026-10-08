//! Phase 7.6 read benchmarks (criterion).
//!
//! Preload uses WriteBatch chunks (500 keys/batch). Default 10_000 keys (smoke/regression).
//! Every 1000 keys calls `db.flush()` during setup (avoids immutable MemTable backlog stall).
//!
//! Environment:
//!   AIDB_BENCH_PRELOAD — override preload size (e.g. 100_000 for larger read working set)
//!
//! Smoke:
//!   cargo bench --bench read_bench

use std::time::Duration;

use aidb::config::Options;
use aidb::{WriteBatch, DB};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::Rng;
use tempfile::tempdir;

const BATCH_SIZE: u64 = 500;
const DEFAULT_PRELOAD_KEYS: u64 = 10_000;
const VALUE_1KB: [u8; 1024] = [0u8; 1024];

fn preload_keys() -> u64 {
    std::env::var("AIDB_BENCH_PRELOAD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PRELOAD_KEYS)
}

fn preload_db(db: &DB, count: u64) {
    let mut written = 0u64;
    while written < count {
        let batch_end = (written + BATCH_SIZE).min(count);
        let mut batch = WriteBatch::new();
        for i in written..batch_end {
            batch.put(format!("key_{:05}", i).as_bytes(), VALUE_1KB.as_slice());
        }
        db.write(&batch).unwrap();
        written = batch_end;
        if written.is_multiple_of(1000) || written == count {
            db.flush().unwrap();
        }
    }
}

fn bench_read(c: &mut Criterion) {
    let preload = preload_keys();
    let mut group = c.benchmark_group("read");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(10);

    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), Options::for_testing()).unwrap();
    preload_db(&db, preload);
    db.flush().unwrap();

    let keys: Vec<String> = (0..preload).map(|i| format!("key_{:05}", i)).collect();
    let mut rng = rand::thread_rng();

    group.bench_function("random_get_1kb", |b| {
        b.iter(|| {
            let idx = rng.gen_range(0..preload as usize);
            let key = &keys[idx];
            let result = db.get(black_box(key.as_bytes())).unwrap();
            black_box(result);
        });
    });

    group.bench_function("sequential_scan_100", |b| {
        b.iter(|| {
            let mut it = db.scan(Some(b"key_00000"), Some(b"key_00100")).unwrap();
            while let Some(Ok((k, v))) = it.next() {
                black_box((k, v));
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_read);
criterion_main!(benches);
