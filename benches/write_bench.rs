//! Phase 7.6 write benchmarks (criterion).
//!
//! Keys use zero-padding (`key_{:08}` / `key_{:03}`) so lex order matches numeric order.
//! 详设伪代码 `key_{i}` 未补零会在 SST flush 时触发 BlockBuilder 乱序 panic — 实现刻意偏离.
//!
//! `write_batch_100_flush` — 详设要求 `write_batch_100` (仅 batch 写), 但无 flush 时长 warmup
//! 会因 MemTable 版本堆积而卡住; 故以 `*_flush` 扩展名交付 batch+落盘 smoke.
//!
//! Smoke:
//!   cargo bench --bench write_bench

use std::time::Duration;

use aidb::config::Options;
use aidb::{WriteBatch, DB};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use tempfile::tempdir;

fn bench_options() -> Options {
    let mut opts = Options::for_testing();
    // Larger MemTable reduces background flush churn during short smoke runs.
    opts.memtable_size = 16 * 1024 * 1024;
    opts
}

fn bench_write_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_sequential");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(10);
    group.throughput(Throughput::Elements(1));

    group.bench_function("put_1kb", |b| {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path(), bench_options()).unwrap();
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key_{:08}", i);
            let value = black_box(vec![0u8; 1024]);
            db.put(black_box(key.as_bytes()), black_box(&value))
                .unwrap();
            i += 1;
        });
    });

    group.bench_function("write_batch_100_flush", |b| {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path(), bench_options()).unwrap();
        b.iter(|| {
            let mut batch = WriteBatch::new();
            for i in 0..100u64 {
                batch.put(
                    format!("key_{:03}", i).as_bytes(),
                    black_box(vec![0u8; 1024]),
                );
            }
            db.write(black_box(&batch)).unwrap();
            db.flush().unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_write_sequential);
criterion_main!(benches);
