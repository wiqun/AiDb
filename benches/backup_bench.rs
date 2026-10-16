//! Phase 18 backup benchmarks (criterion).
//!
//! Measures backup creation throughput for different dataset sizes.
//!
//! Smoke:
//!   cargo bench --bench backup_bench

use std::sync::Arc;
use std::time::Duration;

use aidb::backup::{BackupManager, LocalFileStorage, RetentionPolicy};
use aidb::config::Options;
use aidb::DB;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tempfile::tempdir;

const VALUE_1KB: [u8; 1024] = [0u8; 1024];

fn preload_db(db: &DB, count: u64) {
    let mut written = 0u64;
    while written < count {
        let batch_end = (written + 500).min(count);
        let mut batch = aidb::WriteBatch::new();
        for i in written..batch_end {
            batch.put(format!("key_{:05}", i).as_bytes(), &VALUE_1KB[..]);
        }
        db.write(&batch).unwrap();
        written = batch_end;
        if written.is_multiple_of(1000) || written == count {
            db.flush().unwrap();
        }
    }
}

fn bench_backup(c: &mut Criterion) {
    // ── empty DB backup ──
    let mut group = c.benchmark_group("backup");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(10);

    group.bench_function("create_empty", |b| {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let storage = Arc::new(LocalFileStorage::new(dir.path().join("backups")));
        let manager = BackupManager::new(storage, RetentionPolicy::default());

        b.iter(|| {
            let id = manager.create_backup(black_box(&db)).unwrap();
            manager.delete_backup(id).unwrap();
        });
    });

    // ── 1000 keys backup ──
    group.bench_function("create_1k", |b| {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        preload_db(&db, 1000);
        let storage = Arc::new(LocalFileStorage::new(dir.path().join("backups")));
        let manager = BackupManager::new(storage, RetentionPolicy::default());

        b.iter(|| {
            let id = manager.create_backup(black_box(&db)).unwrap();
            manager.delete_backup(id).unwrap();
        });
    });

    // ── list backups ──
    group.bench_function("list_10", |b| {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let storage = Arc::new(LocalFileStorage::new(dir.path().join("backups")));
        let manager = BackupManager::new(storage.clone(), RetentionPolicy::default());
        for _ in 0..10 {
            let id = manager.create_backup(&db).unwrap();
            manager.delete_backup(id).unwrap_or(());
        }
        drop(manager);

        // Re-create manager pointing to the same storage for list benchmark
        let list_manager = BackupManager::new(storage, RetentionPolicy::default());
        b.iter(|| {
            let list = list_manager.list_backups().unwrap();
            black_box(list);
        });
    });

    group.finish();

    // ── throughput: backup creation with data size ──
    let mut tp = c.benchmark_group("backup_throughput");
    tp.warm_up_time(Duration::from_secs(1));
    tp.measurement_time(Duration::from_secs(2));
    tp.sample_size(10);

    tp.bench_function("create_10k", |b| {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        preload_db(&db, 10_000);
        db.flush().unwrap();
        let storage = Arc::new(LocalFileStorage::new(dir.path().join("backups")));
        let manager = BackupManager::new(storage, RetentionPolicy::default());

        b.iter(|| {
            let id = manager.create_backup(black_box(&db)).unwrap();
            manager.delete_backup(id).unwrap();
        });
    });

    tp.finish();
}

criterion_group!(benches, bench_backup);
criterion_main!(benches);
