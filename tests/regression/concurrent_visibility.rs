//! 并发可见性回归: 单 key `put` 与多 key `WriteBatch` 并发写入时, 快照与迭代器不得出现
//! 序号断层或批次残缺.
//! @component aidb-engine
//!
//! ```bash
//! cargo test --test regression concurrent_visibility -- --test-threads=1
//! ```

use aidb::config::Options;
use aidb::{WriteBatch, DB};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tempfile::tempdir;

const BATCH_SIZE: usize = 32;
const ROUNDS: usize = 100;
/// 末尾抽查窗口: 只检查最近完成的写入, 控制每轮断言成本.
const CHECK_TAIL: u64 = 256;

fn open_db() -> (tempfile::TempDir, Arc<DB>) {
    let dir = tempdir().unwrap();
    let mut opts = Options::for_testing();
    // 拉长组提交窗口: WAL 已追加但尚未 sync 的写入会停留更久, 放大并发窗口.
    opts.sync_wal = true;
    opts.group_commit_batch_us = 2_000;
    let db = DB::open(dir.path(), opts).unwrap();
    (dir, db)
}

fn single_key(i: u64) -> Vec<u8> {
    format!("s{i:08}").into_bytes()
}

fn batch_key(j: u64, k: usize) -> Vec<u8> {
    format!("b{j:08}_{k:02}").into_bytes()
}

/// 后台写入负载: 一个线程循环单 key `put`, 一个线程循环 `WriteBatch` (每批 `BATCH_SIZE` 个 key).
/// `*_done` 在对应写入返回之后才推进, 因此读到的计数对应"已返回"的写入.
struct Workload {
    stop: Arc<AtomicBool>,
    single_done: Arc<AtomicU64>,
    batch_done: Arc<AtomicU64>,
    handles: Vec<JoinHandle<()>>,
}

impl Workload {
    fn start(db: Arc<DB>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let single_done = Arc::new(AtomicU64::new(0));
        let batch_done = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();

        {
            let db = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            let done = Arc::clone(&single_done);
            handles.push(thread::spawn(move || {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    db.put(&single_key(i), b"v").unwrap();
                    i += 1;
                    done.store(i, Ordering::SeqCst);
                }
            }));
        }
        {
            let stop = Arc::clone(&stop);
            let done = Arc::clone(&batch_done);
            handles.push(thread::spawn(move || {
                let mut j = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let mut batch = WriteBatch::new();
                    for k in 0..BATCH_SIZE {
                        batch.put(batch_key(j, k), b"v".to_vec());
                    }
                    db.write(&batch).unwrap();
                    j += 1;
                    done.store(j, Ordering::SeqCst);
                }
            }));
        }

        Self {
            stop,
            single_done,
            batch_done,
            handles,
        }
    }

    fn finish(self) {
        self.stop.store(true, Ordering::Relaxed);
        for h in self.handles {
            h.join().unwrap();
        }
    }
}

/// 断言每个批次要么全部可见, 要么全部不可见.
fn assert_batches_atomic(keys: &[Vec<u8>]) {
    let mut seen: BTreeMap<u64, usize> = BTreeMap::new();
    for key in keys.iter().filter(|k| k.first() == Some(&b'b')) {
        let j: u64 = std::str::from_utf8(&key[1..9]).unwrap().parse().unwrap();
        *seen.entry(j).or_default() += 1;
    }
    for (j, n) in seen {
        assert_eq!(n, BATCH_SIZE, "批次 {j} 只可见 {n}/{BATCH_SIZE} 个 key");
    }
}

/// 断言创建读视图前已返回的写入 (末尾窗口内) 全部可见.
fn assert_completed_visible(keys: &BTreeSet<Vec<u8>>, done_s: u64, done_b: u64) {
    for i in done_s.saturating_sub(CHECK_TAIL)..done_s {
        assert!(
            keys.contains(&single_key(i)),
            "已返回的单 key 写入 {i} 在之后创建的读视图中不可见"
        );
    }
    for j in done_b.saturating_sub(CHECK_TAIL)..done_b {
        for k in 0..BATCH_SIZE {
            assert!(
                keys.contains(&batch_key(j, k)),
                "已返回的批次 {j} 第 {k} 个 key 在之后创建的读视图中不可见"
            );
        }
    }
}

/// bug: 单 key `put` 在 WAL sync 等待期间尚未插入 MemTable, 而序号更高的 `WriteBatch` 已插入并推进
/// `committed_sequence`; 此时创建的快照只见批次不见该 `put`, 其后 `put` 插入 (序号低于快照边界) 会使
/// 同一快照的扫描结果发生变化 (迟到插入).
/// 期望: 同一快照两次扫描结果一致; 创建快照前已返回的写入全部可见; 批次要么全部可见要么全部不可见.
/// Issue: 待补 (未提供编号)
#[test]
fn snapshot_scan_is_stable_under_concurrent_put_and_batch() {
    let (_dir, db) = open_db();
    let work = Workload::start(Arc::clone(&db));
    for _ in 0..ROUNDS {
        let done_s = work.single_done.load(Ordering::SeqCst);
        let done_b = work.batch_done.load(Ordering::SeqCst);
        let snap = db.snapshot().unwrap();
        let first: Vec<Vec<u8>> = snap
            .scan(None, None)
            .unwrap()
            .map(|r| r.unwrap().0)
            .collect();
        thread::sleep(Duration::from_millis(10));
        let second: Vec<Vec<u8>> = snap
            .scan(None, None)
            .unwrap()
            .map(|r| r.unwrap().0)
            .collect();
        if first != second {
            let seen: BTreeSet<&Vec<u8>> = first.iter().collect();
            let late: Vec<String> = second
                .iter()
                .filter(|k| !seen.contains(k))
                .take(3)
                .map(|k| String::from_utf8_lossy(k).into_owned())
                .collect();
            panic!(
                "同一快照两次扫描结果不同 (迟到插入): 首次 {} 条, 再次 {} 条, 迟到可见示例 {late:?}",
                first.len(),
                second.len()
            );
        }
        assert_batches_atomic(&first);
        let set: BTreeSet<Vec<u8>> = first.into_iter().collect();
        assert_completed_visible(&set, done_s, done_b);
    }
    work.finish();
}

/// bug: `iter` / `scan` 以已分配的 `sequence` 为可见上界, 而 `WriteBatch` 在写锁内逐条插入 MemTable,
/// 并发迭代器可能只看到批次的一部分; 单 key `put` 未插入时也可能被更高序号的批次边界越过.
/// 期望: 迭代器输出中每个批次要么完整要么不可见; 创建迭代器前已返回的写入必须可见.
/// Issue: 待补 (未提供编号)
#[test]
fn scan_sees_whole_batches_and_completed_writes_under_concurrency() {
    let (_dir, db) = open_db();
    let work = Workload::start(Arc::clone(&db));
    for _ in 0..ROUNDS {
        let done_s = work.single_done.load(Ordering::SeqCst);
        let done_b = work.batch_done.load(Ordering::SeqCst);
        let it = db.scan(None, None).unwrap();
        // 惰性迭代: 分段消费并让出时间片, 使迭代期间的并发写入有机会推进.
        let mut keys: Vec<Vec<u8>> = Vec::new();
        for item in it {
            keys.push(item.unwrap().0);
            if keys.len().is_multiple_of(1024) {
                thread::sleep(Duration::from_micros(200));
            }
        }
        assert_batches_atomic(&keys);
        let set: BTreeSet<Vec<u8>> = keys.into_iter().collect();
        assert_completed_visible(&set, done_s, done_b);
    }
    work.finish();
}
