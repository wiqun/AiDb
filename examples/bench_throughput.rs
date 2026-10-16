//! AiDb 端到端并发吞吐性能基准测试 (bench_throughput).
//!
//! 用于评估 Atomic-first 指标体系相较于旧 OTel 直调路径的性能改善.
//! 严格仅使用双端 (origin/main 与当前分支) 均支持的公共 API.

use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

const NUM_THREADS: usize = 8;
const OPS_PER_THREAD: usize = 125_000; // 8 * 125,000 = 1,000,000 总操作数
const TOTAL_OPS: usize = NUM_THREADS * OPS_PER_THREAD;
const PREHEAT_OPS: usize = 10_000;
const VALUE_SIZE: usize = 128;
const NUM_ROUNDS: usize = 3;

struct BenchmarkResult {
    qps: f64,
    avg_us: f64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
}

fn run_single_round(use_wal: bool, round: usize) -> BenchmarkResult {
    let dir = TempDir::new().expect("create temp dir");
    let opts = aidb::config::Options {
        create_if_missing: true,
        use_wal,
        sync_wal: false,
        memtable_size: 128 * 1024 * 1024, // 128MB 充足 memtable，专注于纯路径损耗对比
        ..Default::default()
    };

    let db = Arc::new(aidb::DB::open(dir.path(), opts).expect("open aidb"));

    // 预热阶段: 写入 10,000 条
    let dummy_val = vec![b'x'; VALUE_SIZE];
    for i in 0..PREHEAT_OPS {
        let key = format!("preheat_{:08}", i);
        let _ = db.put(key.as_bytes(), &dummy_val);
    }

    // 8 线程并发执行 1,000,000 次操作 (50% Put, 50% Get)
    let barrier = Arc::new(std::sync::Barrier::new(NUM_THREADS + 1));
    let mut handles = Vec::with_capacity(NUM_THREADS);

    for tid in 0..NUM_THREADS {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);

        handles.push(std::thread::spawn(move || {
            let val = vec![tid as u8 % 26 + b'a'; VALUE_SIZE];
            let mut latencies_us = Vec::with_capacity(OPS_PER_THREAD / 10 + 1);

            // 等待主线程与所有工作线程就绪
            barrier.wait();

            for i in 0..OPS_PER_THREAD {
                let key = format!("key_{:02}_{:08}", tid, i);
                if i % 10 == 0 {
                    let t0 = Instant::now();
                    if i % 2 == 0 {
                        let _ = db.put(key.as_bytes(), &val);
                    } else {
                        let _ = db.get(key.as_bytes());
                    }
                    latencies_us.push(t0.elapsed().as_micros() as u32);
                } else if i % 2 == 0 {
                    let _ = db.put(key.as_bytes(), &val);
                } else {
                    let _ = db.get(key.as_bytes());
                }
            }
            latencies_us
        }));
    }

    // 同步起跑
    barrier.wait();
    let start = Instant::now();

    let mut all_latencies = Vec::with_capacity(TOTAL_OPS / 10 + NUM_THREADS);
    for h in handles {
        let mut lats = h.join().expect("thread join");
        all_latencies.append(&mut lats);
    }
    let total_duration = start.elapsed();
    let qps = TOTAL_OPS as f64 / total_duration.as_secs_f64();
    let avg_us = total_duration.as_micros() as f64 / TOTAL_OPS as f64;

    all_latencies.sort_unstable();
    let n = all_latencies.len();
    let p50_us = all_latencies[n * 50 / 100] as u64;
    let p95_us = all_latencies[n * 95 / 100] as u64;
    let p99_us = all_latencies[n * 99 / 100] as u64;

    println!(
        "  [Round {}] 耗时: {:.3}s, QPS: {:.0}, Avg: {:.2}μs, P50: {}μs, P95: {}μs, P99: {}μs",
        round,
        total_duration.as_secs_f64(),
        qps,
        avg_us,
        p50_us,
        p95_us,
        p99_us
    );

    BenchmarkResult {
        qps,
        avg_us,
        p50_us,
        p95_us,
        p99_us,
    }
}

fn run_suite(scene_name: &str, use_wal: bool) -> BenchmarkResult {
    println!(
        "\n=== 场景: {} ({} 次操作, {} 线程) ===",
        scene_name, TOTAL_OPS, NUM_THREADS
    );
    let mut results = Vec::with_capacity(NUM_ROUNDS);
    for round in 1..=NUM_ROUNDS {
        results.push(run_single_round(use_wal, round));
    }

    // 取 QPS 中位数轮次
    results.sort_by(|a, b| a.qps.partial_cmp(&b.qps).unwrap());
    let median = &results[NUM_ROUNDS / 2];
    println!(
        "--- {} 中位数汇总: QPS: {:.0}, Avg: {:.2}μs, P50: {}μs, P95: {}μs, P99: {}μs ---",
        scene_name, median.qps, median.avg_us, median.p50_us, median.p95_us, median.p99_us
    );

    BenchmarkResult {
        qps: median.qps,
        avg_us: median.avg_us,
        p50_us: median.p50_us,
        p95_us: median.p95_us,
        p99_us: median.p99_us,
    }
}

fn main() {
    #[cfg(feature = "monitoring")]
    {
        let _exporter = aidb::metrics::testutil::init_in_memory();
        println!("==> OTel metrics 真实初始化完成 (SdkMeterProvider 已加载)");
    }

    println!(
        "开始执行吞吐性能压测: 8 线程并发, 总计 1,000,000 操作 (50% Put + 50% Get, Value 128B)"
    );

    let wal_on = run_suite("WAL-on (生产真实场景)", true);
    let wal_off = run_suite("WAL-off (纯内存放大镜场景)", false);

    println!("\n================ 最终基准汇总 ================");
    println!(
        "WAL-on : QPS: {:.0}, P50: {}μs, P95: {}μs, P99: {}μs",
        wal_on.qps, wal_on.p50_us, wal_on.p95_us, wal_on.p99_us
    );
    println!(
        "WAL-off: QPS: {:.0}, P50: {}μs, P95: {}μs, P99: {}μs",
        wal_off.qps, wal_off.p50_us, wal_off.p95_us, wal_off.p99_us
    );
    println!("================================================");
}
