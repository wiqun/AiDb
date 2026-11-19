//! Bloom 过滤器长期统计回归测试.
//! @component aidb-filter
//!
//! 验证 BloomFilter 的假阳性率在统计意义上与理论值一致.
//!
//! ```bash
//! cargo test --test regression bloom -- --test-threads=1
//! ```

use aidb::config::Options;
use aidb::engine::filter::bloom::bloom_false_positive_count;
use aidb::engine::filter::{BloomFilter, Filter};
use aidb::DB;
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// 辅助函数
// ---------------------------------------------------------------------------

/// 根据 bits_per_key 计算理论最低假阳性率:
/// p ≈ exp(-bits_per_key × ln(2)^2)
fn theoretical_fpr(bits_per_key: f64) -> f64 {
    const LN2_SQ: f64 = 0.4804530139182014; // ln(2)^2
    (-bits_per_key * LN2_SQ).exp()
}

/// 对给定 BloomFilter 测量假阳性率.
///
/// - `filter`: 已插入键的过滤器
/// - `test_keys`: 测试查询的不存在键数量
/// - `run_offset`: 用于生成测试键的种子偏移 (确保不与训练集重叠)
fn measure_fpr(filter: &BloomFilter, test_keys: usize, run_offset: u64) -> f64 {
    let mut false_positives = 0u64;
    for i in 0..test_keys {
        let key = format!("{:020x}", run_offset + i as u64);
        if filter.may_contain(key.as_bytes()) {
            false_positives += 1;
        }
    }
    false_positives as f64 / test_keys as f64
}

/// 单次 Bloom filter 运行: 构建 → 插入 → 零假阴性检查 → 返回假阳性率.
fn run_single_test(
    expected_keys: usize,
    false_positive_rate: f64,
    test_keys: usize,
    run_id: u64,
) -> f64 {
    let mut filter = BloomFilter::new(expected_keys, false_positive_rate);
    for i in 0..expected_keys {
        let key = format!("{:020x}", run_id * 1_000_000 + i as u64);
        filter.add(key.as_bytes());
    }
    // 零假阴性验证: 抽样检查插入的键都能被识别
    for i in 0..expected_keys.min(200) {
        let key = format!("{:020x}", run_id * 1_000_000 + i as u64);
        assert!(
            filter.may_contain(key.as_bytes()),
            "false negative at run {run_id}, key {i}"
        );
    }
    // 假阳性测量
    measure_fpr(&filter, test_keys, run_id + 1_000_000_000)
}

// ---------------------------------------------------------------------------
// Test 1: 统计回归 — 固定参数 100 次独立运行
// ---------------------------------------------------------------------------

/// 验证 100 次独立运行下 BloomFilter 平均假阳性率符合统计回归预置
#[test]
fn test_bloom_statistical_regression() {
    const EXPECTED_KEYS: usize = 10_000;
    const FPR: f64 = 0.01;
    const RUNS: usize = 100;
    const TEST_KEYS: usize = 10_000;
    const THEORETICAL_FPR: f64 = 0.01;

    let mut rates = Vec::with_capacity(RUNS);

    for run in 0..RUNS {
        let rate = run_single_test(EXPECTED_KEYS, FPR, TEST_KEYS, run as u64);
        rates.push(rate);
    }

    let mean = rates.iter().sum::<f64>() / rates.len() as f64;
    let max_rate = rates.iter().copied().fold(0.0_f64, f64::max);

    eprintln!("=== Bloom Statistical Regression ===");
    eprintln!("RUNS: {RUNS}, expected_keys: {EXPECTED_KEYS}, theoretical FPR: {THEORETICAL_FPR}");
    eprintln!("Mean FPR: {mean:.6}");
    eprintln!("Max FPR: {max_rate:.6}");
    eprintln!("Mean / Theoretical: {:.3}", mean / THEORETICAL_FPR);
    eprintln!("Max / Theoretical: {:.3}", max_rate / THEORETICAL_FPR);

    assert!(
        mean < THEORETICAL_FPR * 2.5,
        "Mean FPR {mean:.6} >= {:.6} (2.5x theoretical {THEORETICAL_FPR})",
        THEORETICAL_FPR * 2.5
    );
    assert!(
        max_rate < THEORETICAL_FPR * 5.0,
        "Max single-run FPR {max_rate:.6} >= {:.6} (5x theoretical {THEORETICAL_FPR})",
        THEORETICAL_FPR * 5.0
    );
}

// ---------------------------------------------------------------------------
// Test 2: 参数化 FPR — 不同 bits_per_key × 不同键数量
// ---------------------------------------------------------------------------

/// 验证 参数化不同 bits_per_key 与键规模下假阳性率在理论阈值内
#[test]
fn test_bloom_parameterized_fpr() {
    let cases = [
        (8usize, 1_000usize),
        (8, 10_000),
        (8, 100_000),
        (10, 1_000),
        (10, 10_000),
        (10, 100_000),
        (12, 1_000),
        (12, 10_000),
        (12, 100_000),
        (14, 1_000),
        (14, 10_000),
        (14, 100_000),
    ];

    for &(bits_per_key, num_keys) in &cases {
        let theoretical = theoretical_fpr(bits_per_key as f64);
        const REPEATS: usize = 10;
        const TEST_KEYS_PER_RUN: usize = 5_000;

        let mut run_results = Vec::with_capacity(REPEATS);
        for run in 0..REPEATS {
            let mut filter = BloomFilter::new(num_keys, theoretical);
            for i in 0..num_keys {
                let key = format!("{:020x}", run as u64 * 1_000_000 + i as u64);
                filter.add(key.as_bytes());
            }
            let run_offset = run as u64 + 1_000_000_000;
            let fpr = measure_fpr(&filter, TEST_KEYS_PER_RUN, run_offset);
            run_results.push(fpr);
        }

        let mean_fpr = run_results.iter().sum::<f64>() / run_results.len() as f64;
        // Cache-line blocked bloom 用「块内负载不均」换取单次 cache line 访问, 目标 FPR 越低
        // 相对经典 bloom 的放大越明显 (实测 14 bits/key 约 2.9x), 阈值放宽到 3.5x.
        let threshold = theoretical * 3.5;

        eprintln!(
            "bits_per_key={bits_per_key}, keys={num_keys}: theoretical={theoretical:.6}, \
       mean FPR={mean_fpr:.6}, ratio={:.3}",
            mean_fpr / theoretical
        );

        assert!(
      mean_fpr < threshold,
      "bits_per_key={bits_per_key}, keys={num_keys}: mean FPR {mean_fpr:.6} >= {threshold:.6} \
       (3.5x theoretical {theoretical:.6})"
    );
    }
}

// ---------------------------------------------------------------------------
// Test 3: 通过 DB API 验证 bloom_false_positive_count 单调递增
// ---------------------------------------------------------------------------

/// 验证 通过 DB API 查询调用时全局 bloom_false_positive_count 计数器单调递增
#[test]
fn test_bloom_false_positive_counter() {
    let dir = tempdir().unwrap();
    let mut opts = Options::for_testing();
    opts.bloom_false_positive_rate = 0.01;
    opts.use_wal = false;

    let db = DB::open(dir.path(), opts).unwrap();

    let before = bloom_false_positive_count();

    // 写入 1000 个键
    for i in 0..1000 {
        let key = format!("key_{i:04x}");
        let _ = db.put(key.as_bytes(), b"v");
    }
    let _ = db.flush();

    // 查询 1000 个不存在的键, 触发可能的 bloom 假阳性
    for i in 0..1000 {
        let key = format!("absent_{i:04x}");
        let _ = db.get(key.as_bytes());
    }

    let after = bloom_false_positive_count();
    eprintln!(
        "bloom_false_positive_count: before={before}, after={after}, delta={}",
        after.saturating_sub(before)
    );

    // 计数器应单调递增 (>=)
    assert!(
        after >= before,
        "bloom_false_positive_count decreased: {after} < {before}"
    );

    let _ = db.close();
    // dir 超出作用域时 tempdir 自动清理
}

// ---------------------------------------------------------------------------
// Test 4: 压力测试 (#[ignore])
// ---------------------------------------------------------------------------

/// 验证 100 万大条目量级下 BloomFilter 假阳性率压力测试
#[test]
#[ignore = "stress: 1M keys bloom FPR sampling"]
fn test_bloom_stress() {
    const NUM_KEYS: usize = 1_000_000;
    const FPR: f64 = 0.01;
    const BATCH_SIZE: usize = 1_000;

    let mut filter = BloomFilter::new(NUM_KEYS, FPR);
    let mut false_positives: u64 = 0;
    let mut test_count: u64 = 0;

    for i in 0..NUM_KEYS {
        let key = format!("{:020x}", i as u64);
        filter.add(key.as_bytes());

        if (i + 1) % BATCH_SIZE == 0 {
            for j in 0..BATCH_SIZE {
                let qk = format!("{:020x}", (NUM_KEYS + j) as u64);
                if filter.may_contain(qk.as_bytes()) {
                    false_positives += 1;
                }
                test_count += 1;
            }
        }
    }

    let fpr = false_positives as f64 / test_count as f64;
    let theoretical = FPR;

    eprintln!("=== Bloom Stress Test ===");
    eprintln!("Keys: {NUM_KEYS}, FPR: {fpr:.6}, Theoretical: {theoretical}");
    eprintln!("Ratio: {:.3}", fpr / theoretical);

    assert!(
        fpr < theoretical * 2.0,
        "Stress test FPR {fpr:.6} >= {:.6} (2x theoretical {theoretical})",
        theoretical * 2.0
    );
}
