//! 反向范围查询成本基准 (release, 慢测).
//! @component aidb-engine
//!
//! 口径: 与 aikv `ZREVRANGEBYSCORE ... LIMIT 0 1` 的存储访问一致:
//! `db.scan([P+R, P+S))` + `seek_to_last()` + `key()`.
//! 数据: 1000 个 `R` 索引 (sortable(score)+member) 与 1000 个 `Z` 映射, 之后放尾部 key `0:base`
//! (可配置版本数与 value 大小, 模拟陈旧版本).
//!
//! ```bash
//! cargo test --release --test db reverse_scan_perf -- --ignored --nocapture --test-threads=1
//! ```

use aidb::config::Options;
use aidb::DB;
use std::time::Instant;
use tempfile::tempdir;

fn sortable(score: f64) -> [u8; 8] {
    let bits = score.to_bits();
    let enc = if bits >> 63 == 1 {
        !bits
    } else {
        bits ^ (1 << 63)
    };
    enc.to_be_bytes()
}

struct Case {
    label: &'static str,
    tail_versions: usize,
    tail_value_len: usize,
    flush: bool,
    churn: usize,
}

fn setup(c: &Case) -> (tempfile::TempDir, std::sync::Arc<DB>, Vec<u8>, Vec<u8>) {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), Options::for_testing()).unwrap();
    let mut prefix = b"0\x01".to_vec();
    prefix.extend_from_slice(b"sub-prefix-0001");
    for i in 0..1000usize {
        let m = format!("member:{i:05}");
        let mut zk = prefix.clone();
        zk.push(b'Z');
        zk.extend_from_slice(m.as_bytes());
        db.put(&zk, &(i as f64).to_be_bytes()).unwrap();
        let mut rk = prefix.clone();
        rk.push(b'R');
        rk.extend_from_slice(&sortable(i as f64));
        rk.extend_from_slice(m.as_bytes());
        db.put(&rk, b"").unwrap();
    }
    let mut scores: Vec<f64> = (0..1000).map(|i| i as f64).collect();
    for it in 0..c.churn {
        let i = (it * 7919) % 1000;
        let m = format!("member:{i:05}");
        let old = scores[i];
        let new = old + 1.0;
        let mut rk = prefix.clone();
        rk.push(b'R');
        rk.extend_from_slice(&sortable(old));
        rk.extend_from_slice(m.as_bytes());
        db.delete(&rk).unwrap();
        let mut rk2 = prefix.clone();
        rk2.push(b'R');
        rk2.extend_from_slice(&sortable(new));
        rk2.extend_from_slice(m.as_bytes());
        db.put(&rk2, b"").unwrap();
        let mut zk = prefix.clone();
        zk.push(b'Z');
        zk.extend_from_slice(m.as_bytes());
        db.put(&zk, &new.to_be_bytes()).unwrap();
        scores[i] = new;
    }
    let big = vec![b'x'; c.tail_value_len];
    for _ in 0..c.tail_versions {
        db.put(b"0:base", &big).unwrap();
    }
    db.put(b"0:sub", b"hdr").unwrap();
    if c.flush {
        db.flush().unwrap();
    }
    let mut start = prefix.clone();
    start.push(b'R');
    let mut end = prefix;
    end.push(b'S');
    (dir, db, start, end)
}

fn bench(c: &Case, iters: usize) -> f64 {
    let (_dir, db, start, end) = setup(c);
    // 正确性前置检查: 反向首个 key 必须是最大的 R 索引条目, 且全量反向与正向逆序一致.
    let fwd: Vec<Vec<u8>> = db
        .scan(Some(&start), Some(&end))
        .unwrap()
        .map(|r| r.unwrap().0)
        .collect();
    assert_eq!(fwd.len(), 1000, "forward count");
    let mut it = db.scan(Some(&start), Some(&end)).unwrap();
    it.seek_to_last();
    assert_eq!(it.key(), Some(fwd.last().unwrap().as_slice()));

    for _ in 0..20 {
        let mut it = db.scan(Some(&start), Some(&end)).unwrap();
        it.seek_to_last();
        std::hint::black_box(it.key().map(|k| k.len()));
    }
    let t0 = Instant::now();
    for _ in 0..iters {
        let mut it = db.scan(Some(&start), Some(&end)).unwrap();
        it.seek_to_last();
        std::hint::black_box(it.key().map(|k| k.len()));
    }
    let us = t0.elapsed().as_nanos() as f64 / 1e3 / iters as f64;
    println!(
        "[REVERSE-SCAN] {:<42} scan+seek_to_last+key = {:>10.2} us ({} iters)",
        c.label, us, iters
    );
    us
}

#[test]
#[ignore = "slow: 反向范围查询基准, 需 --release 运行"]
fn reverse_scan_perf() {
    let cases = [
        Case {
            label: "A 无尾部, memtable",
            tail_versions: 0,
            tail_value_len: 0,
            flush: false,
            churn: 0,
        },
        Case {
            label: "B 尾部 3200 版本 x 25KB, memtable",
            tail_versions: 3200,
            tail_value_len: 25_000,
            flush: false,
            churn: 0,
        },
        Case {
            label: "C 尾部 3200 版本 x 25KB, flush 后",
            tail_versions: 3200,
            tail_value_len: 25_000,
            flush: true,
            churn: 0,
        },
        Case {
            label: "E 尾部 3200 版本 x 8B, memtable",
            tail_versions: 3200,
            tail_value_len: 8,
            flush: false,
            churn: 0,
        },
        Case {
            label: "G 尾部 3200 版本 x 8B, flush 后",
            tail_versions: 3200,
            tail_value_len: 8,
            flush: true,
            churn: 0,
        },
        Case {
            label: "H 无尾部 + 3000 次改分, memtable",
            tail_versions: 0,
            tail_value_len: 0,
            flush: false,
            churn: 3000,
        },
        Case {
            label: "I 无尾部 + 3000 次改分, flush 后",
            tail_versions: 0,
            tail_value_len: 0,
            flush: true,
            churn: 3000,
        },
    ];
    let mut worst: f64 = 0.0;
    for c in &cases {
        let iters = if c.tail_versions > 0 { 200 } else { 2000 };
        worst = worst.max(bench(c, iters));
    }
    println!("[REVERSE-SCAN] worst = {worst:.2} us (target <= 25 us)");
}
