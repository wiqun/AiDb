//! 把 `LatencySnapshot` 渲染成 Prometheus 累计桶文本. 不调用 OTel.

use super::duration::{LatencySnapshot, COMMAND_DURATION_BOUNDS_US};

/// `le` 用秒, 桶计数是前缀和. `_sum` 是观测微秒之和换成秒.
pub fn render_histogram(name: &str, labels: &str, snap: &LatencySnapshot) -> String {
    let mut cumulative = 0u64;
    let mut out = String::new();
    for (i, bound_us) in COMMAND_DURATION_BOUNDS_US.iter().enumerate() {
        cumulative += snap.buckets[i];
        let le = format_seconds(*bound_us);
        out.push_str(&bucket_line(name, labels, &le, cumulative));
    }
    cumulative += snap.buckets[COMMAND_DURATION_BOUNDS_US.len()];
    out.push_str(&bucket_line(name, labels, "+Inf", cumulative));
    let sum_secs = snap.sum_us as f64 / 1_000_000.0;
    out.push_str(&format!("{name}_sum{} {sum_secs}\n", label_prefix(labels)));
    out.push_str(&format!(
        "{name}_count{} {}\n",
        label_prefix(labels),
        snap.count
    ));
    out
}

fn bucket_line(name: &str, labels: &str, le: &str, cumulative: u64) -> String {
    if labels.is_empty() {
        format!("{name}_bucket{{le=\"{le}\"}} {cumulative}\n")
    } else {
        format!("{name}_bucket{{{labels},le=\"{le}\"}} {cumulative}\n")
    }
}

fn label_prefix(labels: &str) -> String {
    if labels.is_empty() {
        String::new()
    } else {
        format!("{{{labels}}}")
    }
}

fn format_seconds(bound_us: u64) -> String {
    let secs = bound_us as f64 / 1_000_000.0;
    let text = format!("{secs}");
    text
}
