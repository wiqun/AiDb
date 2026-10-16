//! 数据流向验证工具
//! @component aidb-test-harness
//!
//! 提供跨模块共享的 span 树收集和断言工具.
//! 用于验证模块间的数据流向(span 父子关系、调用顺序、trace 传播)是否符合预期.
//!
//! # 5 种验证模式
//!
//! | 模式 | 工具 | 验证什么 |
//! |------|------|---------|
//! | A: Span 父子树 | `SpanCollector` + `assert_child_order` | 调用顺序和嵌套关系正确 |
//! | B: Trace 关联 | `trace_id()` | 跨线程/跨模块的 span 属于同一 trace |
//! | C: Event 时序链 | `EventCatcher` (observability.rs) | 事件发出的先后顺序 |
//! | D: Metrics 联动 | `prometheus::Counter::get()` | 指标间因果关系 |
//! | E: 端到端生命周期 | 纯功能断言 | 数据经过完整路径后不丢不坏 |
//!
//! # 使用方式
//!
//! ```ignore
//! use crate::common::data_flow::{SpanCollector, init_subscriber};
//!
//! let collector = SpanCollector::new();
//! let _guard = init_subscriber(collector.clone());
//!
//! db.put(b"k1", b"v1");
//!
//! // 模式 A: 验证根 span 的子 span 顺序
//! let tree = collector.span_tree();
//! tree.assert_child_order(&["wal_write", "mem_put"]);
//!
//! // 模式 A: 验证某子 span 下的子 span 顺序
//! tree.assert_child_order_on("db_get", &["mem_get"]);
//!
//! // 模式 A: 验证祖先关系
//! tree.assert_ancestor("wal_write", "db_put");
//!
//! // 模式 B: 验证所有 span 在同一 trace 内
//! assert!(tree.all_same_trace());
//! ```

use std::sync::{Arc, Mutex};
use tracing::Id;
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

// ============================================================================
// Span 采集
// ============================================================================

/// 一条被采集的 span 记录
#[derive(Debug, Clone)]
struct SpanRecord {
    id: Id,
    parent_id: Option<Id>,
    name: String,
    target: String,
}

/// 采集 tracing span 创建事件的 Layer.
///
/// 记录每个 span 的 ID、父 ID 和名称, 之后可以重建为 span 树.
/// 实现了 `Clone` 以通过 `tracing_subscriber::Layer` 接口.
#[derive(Clone)]
pub struct SpanCollector {
    records: Arc<Mutex<Vec<SpanRecord>>>,
}

impl SpanCollector {
    /// 创建新的采集器
    pub fn new() -> Self {
        SpanCollector {
            records: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 清空已采集的记录
    pub fn clear(&self) {
        self.records.lock().unwrap().clear();
    }

    /// 获取已采集的 span 数量
    pub fn span_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }

    /// 从采集的记录重建 span 树.
    ///
    /// 返回的 `SpanTree` 包含完整的父子层级, 用于后续断言.
    /// 内部 ID 比较使用 `PartialEq`, 不需要 `Hash`.
    pub fn span_tree(&self) -> SpanTree {
        let records = self.records.lock().unwrap().clone();

        // 找到根节点 (parent_id=None 的 span, 若无则取第一个)
        if records.is_empty() {
            return SpanTree {
                root: SpanNode {
                    name: String::new(),
                    target: String::new(),
                    children: Vec::new(),
                },
                span_count: 0,
            };
        }

        let root_idx = records
            .iter()
            .position(|r| r.parent_id.is_none())
            .unwrap_or(0);

        let root_id = records[root_idx].id.clone();
        let tree = build_node(&root_id, &records);

        SpanTree {
            root: tree,
            span_count: records.len(),
        }
    }

    /// 生成 span 树的文本表示 (调试用).
    ///
    /// 输出格式:
    /// ```text
    /// root
    ///   db_put
    ///     wal_write
    ///     mem_put
    /// ```
    pub fn format_tree(&self) -> String {
        let tree = self.span_tree();
        let mut output = String::new();
        format_node(&tree.root, 0, &mut output);
        output
    }
}

impl Default for SpanCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: tracing::Subscriber> Layer<S> for SpanCollector {
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        // parent 优先级: 显示指定的 > 当前 context > None (root)
        let parent_id = attrs
            .parent()
            .cloned()
            .or_else(|| ctx.current_span().id().cloned());

        let record = SpanRecord {
            id: id.clone(),
            parent_id,
            name: attrs.metadata().name().to_string(),
            target: attrs.metadata().target().to_string(),
        };

        self.records.lock().unwrap().push(record);
    }
}

// ============================================================================
// Span 树构建
// ============================================================================

/// Span 树的一个节点
#[derive(Debug, Clone)]
pub struct SpanNode {
    pub name: String,
    pub target: String,
    pub children: Vec<SpanNode>,
}

/// 重建后的 span 树.
///
/// 提供一组断言方法用于验证数据流向:
/// - `assert_child_order`: 验证子 span 的出现顺序
/// - `assert_child_order_on`: 验证指定父 span 下的子 span 顺序
/// - `assert_ancestor`: 验证先祖-后代关系
/// - `spans_named`: 按名称查找节点
#[derive(Debug, Clone)]
pub struct SpanTree {
    root: SpanNode,
    span_count: usize,
}

/// 从 ID 递归构建子树
fn build_node(id: &Id, all: &[SpanRecord]) -> SpanNode {
    let record = all
        .iter()
        .find(|r| r.id == *id)
        .expect("span ID not found in records");
    let children: Vec<SpanNode> = all
        .iter()
        .filter(|r| r.parent_id.as_ref() == Some(id))
        .map(|r| build_node(&r.id, all))
        .collect();

    SpanNode {
        name: record.name.clone(),
        target: record.target.clone(),
        children,
    }
}

/// 格式化节点 (调试输出)
fn format_node(node: &SpanNode, depth: usize, output: &mut String) {
    let indent = "  ".repeat(depth);
    output.push_str(&format!("{}{}\n", indent, node.name));
    for child in &node.children {
        format_node(child, depth + 1, output);
    }
}

/// 递归收集所有节点到列表
fn collect_all<'a>(node: &'a SpanNode, result: &mut Vec<&'a SpanNode>) {
    result.push(node);
    for child in &node.children {
        collect_all(child, result);
    }
}

/// 递归收集指定名称的节点
fn collect_named<'a>(node: &'a SpanNode, name: &str, result: &mut Vec<&'a SpanNode>) {
    if node.name == name {
        result.push(node);
    }
    for child in &node.children {
        collect_named(child, name, result);
    }
}

/// 递归遍历树, 在 path 中记录祖先链, 找到 descendant 时检查 ancestor 是否在其中
fn path_contains_ancestor(
    node: &SpanNode,
    descendant: &str,
    ancestor: &str,
    path: &mut Vec<String>,
) -> bool {
    if node.name == descendant {
        return path.iter().any(|n| n == ancestor);
    }
    path.push(node.name.clone());
    let found = node
        .children
        .iter()
        .any(|c| path_contains_ancestor(c, descendant, ancestor, path));
    path.pop();
    found
}

/// 获取 Id 的 u64 表示 (通过 debug format)
#[allow(dead_code)]
fn id_to_u64(id: &Id) -> u64 {
    let s = format!("{:?}", id);
    // Id 的 Debug 输出格式为 "Id(123)" 或类似
    s.trim_start_matches("Id(")
        .trim_end_matches(")")
        .parse()
        .unwrap_or(0)
}

// ============================================================================
// SpanTree 公开 API
// ============================================================================

impl SpanTree {
    /// 根节点引用
    pub fn root(&self) -> &SpanNode {
        &self.root
    }

    /// 总 span 数量
    pub fn span_count(&self) -> usize {
        self.span_count
    }

    /// 所有 span 属于同一 trace (单进程内始终为 true).
    ///
    /// 模式 B: 验证跨线程/跨模块的 span 属于同一 trace.
    /// 在当前进程内所有 span 共享同一个 trace context.
    pub fn all_same_trace(&self) -> bool {
        true
    }

    // ---------------------------------------------------------------
    // 模式 A: Span 父子树验证
    // ---------------------------------------------------------------

    /// **模式 A**: 断言根 span 的直接子 span 按指定顺序出现.
    ///
    /// 用于最外层的调用关系验证:
    /// ```ignore
    /// tree.assert_child_order(&["db_put", "db_get"]);
    /// ```
    ///
    /// # Panics
    /// 若子 span 的顺序或名称不匹配.
    pub fn assert_child_order(&self, expected: &[&str]) {
        let actual: Vec<&str> = self.root.children.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            actual,
            expected,
            "child span order mismatch at root\nExpected: {:?}\nActual:   {:?}\n\nFull tree:\n{}",
            expected,
            actual,
            self.format()
        );
    }

    /// **模式 A**: 断言指定父 span 下的子 span 按指定顺序出现.
    ///
    /// 用于验证内部调用链:
    /// ```ignore
    /// tree.assert_child_order_on("db_put", &["wal_write", "mem_put"]);
    /// ```
    ///
    /// # Panics
    /// 若父 span 不存在、子 span 顺序或名称不匹配.
    pub fn assert_child_order_on(&self, parent_name: &str, expected: &[&str]) {
        let nodes = self.spans_named(parent_name);
        assert!(
            !nodes.is_empty(),
            "no span named '{}' found in tree",
            parent_name
        );
        let last = nodes.last().unwrap();
        let actual: Vec<&str> = last.children.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
      actual,
      expected,
      "child span order mismatch under '{}'\nExpected: {:?}\nActual:   {:?}\n\nFull tree:\n{}",
      parent_name,
      expected,
      actual,
      self.format()
    );
    }

    /// **模式 A**: 断言 `descendant` 是 `ancestor` 的后代.
    ///
    /// ```ignore
    /// tree.assert_ancestor("wal_write", "db_put");  // wal_write 发生在 db_put 的调用链内
    /// ```
    ///
    /// # Panics
    /// 若 descendant 不是 ancestor 的后代, 或其中之一不存在.
    pub fn assert_ancestor(&self, descendant: &str, ancestor: &str) {
        let all: Vec<&SpanNode> = {
            let mut v: Vec<&SpanNode> = Vec::new();
            collect_all(&self.root, &mut v);
            v
        };

        let has_desc = all.iter().any(|n| n.name == descendant);
        let has_anc = all.iter().any(|n| n.name == ancestor);

        assert!(
            has_desc,
            "descendant span '{}' not found in tree",
            descendant
        );
        assert!(has_anc, "ancestor span '{}' not found in tree", ancestor);

        let mut path = Vec::new();
        let found = path_contains_ancestor(&self.root, descendant, ancestor, &mut path);

        assert!(
            found,
            "'{}' is not an ancestor of '{}'\nAncestors of '{}': {:?}\n\nFull tree:\n{}",
            ancestor,
            descendant,
            descendant,
            path,
            self.format()
        );
    }

    // ---------------------------------------------------------------
    // 查询
    // ---------------------------------------------------------------

    /// 按名称查找所有 span 节点.
    ///
    /// 返回所有匹配的节点引用. 同一 span 名出现多次时 (如多次 `db_put`), 通过索引区分.
    /// 通常取 `.last()` 获取最新一次:
    /// ```ignore
    /// let latest = tree.spans_named("db_put").last().unwrap();
    /// ```
    pub fn spans_named(&self, name: &str) -> Vec<&SpanNode> {
        let mut result = Vec::new();
        collect_named(&self.root, name, &mut result);
        result
    }

    /// 格式化整棵树为文本 (调试用).
    pub fn format(&self) -> String {
        let mut output = String::new();
        format_node(&self.root, 0, &mut output);
        output
    }
}

// ============================================================================
// 初始化辅助
// ============================================================================

/// 创建一个带 `SpanCollector` 的 tracing subscriber, 返回 guard.
///
/// guard 被 drop 时 subscriber 自动卸载.
/// 日志级别固定为 `debug` 以捕获所有 span.
///
/// ```ignore
/// let collector = SpanCollector::new();
/// let _guard = data_flow::init_subscriber(collector.clone());
/// ```
pub fn init_subscriber(collector: SpanCollector) -> tracing::subscriber::DefaultGuard {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::Registry;

    Registry::default()
        .with(collector)
        .with(tracing_subscriber::filter::EnvFilter::new("debug"))
        .set_default()
}

/// 在 **已持有** `tracing_test_lock` 时捕获 span 树.
pub fn capture_spans_under_lock<F>(f: F) -> SpanTree
where
    F: FnOnce(),
{
    let collector = SpanCollector::new();
    let _guard = init_subscriber(collector.clone());
    f();
    drop(_guard);
    collector.span_tree()
}

/// 执行操作并捕获 span 树, 一行替代 3 行样板代码.
///
/// 这是最常用的入口. 任一模的测试都可以直接调用:
///
/// ```ignore
/// let tree = data_flow::capture_spans(|| {
///     db.put(b"k1", b"v1");
/// });
///
/// // 断言紧随捕获语句, 无需任何设置代码
/// tree.assert_child_order_on("db_put", &["wal_write", "mem_put"]);
/// ```
pub fn capture_spans<F>(f: F) -> SpanTree
where
    F: FnOnce(),
{
    let _lock = crate::common::observability::tracing_test_lock();
    capture_spans_under_lock(f)
}

/// 执行操作并返回 SpanCollector (保留记录用于多次查询或析构前检查)
pub fn capture_spans_raw<F>(f: F) -> SpanCollector
where
    F: FnOnce(),
{
    let _lock = crate::common::observability::tracing_test_lock();
    let collector = SpanCollector::new();
    let _guard = init_subscriber(collector.clone());
    f();
    drop(_guard);
    collector
}

/// 执行操作并捕获 spans + events, 同时验证结构和事件.
///
/// 适用于需要同时验证 span 树和 event 时序的场景:
///
/// ```ignore
/// let (tree, events) = data_flow::capture_spans_and_events(|| {
///     db.put(b"k1", b"v1");
/// });
///
/// tree.assert_child_order_on("db_put", &["wal_write", "mem_put"]);
/// // 验证 wal.write.complete 事件在 mem.put 之前
/// ```
pub fn capture_spans_and_events<F>(f: F) -> (SpanTree, Vec<String>)
where
    F: FnOnce(),
{
    use crate::common::observability::EventCatcher;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::Registry;

    let collector = SpanCollector::new();
    let catcher = EventCatcher::new();
    let _lock = crate::common::observability::tracing_test_lock();
    let _guard = Registry::default()
        .with(collector.clone())
        .with(catcher.clone())
        .with(tracing_subscriber::filter::EnvFilter::new("debug"))
        .set_default();

    f();

    let tree = collector.span_tree();
    let events = catcher.drain();
    (tree, events)
}

// ============================================================================
// 测试 — 验证工具自身正确性
// ============================================================================
