//! Compaction 辅助 Tracker: 快照去重与范围墓碑过滤.
//! 拆分自 job.rs, 由 CompactionJob 归并循环内部使用.

/// 追踪同一个 user_key 分组内是否已经保留过"跨越 `min_snapshot_sequence`
/// 边界"的版本 (即 sequence <= `min_snapshot_sequence` 的版本).
///
/// 正确的多版本保留规则是: 对同一个 user_key, 从最新版本开始往老版本扫描,
/// 一旦保留了某个 sequence <= min_snapshot_sequence 的版本, 该版本就是所有
/// 活跃快照 (其边界 >= min_snapshot_sequence) 都能落到的"边界穿越版本",
/// 更老的版本无论 sequence 是多少都不再被任何快照需要, 可以安全丢弃.
///
/// 之前的实现用 `sequence >= min_snapshot_sequence` 作为无状态的逐条判断,
/// 语义反了: 会保留边界*以上*、其实没有任何快照需要的冗余版本, 却在
/// snapshot 边界与该 key 自身版本不精确对齐时 (只要期间有别的 key 写入,
/// 全局 sequence 前进但这个 key 没变, 这种情况在真实工作负载里几乎必然
/// 发生), 把边界*以下*那个真正被需要的版本当成"不受保护"直接丢弃,
/// 导致存活的 snapshot 在 compaction 后读到错误结果 (缺失或读到更新版本).
#[derive(Default)]
pub(super) struct SnapshotDedupTracker {
    crossed: bool,
}

impl SnapshotDedupTracker {
    /// 开始处理一个新的 user_key 分组 (遇到和上一条不同的 user_key 时调用).
    pub(super) fn start_key(&mut self) {
        self.crossed = false;
    }

    /// 本条 entry (无论是否被保留进输出) 是否已经跨过快照保护边界:
    /// 一旦某条 sequence <= min_snapshot_sequence 的版本被观察到, 同一个
    /// user_key 分组内更老的版本都不再需要保留.
    pub(super) fn observe(&mut self, sequence: u64, min_snapshot_sequence: u64) {
        if sequence <= min_snapshot_sequence {
            self.crossed = true;
        }
    }

    /// 是否已经跨过边界 (跨过之后, 后续更老的重复版本应直接丢弃).
    pub(super) fn already_crossed(&self) -> bool {
        self.crossed
    }
}

/// 追踪 compaction 归并时的活跃 range tombstone.
#[derive(Default)]
pub(super) struct RangeTombstoneTracker {
    items: Vec<RangeTombstoneItem>,
}

pub(super) struct RangeTombstoneItem {
    start: Vec<u8>,
    end: Vec<u8>,
    sequence: u64,
}

impl RangeTombstoneTracker {
    pub(super) fn add(&mut self, start: Vec<u8>, end: Vec<u8>, sequence: u64) {
        self.items.push(RangeTombstoneItem {
            start,
            end,
            sequence,
        });
    }

    pub(super) fn advance_past(&mut self, user_key: &[u8]) {
        self.items.retain(|item| item.end.as_slice() > user_key);
    }

    /// 判断 `(user_key, sequence)` 这个点版本是否可以被墓碑裁掉.
    ///
    /// 只有**所有活跃快照都能看到**的那条墓碑才允许裁: 快照位点都不小于
    /// `min_snapshot_sequence`, 所以墓碑 `sequence <= min_snapshot_sequence` 时
    /// 对每个快照都可见. 反过来, 墓碑比最小快照位点新时, 存在 `[min_snapshot_sequence,
    /// tombstone.sequence)` 区间的快照看不到这次删除, 它需要被覆盖的最新版本, 此时
    /// 不能裁 —— 否则那条快照会读到更旧的版本, 或者干脆读不到.
    pub(super) fn covers(
        &self,
        user_key: &[u8],
        sequence: u64,
        min_snapshot_sequence: u64,
    ) -> bool {
        self.items.iter().any(|item| {
            item.sequence <= min_snapshot_sequence
                && item.start.as_slice() <= user_key
                && user_key < item.end.as_slice()
                && item.sequence >= sequence
        })
    }
}
