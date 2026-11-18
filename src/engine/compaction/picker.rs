//! Leveled compaction 文件选择: 判断何时触发 compaction 并挑选参与文件 (`CompactionPicker`).
//!
//! L0 优先: 文件数 >= `level0_compaction_trigger` 时整层为输入, 并扩展 L1 overlap;
//! 否则按 `size / target_size` 选 score 最高的 L1+ 层级 (target =
//! `max_bytes_for_level_base × max_bytes_for_level_multiplier^(n-1)`, L0 target 无上限).
//! 与目标层无 overlap 且输入为单文件时得到 trivial move (rename 提升, 不重写).
//!
//! # Invariant
//!
//! - L0 允许多文件 overlap, 作为整体参与 compaction; L1+ 同层目标不重叠.
//! - overlap 判断基于 SST meta 中 smallest / largest 的 raw user_key range.

use super::helpers::key_ranges_overlap_by_meta_raw;
use crate::config::Options;
use crate::engine::sstable::SSTableReader;
use std::sync::Arc;

#[derive(Clone)]
pub struct CompactionTask {
    pub inputs: Vec<Arc<SSTableReader>>,
    pub level: usize,
    pub output_level: usize,
    pub expanded_inputs: Vec<Arc<SSTableReader>>,
    pub grandparents: Vec<Arc<SSTableReader>>,
    pub is_trivial_move: bool,
}

pub struct CompactionPicker {
    level0_compaction_trigger: usize,
    max_bytes_for_level_base: u64,
    max_bytes_for_level_multiplier: u64,
    max_levels: usize,
    dynamic_level_bytes: bool,
}

impl CompactionPicker {
    pub fn from_options(options: &Options) -> Self {
        Self {
            level0_compaction_trigger: options.level0_compaction_trigger,
            max_bytes_for_level_base: options.max_bytes_for_level_base as u64,
            max_bytes_for_level_multiplier: options.max_bytes_for_level_multiplier.max(1) as u64,
            max_levels: options.max_levels,
            dynamic_level_bytes: options.level_compaction_dynamic_level_bytes,
        }
    }

    pub fn level0_compaction_trigger(&self) -> usize {
        self.level0_compaction_trigger
    }

    /// 计算动态层目标模式下的 L0 目标层 (`base_level`).
    /// 未启用 `level_compaction_dynamic_level_bytes` 时恒返回 `1`.
    pub fn base_level(&self, levels: &[Vec<Arc<SSTableReader>>]) -> usize {
        let bottom = self.max_levels.min(levels.len()).saturating_sub(1);
        if !self.dynamic_level_bytes || bottom <= 1 {
            return 1;
        }
        let Some(deepest) = (1..=bottom).rfind(|&l| !levels[l].is_empty()) else {
            return bottom;
        };
        let mut cur = Self::calculate_level_size(&levels[deepest]);
        let mut base = deepest;
        while base > 1 && cur / self.max_bytes_for_level_multiplier > self.max_bytes_for_level_base
        {
            cur /= self.max_bytes_for_level_multiplier;
            base -= 1;
        }
        base
    }

    pub fn target_size_for_level(&self, level: usize) -> u64 {
        if level == 0 {
            return u64::MAX;
        }
        self.max_bytes_for_level_base
            * self
                .max_bytes_for_level_multiplier
                .pow(level.saturating_sub(1) as u32)
    }

    fn dynamic_target_size(&self, levels: &[Vec<Arc<SSTableReader>>], level: usize) -> u64 {
        if !self.dynamic_level_bytes {
            return self.target_size_for_level(level);
        }
        let base = self.base_level(levels);
        if level < base {
            return 0;
        }
        self.max_bytes_for_level_base.saturating_mul(
            self.max_bytes_for_level_multiplier
                .pow((level - base) as u32),
        )
    }

    pub fn calculate_level_size(level: &[Arc<SSTableReader>]) -> u64 {
        level.iter().map(|r| r.file_size()).sum()
    }

    pub fn pick_compaction(&self, levels: &[Vec<Arc<SSTableReader>>]) -> Option<CompactionTask> {
        if levels.is_empty() {
            return None;
        }
        // L0 优先: 文件数触发 (读写阻塞比 L1+ 写放大更紧迫)
        if levels[0].len() >= self.level0_compaction_trigger {
            return self.pick_level0(levels);
        }
        // 选 score 最高的层级 (size / target_size).
        // u64 as f64: 尾数 64 位 > f64 尾数 53 位, level size > 2^53 (≈ 9PB) 时精度受损,
        // 但当前规模下该限制不构成实际问题.
        let mut best_score = 1.0_f64;
        let mut best_level: Option<usize> = None;
        for level in 1..self.max_levels.saturating_sub(1) {
            if level >= levels.len() {
                break;
            }
            let target = self.dynamic_target_size(levels, level);
            let size = Self::calculate_level_size(&levels[level]);
            let score = if target == 0 {
                if size > 0 {
                    f64::MAX / 2.0
                } else {
                    0.0
                }
            } else {
                size as f64 / target as f64
            };
            debug_assert!(score.is_finite(), "score must be finite for u64 inputs");
            if score > best_score {
                best_score = score;
                best_level = Some(level);
            }
        }
        match best_level {
            Some(level) => self.pick_level_n(levels, level),
            None => None,
        }
    }

    fn pick_level0(&self, levels: &[Vec<Arc<SSTableReader>>]) -> Option<CompactionTask> {
        if levels[0].is_empty() {
            return None;
        }
        let inputs = levels[0].clone();
        let output_level = self.base_level(levels);
        let (in_start, in_end) = combined_range(&inputs);
        let mut expanded = overlap_in_level(levels, output_level, in_start, in_end);
        if levels.len() > output_level {
            append_tombstone_overlaps(&levels[output_level], &inputs, &mut expanded);
        }
        let (out_start, out_end) = if expanded.is_empty() {
            (in_start, in_end)
        } else {
            let mut all = inputs.clone();
            all.extend(expanded.iter().cloned());
            // Use combined bounds across inputs + expanded
            let s = all
                .iter()
                .map(|f| f.smallest_key())
                .min()
                .unwrap_or(in_start);
            let e = all.iter().map(|f| f.largest_key()).max().unwrap_or(in_end);
            return Some(CompactionTask {
                grandparents: collect_grandparents(levels, output_level + 1, s, e),
                inputs,
                level: 0,
                output_level,
                expanded_inputs: expanded,
                is_trivial_move: false,
            });
        };
        let grandparents = collect_grandparents(levels, output_level + 1, out_start, out_end);
        let is_trivial_move = expanded.is_empty() && inputs.len() == 1;
        Some(CompactionTask {
            inputs,
            level: 0,
            output_level,
            expanded_inputs: expanded,
            grandparents,
            is_trivial_move,
        })
    }

    fn pick_level_n(
        &self,
        levels: &[Vec<Arc<SSTableReader>>],
        level: usize,
    ) -> Option<CompactionTask> {
        if level + 1 >= levels.len() || levels[level].is_empty() {
            return None;
        }
        let seed = pick_seed_level_n(levels, level);
        let mut inputs = vec![seed.clone()];
        let mut expanded = overlap_with_reader(levels, level + 1, &seed);
        append_tombstone_overlaps(&levels[level + 1], &inputs, &mut expanded);

        if expanded.is_empty() {
            // Trivial move: no overlap with target level, promote without rewriting
            return Some(CompactionTask {
                grandparents: collect_grandparents(
                    levels,
                    level + 2,
                    seed.smallest_key(),
                    seed.largest_key(),
                ),
                inputs,
                level,
                output_level: level + 1,
                expanded_inputs: Vec::new(),
                is_trivial_move: true,
            });
        }

        let (ex_start, ex_end) = combined_range(&expanded);
        for f in &levels[level] {
            if f.file_number() != seed.file_number()
                && key_ranges_overlap_by_meta_raw(
                    ex_start,
                    ex_end,
                    f.smallest_key(),
                    f.largest_key(),
                )
            {
                inputs.push(f.clone());
            }
        }
        append_sources_covered_by_tombstone(&levels[level], &mut inputs, &expanded);
        let (in_start, in_end) = combined_range(&inputs);
        expanded = overlap_in_level(levels, level + 1, in_start, in_end);
        append_tombstone_overlaps(&levels[level + 1], &inputs, &mut expanded);

        let all_start = inputs
            .iter()
            .chain(expanded.iter())
            .map(|f| f.smallest_key())
            .min()
            .unwrap_or(in_start);
        let all_end = inputs
            .iter()
            .chain(expanded.iter())
            .map(|f| f.largest_key())
            .max()
            .unwrap_or(in_end);
        let grandparents = collect_grandparents(levels, level + 2, all_start, all_end);

        Some(CompactionTask {
            inputs,
            level,
            output_level: level + 1,
            expanded_inputs: expanded,
            grandparents,
            is_trivial_move: false,
        })
    }
}

fn combined_range(files: &[Arc<SSTableReader>]) -> (&[u8], &[u8]) {
    let mut start = files[0].smallest_key();
    let mut end = files[0].largest_key();
    for f in files.iter().skip(1) {
        if f.smallest_key() < start {
            start = f.smallest_key();
        }
        if f.largest_key() > end {
            end = f.largest_key();
        }
    }
    (start, end)
}

fn overlap_in_level(
    levels: &[Vec<Arc<SSTableReader>>],
    level: usize,
    start: &[u8],
    end: &[u8],
) -> Vec<Arc<SSTableReader>> {
    if level >= levels.len() {
        return Vec::new();
    }
    levels[level]
        .iter()
        .filter(|f| key_ranges_overlap_by_meta_raw(start, end, f.smallest_key(), f.largest_key()))
        .cloned()
        .collect()
}

fn collect_grandparents(
    levels: &[Vec<Arc<SSTableReader>>],
    gp_level: usize,
    start: &[u8],
    end: &[u8],
) -> Vec<Arc<SSTableReader>> {
    let mut gps = overlap_in_level(levels, gp_level, start, end);
    gps.sort_by(|a, b| a.smallest_key().cmp(b.smallest_key()));
    gps
}

fn append_tombstone_overlaps(
    level: &[Arc<SSTableReader>],
    against: &[Arc<SSTableReader>],
    into: &mut Vec<Arc<SSTableReader>>,
) {
    for file in level {
        if into
            .iter()
            .any(|kept| kept.file_number() == file.file_number())
        {
            continue;
        }
        if against
            .iter()
            .any(|other| tombstone_cross_overlap(other, file))
        {
            into.push(file.clone());
        }
    }
}

fn append_sources_covered_by_tombstone(
    level: &[Arc<SSTableReader>],
    inputs: &mut Vec<Arc<SSTableReader>>,
    expanded: &[Arc<SSTableReader>],
) {
    for file in level {
        if inputs
            .iter()
            .any(|kept| kept.file_number() == file.file_number())
        {
            continue;
        }
        let covered = inputs
            .iter()
            .chain(expanded.iter())
            .any(|other| tombstone_cross_overlap(other, file));
        if covered {
            inputs.push(file.clone());
        }
    }
}

/// 点 key 范围不相交时, 任一侧的范围墓碑 `[start, end)` 仍可能盖住另一侧的点.
fn tombstone_cross_overlap(left: &SSTableReader, right: &SSTableReader) -> bool {
    tombstone_hits_file(left, right) || tombstone_hits_file(right, left)
}

fn tombstone_hits_file(source: &SSTableReader, file: &SSTableReader) -> bool {
    let smallest = file.smallest_key();
    let largest = file.largest_key();
    if smallest.len() < 8 || largest.len() < 8 {
        return true;
    }
    let lower = &smallest[..smallest.len() - 8];
    let upper = &largest[..largest.len() - 8];
    source
        .collect_range_tombstones()
        .iter()
        .any(|(start, end, _)| start.as_slice() <= upper && lower < end.as_slice())
}

fn overlap_with_reader(
    levels: &[Vec<Arc<SSTableReader>>],
    level: usize,
    seed: &SSTableReader,
) -> Vec<Arc<SSTableReader>> {
    overlap_in_level(levels, level, seed.smallest_key(), seed.largest_key())
}

/// 选源层中与目标层重叠最少的文件作为 seed.
/// `usize::MAX` 哨兵确保第一个文件总是被初始 candidate 接受;
/// 后续文件严格按 overlap count 和 file_number tie-break 比较.
/// 平局按 file_number 升序确定, 保证确定性.
fn pick_seed_level_n(levels: &[Vec<Arc<SSTableReader>>], level: usize) -> Arc<SSTableReader> {
    let target = level + 1;
    debug_assert!(target < levels.len(), "target level must exist");
    let mut best = (levels[level][0].clone(), usize::MAX);
    for f in &levels[level] {
        let count = levels[target]
            .iter()
            .filter(|t| {
                key_ranges_overlap_by_meta_raw(
                    f.smallest_key(),
                    f.largest_key(),
                    t.smallest_key(),
                    t.largest_key(),
                )
            })
            .count();
        if count < best.1 || (count == best.1 && f.file_number() < best.0.file_number()) {
            best = (f.clone(), count);
        }
    }
    best.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Options;
    use crate::engine::memtable::{encode_internal_key, ValueType};
    use crate::engine::sstable::{sstable_path, SSTableBuilder, SSTableReader};
    use tempfile::tempdir;

    fn file(dir: &std::path::Path, num: u64, level: usize, key: &[u8]) -> Arc<SSTableReader> {
        let path = sstable_path(dir, num, level);
        let ik = encode_internal_key(key, 1, ValueType::TypePut);
        let mut b =
            SSTableBuilder::new(&path, 512, 16, crate::config::CompressionType::None, 0.0).unwrap();
        b.add(&ik, b"v").unwrap();
        b.finish().unwrap();
        Arc::new(SSTableReader::open(&path, None).unwrap())
    }

    fn empty_levels(n: usize) -> Vec<Vec<Arc<SSTableReader>>> {
        vec![Vec::new(); n]
    }

    #[test]
    fn test_pick_level0() {
        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        opts.level0_compaction_trigger = 2;
        let picker = CompactionPicker::from_options(&opts);
        let mut levels = empty_levels(7);
        levels[0].push(file(dir.path(), 1, 0, b"a"));
        levels[0].push(file(dir.path(), 2, 0, b"b"));
        let task = picker.pick_compaction(&levels).unwrap();
        assert_eq!(task.level, 0);
        assert_eq!(task.inputs.len(), 2);
    }

    #[test]
    fn test_no_compaction_when_below_threshold() {
        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        opts.level0_compaction_trigger = 4;
        let picker = CompactionPicker::from_options(&opts);
        let mut levels = empty_levels(7);
        levels[0].push(file(dir.path(), 1, 0, b"a"));
        assert!(picker.pick_compaction(&levels).is_none());
    }

    fn big_file(
        dir: &std::path::Path,
        num: u64,
        level: usize,
        user_key: &[u8],
        value_len: usize,
    ) -> Arc<SSTableReader> {
        let path = sstable_path(dir, num, level);
        let ik = encode_internal_key(user_key, 1, ValueType::TypePut);
        let val = vec![0u8; value_len];
        let mut b =
            SSTableBuilder::new(&path, 512, 16, crate::config::CompressionType::None, 0.0).unwrap();
        b.add(&ik, &val).unwrap();
        b.finish().unwrap();
        Arc::new(SSTableReader::open(&path, None).unwrap())
    }

    #[test]
    fn test_calculate_level_size() {
        let dir = tempdir().unwrap();
        let a = big_file(dir.path(), 1, 1, b"a", 400);
        let b = big_file(dir.path(), 2, 1, b"b", 400);
        let total = CompactionPicker::calculate_level_size(&[a, b]);
        assert!(total > 800);
    }

    #[test]
    fn test_pick_level1_compaction() {
        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        opts.max_bytes_for_level_base = 500;
        opts.max_bytes_for_level_multiplier = 10;
        let picker = CompactionPicker::from_options(&opts);
        let mut levels = empty_levels(7);
        levels[1].push(big_file(dir.path(), 1, 1, b"a", 400));
        levels[1].push(big_file(dir.path(), 2, 1, b"b", 400));
        let task = picker
            .pick_compaction(&levels)
            .expect("level1 should trigger");
        assert_eq!(task.level, 1);
        assert_eq!(task.output_level, 2);
    }

    #[test]
    fn test_level0_priority_over_level1() {
        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        opts.level0_compaction_trigger = 2;
        opts.max_bytes_for_level_base = 100;
        let picker = CompactionPicker::from_options(&opts);
        let mut levels = empty_levels(7);
        levels[0].push(file(dir.path(), 1, 0, b"a"));
        levels[0].push(file(dir.path(), 2, 0, b"b"));
        levels[1].push(big_file(dir.path(), 3, 1, b"c", 500));
        let task = picker.pick_compaction(&levels).unwrap();
        assert_eq!(task.level, 0);
    }

    #[test]
    fn test_pick_last_level_not_compacted() {
        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        opts.max_levels = 3;
        opts.max_bytes_for_level_base = 100;
        let picker = CompactionPicker::from_options(&opts);
        let mut levels = empty_levels(3);
        levels[2].push(big_file(dir.path(), 1, 2, b"z", 10_000));
        assert!(picker.pick_compaction(&levels).is_none());
    }

    #[test]
    fn test_pick_level_n_key_range_expansion() {
        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        opts.max_bytes_for_level_base = 200;
        let picker = CompactionPicker::from_options(&opts);
        let mut levels = empty_levels(7);
        levels[1].push(big_file(dir.path(), 1, 1, b"a", 300));
        levels[1].push(file(dir.path(), 2, 1, b"m"));
        levels[2].push(file(dir.path(), 3, 2, b"a"));
        let task = picker.pick_compaction(&levels).expect("level1 overflow");
        assert!(!task.inputs.is_empty());
        assert_eq!(task.output_level, 2);
    }

    #[test]
    fn test_pick_level_n_trivial_move() {
        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        opts.max_bytes_for_level_base = 200;
        let picker = CompactionPicker::from_options(&opts);
        let mut levels = empty_levels(7);
        // Level 1 has one file with key "a", level 2 has NO overlapping files
        levels[1].push(big_file(dir.path(), 1, 1, b"a", 300));
        let task = picker.pick_compaction(&levels).expect("level1 overflow");
        assert!(
            task.is_trivial_move,
            "should be trivial move when no overlap"
        );
        assert_eq!(task.inputs.len(), 1);
        assert!(task.expanded_inputs.is_empty());
    }

    #[test]
    fn test_pick_level_n_no_trivial_move_when_overlap() {
        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        opts.max_bytes_for_level_base = 200;
        let picker = CompactionPicker::from_options(&opts);
        let mut levels = empty_levels(7);
        levels[1].push(big_file(dir.path(), 1, 1, b"a", 300));
        levels[2].push(file(dir.path(), 2, 2, b"a")); // overlaps with seed "a"
        let task = picker.pick_compaction(&levels).expect("level1 overflow");
        assert!(
            !task.is_trivial_move,
            "should NOT be trivial move when overlap exists"
        );
    }

    #[test]
    fn test_pick_level0_no_trivial_move_multiple_files() {
        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        opts.level0_compaction_trigger = 2;
        let picker = CompactionPicker::from_options(&opts);
        let mut levels = empty_levels(7);
        levels[0].push(file(dir.path(), 1, 0, b"a"));
        levels[0].push(file(dir.path(), 2, 0, b"z"));
        // L1 empty, but multiple L0 files may overlap each other -> not trivial
        let task = picker.pick_compaction(&levels).unwrap();
        assert!(
            !task.is_trivial_move,
            "multiple L0 files should not be trivial move even when L1 is empty"
        );
    }

    #[test]
    fn test_key_ranges_overlap() {
        use crate::engine::compaction::helpers::key_ranges_overlap_by_meta_raw;
        use crate::engine::memtable::{encode_internal_key, ValueType};
        let a0 = encode_internal_key(b"a", 1, ValueType::TypePut);
        let a1 = encode_internal_key(b"c", 1, ValueType::TypePut);
        let b0 = encode_internal_key(b"b", 1, ValueType::TypePut);
        let b1 = encode_internal_key(b"d", 1, ValueType::TypePut);
        assert!(key_ranges_overlap_by_meta_raw(&a0, &a1, &b0, &b1));
        let z0 = encode_internal_key(b"x", 1, ValueType::TypePut);
        let z1 = encode_internal_key(b"y", 1, ValueType::TypePut);
        assert!(!key_ranges_overlap_by_meta_raw(&a0, &a1, &z0, &z1));
    }

    // --- F-026: Score 排序 ---

    #[test]
    fn test_pick_by_score_multi_level() {
        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        opts.max_bytes_for_level_base = 500;
        opts.max_bytes_for_level_multiplier = 10;
        // L1 target=500, L2 target=5000
        let picker = CompactionPicker::from_options(&opts);
        let mut levels = empty_levels(7);
        // L1: ~600 bytes, score 1.2
        levels[1].push(big_file(dir.path(), 1, 1, b"a", 300));
        levels[1].push(big_file(dir.path(), 2, 1, b"b", 300));
        // L2: ~10000 bytes, score 2.0 — 更紧迫
        levels[2].push(big_file(dir.path(), 3, 2, b"x", 5000));
        levels[2].push(big_file(dir.path(), 4, 2, b"y", 5000));
        let task = picker
            .pick_compaction(&levels)
            .expect("L2 has higher score, should be selected");
        assert_eq!(task.level, 2, "should pick level 2 with higher score");
        assert_eq!(task.output_level, 3);
    }

    // --- F-027: Seed 选择 ---

    #[test]
    fn test_pick_seed_min_overlap() {
        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        opts.max_bytes_for_level_base = 200;
        let picker = CompactionPicker::from_options(&opts);
        let mut levels = empty_levels(7);
        // L1: file("a") + file("m"), L2: file("a") + file("b")
        levels[1].push(big_file(dir.path(), 1, 1, b"a", 300));
        levels[1].push(file(dir.path(), 2, 1, b"m"));
        levels[2].push(file(dir.path(), 3, 2, b"a"));
        levels[2].push(file(dir.path(), 4, 2, b"b"));
        let task = picker.pick_compaction(&levels).expect("level1 overflow");
        // file("a") overlaps 2, file("m") overlaps 0 → seed=file("m") → trivial move
        assert!(
            task.is_trivial_move,
            "seed with zero overlap should be trivial move"
        );
    }

    #[test]
    fn test_pick_seed_tie_break_by_file_number() {
        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        opts.max_bytes_for_level_base = 200;
        let picker = CompactionPicker::from_options(&opts);
        let mut levels = empty_levels(7);
        // L1: file num=2 key="a", file num=1 key="b". L2 无重叠文件.
        levels[1].push(big_file(dir.path(), 2, 1, b"a", 300));
        levels[1].push(big_file(dir.path(), 1, 1, b"b", 300));
        let task = picker.pick_compaction(&levels).expect("level1 overflow");
        assert!(task.is_trivial_move, "no overlap → trivial move");
        // tie-break: 两者 overlap 均为 0, 选 file_number 最小的 (num=1, key="b")
        assert_eq!(task.inputs.len(), 1);
    }

    #[test]
    fn test_pick_seed_expansion_still_works() {
        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        opts.max_bytes_for_level_base = 200;
        let picker = CompactionPicker::from_options(&opts);
        let mut levels = empty_levels(7);
        // L1: file("a", 300B) + file("b", 300B). L2: file("a") + file("b").
        // 两个源文件都与目标层有重叠 → 不走 trivial move → 验证 expansion.
        levels[1].push(big_file(dir.path(), 1, 1, b"a", 300));
        levels[1].push(big_file(dir.path(), 2, 1, b"b", 300));
        levels[2].push(file(dir.path(), 3, 2, b"a"));
        levels[2].push(file(dir.path(), 4, 2, b"b"));
        let task = picker.pick_compaction(&levels).expect("level1 overflow");
        assert!(
            !task.is_trivial_move,
            "all files overlap → must do real compaction"
        );
        assert!(
            !task.inputs.is_empty(),
            "inputs should include overlapping files"
        );
        assert_eq!(task.output_level, 2);
    }

    #[test]
    fn bottom_level_is_not_a_score_source() {
        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        opts.max_levels = 3;
        opts.max_bytes_for_level_base = 500;
        let picker = CompactionPicker::from_options(&opts);
        let mut levels = empty_levels(3);
        levels[2].push(big_file(dir.path(), 1, 2, b"z", 4000));
        assert!(picker.pick_compaction(&levels).is_none());
    }

    #[test]
    fn compaction_into_bottom_includes_overlapping_bottom_files() {
        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        opts.max_levels = 3;
        opts.max_bytes_for_level_base = 500;
        let picker = CompactionPicker::from_options(&opts);
        let mut levels = empty_levels(3);
        levels[1].push(big_file(dir.path(), 1, 1, b"a", 4000));
        levels[2].push(big_file(dir.path(), 2, 2, b"a", 64));
        let task = picker
            .pick_compaction(&levels)
            .expect("level 1 compacts into bottom");
        assert_eq!(task.level, 1);
        assert_eq!(task.output_level, 2);
        assert!(task.expanded_inputs.iter().any(|f| f.file_number() == 2));
    }

    #[test]
    fn tombstone_span_pulls_target_file_outside_point_range() {
        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        opts.level0_compaction_trigger = 1;
        let picker = CompactionPicker::from_options(&opts);
        let mut levels = empty_levels(7);
        levels[0].push(file_with_wide_tombstone(dir.path(), 1, 0));
        levels[1].push(file(dir.path(), 2, 1, b"m"));
        let task = picker.pick_compaction(&levels).expect("l0 compaction");
        assert!(
            task.expanded_inputs.iter().any(|f| f.file_number() == 2),
            "tombstone [a, z) must pull the L1 file at m"
        );
    }

    fn file_with_wide_tombstone(
        dir: &std::path::Path,
        num: u64,
        level: usize,
    ) -> Arc<SSTableReader> {
        let path = sstable_path(dir, num, level);
        let mut builder =
            SSTableBuilder::new(&path, 512, 16, crate::config::CompressionType::None, 0.0).unwrap();
        builder
            .add(
                &encode_internal_key(b"a", 2, ValueType::TypeRangeDelete),
                b"z",
            )
            .unwrap();
        builder
            .add(&encode_internal_key(b"a", 1, ValueType::TypePut), b"v")
            .unwrap();
        builder.finish().unwrap();
        Arc::new(SSTableReader::open(&path, None).unwrap())
    }

    #[test]
    fn dynamic_level_bytes_routes_l0_to_bottom_or_base_level() {
        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        opts.level0_compaction_trigger = 2;
        opts.max_bytes_for_level_base = 1000;
        opts.max_bytes_for_level_multiplier = 10;
        opts.max_levels = 7;
        opts.level_compaction_dynamic_level_bytes = true;
        let picker = CompactionPicker::from_options(&opts);
        let mut levels = empty_levels(7);
        levels[0].push(file(dir.path(), 1, 0, b"a"));
        levels[0].push(file(dir.path(), 2, 0, b"b"));
        // Empty L1..L6 -> base_level must be L6
        assert_eq!(picker.base_level(&levels), 6);
        let task = picker.pick_compaction(&levels).expect("l0 compaction");
        assert_eq!(task.level, 0);
        assert_eq!(task.output_level, 6);

        // When L6 exceeds base (e.g. 50_000 bytes), L5 target = 50_000 / 10 = 5_000 > 1000,
        // L4 target = 500 <= 1000 -> base_level becomes L5.
        levels[6].push(big_file(dir.path(), 3, 6, b"b", 50_000));
        assert_eq!(picker.base_level(&levels), 5);
        let task2 = picker.pick_compaction(&levels).expect("l0 compaction");
        assert_eq!(task2.output_level, 5);
        assert!(!task2.grandparents.is_empty());
    }
}
