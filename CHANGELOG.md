# Changelog

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/),
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/).

## [Unreleased]

### Added

- `MultiRaftNode::scan_local_user_pairs_limited` / `scan_local_user_pairs_limited_sync`: 读取本地 group 状态机中用户 key 半开区间内至多 `limit` 条记录, 支持反向 (从区间末尾向前), 只读取需要的条数, 供有序索引 (如 ZSet score 索引) 的 TopN 与分页读取.
- 范围墓碑 (RangeTombstone) 全面接入: `DB::delete_range` 以 O(1) 写入半开区间删除标记, 不扫描区间; 读路径按层维护 `FragmentedRangeTombstones` 索引, 点查先放下索引锁再按层二分, 不随 SST 文件数遍历; 压缩在没有快照且没有幸存文件与 `[start, end)` 重叠时丢弃标记, 否则按标记区间裁剪写出; 选压缩输入时, 点 key 范围之外被墓碑区间盖住的文件也会拉进任务. 子压缩把跨出本段的标记截成落在 `[range_start, range_end)` 内的片段, 由下一段写出剩下的尾巴. Raft 日志 `purge` / `truncate_after` 改用范围墓碑批量删, 避免日志累积后逐条删除.
- `LogCommitterHandle::purge` 接收本次清理的 `start_index`, 与 `log_id` 一起下发给 committer, 由它在一次 `delete_range` 里删掉 `[start_index, log_id.index]`.

### Changed

- `ThinWriteOp::Put` 与 `Request::Put` / `Request::PutConditional` 的 value 类型由 `Vec<u8>` 改为 `bytes::Bytes`, 并给 `bytes` 启用 `serde` feature; `ThinWriteBatch::put` 接受 `impl Into<Bytes>`. apply 路径可以直接复用收到的字节缓冲, 不再逐次拷贝整条 value.

### Performance

- `InternalKeySlice` 零分配跳表定位与 `consume_memtable_group_backward` 链表优先反向回退: 引入 `#[repr(transparent)] InternalKeySlice([u8])` 统一委托 `compare_internal_key`, 消除 `MemTable::search` / `get` / `point_state` / `scan_limited` 的 `Arc<[u8]>` 堆分配; 反向有界扫描在同 key 版本数不超过 8 时直接沿 `prev()` 顺序回退, 超长版本组自动切换 `upper_bound` 跳跃, 并完善 `scan_limited` 的 `sst_overlaps` 区间判定与 `DBIterator::new_unpositioned` 构造.
- `scan_memtable_limited_backward`: 实现纯内存跳表原生有界反向扫描与快速跳跃, 状态机 `last_sm_user_pair` 接入, 反向遍历耗时由 122µs 降至 3.45µs.
- `DB::scan_limited` 与 `scan_memtable_limited_forward`: 新增纯内存跳表单向链表遍历原生有界扫描快路径, 消除跳表重复二分查找, 堆内存分配与墓碑多版本归并读放大; 集群状态机 `scan_sm_user_pairs_limited` 全面接入该快路径, 使消费队列 `ZRANGEBYSCORE` 单次扫描耗时从 1.78ms 降低至 200µs 以内.

### Fixed

- 修复超过 gRPC 单消息上限的快照无法发给新副本: 整包一次塞进 `InstallSnapshot`, 约 263MB 的状态机在 64MB 上限处被 HTTP/2 复位, 从节点一直收不到. 现在按 8MB 分片顺序拼包, 拼满才安装; 安装要写回整个数据目录, 这一次 RPC 单独用 120s 超时, 不改心跳超时. 回归测试: `cluster::network::server::tests::snapshot_chunks_reassemble_in_order`.
- 修复主节点做完快照后无法把快照发给新副本: `build_snapshot` 只把 meta 交回 openraft, 没有写入 `snapshot_meta`, 也没有留下字节. 日志被 purge 之后, 落后副本只能靠快照追平, `get_current_snapshot` 却返回 None, 复制报 `snapshot not found` 并且 group 进入 fatal. 现在构建结束时落盘 meta 并记住字节, 发送时原样返回. 回归测试: `cluster::storage::snapshot::tests::built_snapshot_can_be_sent_to_a_follower`.
- 修复 `truncate_after` 删掉该保留的最后一条 raft 日志: openraft 的契约是 "Truncate logs after `last_log_id`, exclusive", 即 `last_log_id` 本身必须保留. 原实现从 `lid.index` 开始 `delete_range`, 把这条也删了, 日志中间留下一个洞 —— openraft 仍认为该位点存在并据此 apply, 实际从存储取回的 entry 数少于请求区间, 触发 `core/sm/worker.rs` 的 `assert_eq!(end - 1, got_last_index)`; debug 构建下 core worker 任务 panic 后无人回收, 集群集成测试表现为永久卡死. 现在 committer 分支与非 committer 分支都从 `lid.index + 1` 开始删, `last_log_id` 回写 `lid.index` 这一条. 回归测试: `cluster::log_committer::tests::test_truncate_after` 与 `cluster::storage::log::tests::truncate_after_keeps_last_log_id_without_hole` (修复前均失败). 同一个 bug 也让 `truncate_after` 在 `cluster::integration` / `linearizable_read` / `partition` 三个子模块里卡死超过 30 分钟.
- 修复 raft 日志在落盘前的读空洞: `append` 在提交时就把 `last_log_id` 推进到本批末尾, 而 committer 提取批次时用 `drain_range` 立刻把 entry 移出 `PendingLogOverlay`, 此刻 `db.write` 还没返回. 这段窗口里 `get_log_entries` 既不在 overlay 也读不到 DB, openraft 据此 apply 一个残缺区间, 同样触发上述断言 (release 构建没有该断言, 会把未落盘的 entry 当成已 apply). 现在提取批次改为 `get_range` 取副本, 只在本轮 `db.write` 成功后用 `mark_durable` 按 generation 移除. 回归测试: `cluster::log_committer::tests::failed_flush_keeps_entries_in_overlay` (修复前失败).
- 修复写路径 (`WriteThread` Group Commit) 的 use-after-free 竞态: Leader 把 Follower 置为 `COMPLETED`/`FAILED` 后才对其 `Writer.unparker` 调用 `unpark()`, 而 Follower 每 50µs 轮询状态, 可能已返回并销毁栈上的 `Writer`, Leader 随后在被复用的内存上永久阻塞, 表现为整个服务卡死 (线上 gdb 现场: `Mutex::lock_contended`). 现在发布终态前先克隆 `Unparker`, 并把发布逻辑收敛到 `publish_follower_state`; 新增 `WriteGroupLeader::complete()` / `fail()`. 回归测试: `write_thread::tests::leader_must_not_touch_follower_writer_after_publishing_terminal_state` 与 `concurrent_writers_all_complete`.
- 修复子压缩分裂边界按 InternalKey 比较导致范围墓碑丢失: 分裂点是 user key, `MergeIterator::with_range` 的 `range_end` 过滤必须比较 user key. InternalKey 尾部是取反的 sequence, 空 user key 的范围墓碑内部键形如 `0xff..`, 字节序上大于任意普通 user key, 按字节比较会把跨段的墓碑整条漏在左侧子任务之外, 被它覆盖的 key 全部复活. 回归测试: `engine::compaction::merge::tests::test_merge_with_range_keeps_prefix_range_tombstone` 与 `test_merge_with_range_keeps_tombstone_whose_start_is_prefix_of_end`.
- 修复快照位点可能领先于快照内容: `build_snapshot` 在 `db.flush()` 之后、读取 `last_applied` 之前没有与 apply 互斥, 并发 apply 写入的新 key 与推进后的 `last_applied` 会让 `SnapshotMeta.last_log_id` 大于 bundle 实际位点; openraft 随后按该位点清理 raft 日志, 重启时这段日志既不在 SST 也不在日志里, 造成数据丢失且 group 在启动阶段 panic 卡死. 现在 `apply_entries_internal` 持 `capture_lock` 读锁, `build_snapshot` 持写锁. 回归测试: `cluster::storage::snapshot::tests::test_snapshot_meta_matches_bundle_content_under_concurrent_apply`.
- 修复子压缩切分点在平均记录较大时静默失效: `count_dedup_with_splits` 按 `file_size / 100` 估算总条目数并据此放置切分间隔, 记录压缩后 (如 2KB value 压到几百字节/条) 估算值会高估一个数量级, 迭代计数永远够不到间隔, `splits` 为空 —— `num_splits` 报 2 却只写出 1 个文件, 多线程压缩退化成单写手, 还白做一遍计数扫描. 现在改为单遍扫描时按输出序号均匀采样候选切分点 (样本超上限时步长翻倍, 内存有界), 扫描结束后总数已知, 再按 `count * k / num_splits` 取最近样本, 不再依赖任何条目数估算. 回归测试: `engine::compaction::job::tests::split_points_survive_large_values` (修复前只产出 1 个文件), 并补上 "切分 + 活跃快照 + 跨段范围墓碑" 这一此前无覆盖的组合: `split_with_snapshot_keeps_pinned_versions_and_stitches_tombstone` 与 `engine::db::inner::compaction::tests::compaction_pins_active_snapshot_and_reload_has_no_resurrection`.
- 修复范围墓碑覆盖裁剪丢掉快照需要的最新版本: `RangeTombstoneTracker::covers` 只比较 `tombstone.sequence >= point.sequence`, 不看 `min_snapshot_sequence`. 墓碑 seq 大于最小快照位点时, 位点在 `[min_snapshot_sequence, tombstone.sequence)` 之间的快照看不到这次删除, 却需要被覆盖的最新版本 —— 该版本被裁掉后这类快照会读到更旧的值; 若该 key 只有这一个版本, 则直接读不到. 现在只有 `sequence <= min_snapshot_sequence` (对每个活跃快照都可见) 的墓碑才允许裁剪. 回归测试: `engine::compaction::job::tests::range_tombstone_must_not_drop_version_needed_by_snapshot`.
