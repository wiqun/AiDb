# Changelog

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/),
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/).

## [Unreleased]

### Added

- `MultiRaftNode::scan_local_user_pairs_limited` / `scan_local_user_pairs_limited_sync`: 读取本地 group 状态机中用户 key 半开区间内至多 `limit` 条记录, 支持反向 (从区间末尾向前), 只读取需要的条数, 供有序索引 (如 ZSet score 索引) 的 TopN 与分页读取.

### Performance

- `DB::scan_limited` 与 `scan_memtable_limited_forward`: 新增纯内存跳表单向链表遍历原生有界扫描快路径, 消除跳表重复二分查找、堆内存分配与墓碑多版本归并读放大; 集群状态机 `scan_sm_user_pairs_limited` 全面接入该快路径, 使消费队列 `ZRANGEBYSCORE` 单次扫描耗时从 1.78ms 降低至 200µs 以内.

### Fixed

- 修复写路径 (`WriteThread` Group Commit) 的 use-after-free 竞态: Leader 把 Follower 置为 `COMPLETED`/`FAILED` 后才对其 `Writer.unparker` 调用 `unpark()`, 而 Follower 每 50µs 轮询状态, 可能已返回并销毁栈上的 `Writer`, Leader 随后在被复用的内存上永久阻塞, 表现为整个服务卡死 (线上 gdb 现场: `Mutex::lock_contended`). 现在发布终态前先克隆 `Unparker`, 并把发布逻辑收敛到 `publish_follower_state`; 新增 `WriteGroupLeader::complete()` / `fail()`. 回归测试: `write_thread::tests::leader_must_not_touch_follower_writer_after_publishing_terminal_state` 与 `concurrent_writers_all_complete`.
