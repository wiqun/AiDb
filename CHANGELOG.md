# Changelog

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/),
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/).

## [Unreleased]

### Fixed

- 修复写路径 (`WriteThread` Group Commit) 的 use-after-free 竞态: Leader 把 Follower 置为 `COMPLETED`/`FAILED` 后才对其 `Writer.unparker` 调用 `unpark()`, 而 Follower 每 50µs 轮询状态, 可能已返回并销毁栈上的 `Writer`, Leader 随后在被复用的内存上永久阻塞, 表现为整个服务卡死 (线上 gdb 现场: `Mutex::lock_contended`). 现在发布终态前先克隆 `Unparker`, 并把发布逻辑收敛到 `publish_follower_state`; 新增 `WriteGroupLeader::complete()` / `fail()`. 回归测试: `write_thread::tests::leader_must_not_touch_follower_writer_after_publishing_terminal_state` 与 `concurrent_writers_all_complete`.
