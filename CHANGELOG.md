# Changelog

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/),
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/).

## [Unreleased]

### Added

- `CompactionFilter::filter_with_reader` 与 `CompactionPointReader`: compaction filter 可点查其它 key 的最新可见版本. 供上层回收孤儿数据 (如 aikv 版本化 subkey).
- `CompactionJob::run_with_reader`: 带点查句柄的 compaction 入口; `run` 行为不变.

### Performance

- `MultiRaftNode::last_local_user_pair`: 半开用户 key 区间内直接定位字典序最大的一条状态机记录.

### Fixed

- compaction filter 删除某 key 的最新版本时, 若该 key 的更老版本仍在快照或幸存文件中, 现在写出同 sequence 的删除标记. 此前旧版本会在 compaction 后重新可见.
