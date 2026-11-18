//! Raft snapshot 构建与安装 — 以 DB 目录文件级打包传输 (借鉴 Kvrocks/TiKV),
//! 替代逐条 KV 拷贝, 避免 build 端全量 scan 与 install 端逐条 put 的开销.
//!
//! # 数据流
//!
//! ```text
//! build (OpenRaftSnapshotBuilder / get_current_snapshot)
//!   ├─ db.flush() -> 数据全部进 SST
//!   ├─ checkpoint 协议 pin SST (SnapshotCheckpointGuard)
//!   ├─ 收集文件 -> postcard(SnapshotBundle { relative_path, data })
//!   └─ meta = SnapshotMeta { last_log_id, last_membership }
//!
//! install (install_snapshot_atomic)
//!   ├─ 先写临时文件 + fsync 父目录 (crash 安全)
//!   ├─ 关闭旧 DB -> 清空目录 -> 写入接收文件 -> DB::open 重开
//!   ├─ 写 snapshot_meta / last_applied
//!   ├─ 重建 MetaStateMachine (旧实例持有已关闭 DB)
//!   └─ 清空 overlay 与内存态 -> load_state 重载
//! ```
//!
//! # Invariant
//!
//! - install 是"目录整体替换"式原子操作, 失败时清理临时文件.
//! - snapshot 安装后所有 in-memory 缓存 (overlay / StorageState) 一律作废重载.
//! - MetaRaft 也有快照: 目录打包天然包含 `\x00meta_raft/*`, 安装后重建
//!   `MetaStateMachine` 恢复集群元数据.

use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use openraft::{Snapshot, SnapshotMeta};
use serde::{Deserialize, Serialize};

use crate::cluster::storage::keys::snapshot_meta_key;
use crate::cluster::types::TypeConfig;
use crate::error::{ClusterError, Error, Result};
use crate::DB;

use super::{CLId, LIdOf, MOf, OpenRaftStorage, SMOf};

const SNAPSHOT_STREAM_MAGIC: &[u8; 8] = b"AIDBSNAP";
static NEXT_TEMP_SNAP_SEQ: AtomicU64 = AtomicU64::new(1);

pub(crate) fn next_temp_snapshot_path(tag: &str) -> PathBuf {
    let seq = NEXT_TEMP_SNAP_SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "aidb_snap_{}_{}_{}.bin",
        tag,
        std::process::id(),
        seq
    ))
}

#[derive(Debug)]
struct TempSnapshotFile {
    path: PathBuf,
    cleanup: bool,
}

impl Drop for TempSnapshotFile {
    fn drop(&mut self) {
        if self.cleanup {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// 基于磁盘临时文件的流式快照载体, Drop 时自动清理临时文件.
#[derive(Debug, Clone)]
pub struct SnapshotFileData {
    file: Option<Arc<TempSnapshotFile>>,
    len: u64,
}

impl SnapshotFileData {
    pub fn empty() -> Self {
        Self { file: None, len: 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn from_temp_path(path: PathBuf, len: u64) -> Self {
        Self {
            file: Some(Arc::new(TempSnapshotFile {
                path,
                cleanup: true,
            })),
            len,
        }
    }

    pub fn from_bytes_temp(bytes: &[u8]) -> std::io::Result<Self> {
        if bytes.is_empty() {
            return Ok(Self::empty());
        }
        let path = next_temp_snapshot_path("mem");
        std::fs::write(&path, bytes)?;
        Ok(Self::from_temp_path(path, bytes.len() as u64))
    }

    pub fn path(&self) -> Option<&Path> {
        self.file.as_ref().map(|f| f.path.as_path())
    }

    pub fn list_relative_paths(&self) -> Result<Vec<String>> {
        let Some(path) = self.path() else {
            return Ok(Vec::new());
        };
        let mut reader = BufReader::new(std::fs::File::open(path)?);
        let mut magic = [0u8; 8];
        if reader.read_exact(&mut magic).is_ok() && &magic == SNAPSHOT_STREAM_MAGIC {
            let mut count_buf = [0u8; 4];
            reader.read_exact(&mut count_buf)?;
            let count = u32::from_le_bytes(count_buf) as usize;
            let mut out = Vec::with_capacity(count);
            for _ in 0..count {
                let mut plen = [0u8; 4];
                reader.read_exact(&mut plen)?;
                let mut pbuf = vec![0u8; u32::from_le_bytes(plen) as usize];
                reader.read_exact(&mut pbuf)?;
                let rel = String::from_utf8(pbuf)
                    .map_err(|e| Error::Cluster(ClusterError::Serialization(e.to_string())))?;
                let mut flen = [0u8; 8];
                reader.read_exact(&mut flen)?;
                let skip = u64::from_le_bytes(flen);
                std::io::copy(&mut reader.by_ref().take(skip), &mut std::io::sink())?;
                out.push(rel);
            }
            return Ok(out);
        }
        let raw = std::fs::read(path)?;
        let bundle: SnapshotBundle = postcard::from_bytes(&raw)
            .map_err(|e| Error::Cluster(ClusterError::Serialization(e.to_string())))?;
        Ok(bundle.files.into_iter().map(|f| f.relative_path).collect())
    }

    pub fn extract_to_dir(&self, dest_dir: &Path) -> Result<()> {
        let Some(path) = self.path() else {
            return Ok(());
        };
        let mut reader = BufReader::new(std::fs::File::open(path)?);
        let mut magic = [0u8; 8];
        if reader.read_exact(&mut magic).is_ok() && &magic == SNAPSHOT_STREAM_MAGIC {
            let mut count_buf = [0u8; 4];
            reader.read_exact(&mut count_buf)?;
            let count = u32::from_le_bytes(count_buf) as usize;
            for _ in 0..count {
                let mut plen = [0u8; 4];
                reader.read_exact(&mut plen)?;
                let mut pbuf = vec![0u8; u32::from_le_bytes(plen) as usize];
                reader.read_exact(&mut pbuf)?;
                let rel = String::from_utf8(pbuf)
                    .map_err(|e| Error::Cluster(ClusterError::Serialization(e.to_string())))?;
                let mut flen = [0u8; 8];
                reader.read_exact(&mut flen)?;
                let file_len = u64::from_le_bytes(flen);
                let dest = dest_dir.join(&rel);
                if let Some(parent) = dest.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let mut out = BufWriter::new(std::fs::File::create(&dest)?);
                std::io::copy(&mut reader.by_ref().take(file_len), &mut out)?;
                out.flush()?;
            }
            return Ok(());
        }
        let raw = std::fs::read(path)?;
        let bundle: SnapshotBundle = postcard::from_bytes(&raw)
            .map_err(|e| Error::Cluster(ClusterError::Serialization(e.to_string())))?;
        for file in &bundle.files {
            let dest = dest_dir.join(&file.relative_path);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&dest, &file.data)?;
        }
        Ok(())
    }
}

/// snapshot 传输格式: 由原来的逐条 KV (SnapshotKv) 改为 DB 目录文件级打包.
///
/// 借鉴 Kvrocks/TiKV 的做法: flush → SST 文件硬链接 → 发送文件内容 → 接收端替换 DB 目录.
/// 避免 build 端的 db.scan() 全量迭代开销和 install 端的逐条 batch.put() 开销.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SnapshotBundle {
    files: Vec<SnapshotFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SnapshotFile {
    /// DB 目录下的相对路径, 如 "CURRENT", "MANIFEST-000001", "000005_L0.sst"
    pub(crate) relative_path: String,
    pub(crate) data: Vec<u8>,
}

/// 在 `capture_lock` 写锁内抓到的快照"定格"状态.
///
/// 非 SST 文件 (CURRENT / MANIFEST) 会被后续 flush 追加或改写, 必须在锁内读成字节;
/// SST 不可变, 只记路径, 由 `guard` 阻止 compaction 删除, 锁外再慢慢读.
struct CapturedSnapshot {
    inline_files: Vec<SnapshotFile>,
    sst_paths: Vec<(String, std::path::PathBuf)>,
    _guard: SnapshotCheckpointGuard,
}

impl CapturedSnapshot {
    /// 调用方须已 `flush`. 这里只做 O(文件数) 的元数据收集和两个小文件读取.
    fn capture(db: &Arc<DB>) -> Result<Self> {
        let guard = SnapshotCheckpointGuard::new(Arc::clone(db));
        let file_paths = db.collect_checkpoint_file_paths(false)?;
        let db_path = db.path();

        let mut inline_files = Vec::new();
        let mut sst_paths = Vec::with_capacity(file_paths.len());
        for src_path in file_paths {
            let relative = src_path
                .strip_prefix(db_path)
                .map_err(|_| Error::InvalidArgument("checkpoint file outside db dir".into()))?
                .to_string_lossy()
                .to_string();
            if src_path.extension().is_some_and(|e| e == "sst") {
                sst_paths.push((relative, src_path));
            } else {
                let data = std::fs::read(&src_path)?;
                inline_files.push(SnapshotFile {
                    relative_path: relative,
                    data,
                });
            }
        }
        Ok(Self {
            inline_files,
            sst_paths,
            _guard: guard,
        })
    }

    /// 锁外读取 SST 并流式写入临时 bundle 文件 (消除整包内存分配).
    fn into_bundle_file(self) -> Result<SnapshotFileData> {
        let out_path = next_temp_snapshot_path("build");
        let res = (|| -> Result<u64> {
            let file = std::fs::File::create(&out_path)?;
            let mut writer = BufWriter::new(file);
            writer.write_all(SNAPSHOT_STREAM_MAGIC)?;
            let total_files = (self.inline_files.len() + self.sst_paths.len()) as u32;
            writer.write_all(&total_files.to_le_bytes())?;

            for inline in &self.inline_files {
                let rel = inline.relative_path.as_bytes();
                writer.write_all(&(rel.len() as u32).to_le_bytes())?;
                writer.write_all(rel)?;
                writer.write_all(&(inline.data.len() as u64).to_le_bytes())?;
                writer.write_all(&inline.data)?;
            }

            for (relative_path, src_path) in &self.sst_paths {
                let rel = relative_path.as_bytes();
                let mut src = std::fs::File::open(src_path)?;
                let len = src.metadata()?.len();
                writer.write_all(&(rel.len() as u32).to_le_bytes())?;
                writer.write_all(rel)?;
                writer.write_all(&len.to_le_bytes())?;
                std::io::copy(&mut src, &mut writer)?;
            }

            writer.flush()?;
            Ok(writer.get_ref().metadata()?.len())
        })();
        match res {
            Ok(len) => Ok(SnapshotFileData::from_temp_path(out_path, len)),
            Err(e) => {
                let _ = std::fs::remove_file(&out_path);
                Err(e)
            }
        }
    }

    /// 锁外读取 SST 并序列化 (仅测试兼容保留).
    #[cfg(test)]
    fn into_bundle_bytes(self) -> Result<Vec<u8>> {
        let mut files = self.inline_files;
        files.reserve(self.sst_paths.len());
        for (relative_path, path) in &self.sst_paths {
            files.push(SnapshotFile {
                relative_path: relative_path.clone(),
                data: std::fs::read(path)?,
            });
        }
        postcard::to_allocvec(&SnapshotBundle { files })
            .map_err(|e| Error::Cluster(ClusterError::Serialization(e.to_string())))
    }
}

struct SnapshotCheckpointGuard {
    db: Arc<DB>,
}

impl SnapshotCheckpointGuard {
    fn new(db: Arc<DB>) -> Self {
        db.enter_checkpoint();
        Self { db }
    }
}

impl Drop for SnapshotCheckpointGuard {
    fn drop(&mut self) {
        self.db.leave_checkpoint();
    }
}

/// 构建文件级 snapshot bundle (不要求与 apply 互斥, 仅测试用).
/// 先 flush 确保所有数据在 SST 文件中, 再用 checkpoint 协议读文件内容.
#[cfg(test)]
pub(crate) fn prepare_snapshot_bundle(db: &Arc<DB>) -> Result<Vec<u8>> {
    db.flush()?;
    CapturedSnapshot::capture(db)?.into_bundle_bytes()
}

#[derive(Clone)]
pub struct OpenRaftSnapshotBuilder {
    db: super::DbSlot,
    group_id: u64,
    state: std::sync::Arc<parking_lot::RwLock<super::StorageState>>,
    /// 与 `OpenRaftStorage::get_current_snapshot` 共享. 复制落后副本时从这里取回
    /// 刚构建的那一份, 而不是回答「没有快照」.
    built_snapshot: super::BuiltSnapshotSlot,
    /// 与 apply 共享的抓取锁: 抓快照期间持有写锁, 保证 flush 到读取 `last_applied`
    /// 之间没有并发 apply 插入.
    capture_lock: std::sync::Arc<parking_lot::RwLock<()>>,
}

impl OpenRaftSnapshotBuilder {
    pub(crate) fn new(
        db: super::DbSlot,
        group_id: u64,
        state: std::sync::Arc<parking_lot::RwLock<super::StorageState>>,
        built_snapshot: super::BuiltSnapshotSlot,
        capture_lock: std::sync::Arc<parking_lot::RwLock<()>>,
    ) -> Self {
        Self {
            db,
            group_id,
            state,
            built_snapshot,
            capture_lock,
        }
    }

    /// 持久化快照位点并更新内存态. openraft 据此位点 purge raft 日志.
    fn persist_meta(&self, db: &DB, meta: &SMOf) -> std::io::Result<()> {
        let meta_bytes =
            postcard::to_allocvec(meta).map_err(|e| std::io::Error::other(e.to_string()))?;
        db.put(&snapshot_meta_key(self.group_id), &meta_bytes)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        self.state.write().snapshot_meta = Some(meta.clone());
        Ok(())
    }

    /// 留一份完整 bundle 文件句柄, 复制给落后副本时由 `get_current_snapshot` 取回.
    fn remember(&self, db: &DB, meta: &SMOf, snap: &SnapshotFileData) -> std::io::Result<()> {
        self.persist_meta(db, meta)?;
        *self.built_snapshot.write() = Some((meta.clone(), snap.clone()));
        Ok(())
    }
}

impl OpenRaftStorage {
    /// 文件级 snapshot 安装: 关闭旧 DB → 清空目录 → 流式解包新文件 → 重新打开 DB.
    pub(crate) fn install_snapshot_atomic_from_file(
        &mut self,
        meta: &SMOf,
        snap: &SnapshotFileData,
    ) -> Result<()> {
        // 写锁盖住整个换库. 日志线程要读锁才能碰到 DB, 所以 close 不会打到正在 purge 的那次调用.
        let mut slot = self.db.write();
        let old = Arc::clone(&slot);
        old.close()?;

        let db_path = old.path().to_path_buf();
        let options = Arc::clone(old.options());

        // 清空 DB 目录, 保留目录本身.
        if db_path.exists() {
            let dir_entries: Vec<_> = std::fs::read_dir(&db_path)
                .map_err(Error::Io)?
                .filter_map(|e| e.ok())
                .collect();
            for entry in dir_entries {
                let path = entry.path();
                if path.is_dir() {
                    std::fs::remove_dir_all(&path)?;
                } else {
                    std::fs::remove_file(&path)?;
                }
            }
        }

        // 流式解包写入接收到的文件.
        snap.extract_to_dir(&db_path)?;

        // 重新打开 DB: CURRENT + MANIFEST + SST 文件已在目录中.
        let new_db = DB::open(&db_path, (*options).clone())?;

        // 写入 snapshot meta 等元数据 (与原逻辑相同).
        let meta_bytes = postcard::to_allocvec(meta)
            .map_err(|e| Error::Cluster(ClusterError::Serialization(e.to_string())))?;
        new_db.put(&snapshot_meta_key(self.group_id), &meta_bytes)?;
        if let Some(log_id) = meta.last_log_id {
            let la = rmp_serde::to_vec(&log_id)
                .map_err(|e| Error::Cluster(ClusterError::Serialization(e.to_string())))?;
            new_db.put(&super::keys::last_applied_key(self.group_id), &la)?;
        }

        // 重建 MetaStateMachine (旧实例持有已关闭的 DB 引用).
        if self.meta_state.is_some() {
            let new_meta = std::sync::Arc::new(
                crate::cluster::meta_state_machine::MetaStateMachine::new(Arc::clone(&new_db))?,
            );
            self.meta_state = Some(new_meta);
        }
        *slot = new_db;

        let mut state = self.state.write();
        state.snapshot_meta = Some(meta.clone());
        state.last_applied = meta.last_log_id;
        if let Some(log_id) = meta.last_log_id {
            state.last_purged_log_id = Some(log_id);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn install_snapshot_atomic(&mut self, meta: &SMOf, data: &[u8]) -> Result<()> {
        let snap = SnapshotFileData::from_bytes_temp(data)?;
        self.install_snapshot_atomic_from_file(meta, &snap)
    }
}

#[cfg(feature = "cluster")]
impl openraft::RaftSnapshotBuilder<TypeConfig> for OpenRaftSnapshotBuilder {
    type SnapshotData = SnapshotFileData;

    /// openraft 每次日志清理都会调用, 且只取返回值里的 meta, 数据直接丢弃.
    /// 因此这里只 flush 并记位点, 不打包整库; 整库 bundle 见 `build_full_snapshot`.
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<
        Snapshot<CLId, openraft::Membership<u64, openraft::BasicNode>, SnapshotFileData>,
        std::io::Error,
    > {
        self.run_blocking(false).await
    }
}

impl OpenRaftSnapshotBuilder {
    /// 复制给落后副本时才需要: 完整 bundle, meta 与内容来自同一次抓取.
    pub(crate) async fn build_full_snapshot(
        &self,
    ) -> std::io::Result<
        Snapshot<CLId, openraft::Membership<u64, openraft::BasicNode>, SnapshotFileData>,
    > {
        self.run_blocking(true).await
    }

    // flush / 读文件 / 序列化都是同步 IO, 不能占用 tokio worker.
    async fn run_blocking(
        &self,
        full: bool,
    ) -> std::io::Result<
        Snapshot<CLId, openraft::Membership<u64, openraft::BasicNode>, SnapshotFileData>,
    > {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.build_blocking(full))
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?
    }

    fn build_blocking(
        &self,
        full: bool,
    ) -> std::io::Result<
        Snapshot<CLId, openraft::Membership<u64, openraft::BasicNode>, SnapshotFileData>,
    > {
        let io_err = |e: &dyn std::fmt::Display| std::io::Error::other(e.to_string());
        let started = std::time::Instant::now();
        let db = Arc::clone(&self.db.read());

        // 抓快照必须与 apply 互斥. 否则并发 apply 会把新条目写进 memtable 并推进
        // `last_applied`, 随后读到的位点会大于已落盘内容. openraft 会按该位点 purge
        // raft 日志, 于是 (内容, 位点] 这段日志既不在 SST 也不在日志里 —— 重启后出现
        // 空洞, RaftCore 在 apply 时 panic 并且 group 再也无法写.
        //
        // 日常清理 (`full=false`): 锁内只冻结 memtable + 读位点 (微秒级), 落盘放到锁外,
        // 落盘完成后才发布位点, 所以位点之前的数据一定已在 SST. 实测同步 flush 要 2~3 秒.
        // 完整 bundle (`full=true`): 文件清单必须与位点同一时刻, 仍在锁内 flush.
        let lock_started = std::time::Instant::now();
        let (captured, meta, wait_ms) = {
            let _capture = self.capture_lock.write();
            let wait_ms = lock_started.elapsed().as_millis() as u64;
            let captured = if full {
                db.flush().map_err(|e| io_err(&e))?;
                Some(CapturedSnapshot::capture(&db).map_err(|e| io_err(&e))?)
            } else {
                db.freeze_active().map_err(|e| io_err(&e))?;
                None
            };

            let membership: MOf = db
                .get(&super::keys::membership_key(self.group_id))
                .map_err(|e| io_err(&e))?
                .map(|d| postcard::from_bytes(&d).unwrap_or_default())
                .unwrap_or_default();
            let last_applied: Option<LIdOf> = db
                .get(&super::keys::last_applied_key(self.group_id))
                .map_err(|e| io_err(&e))?
                .and_then(|d| rmp_serde::from_slice(&d).ok());
            (
                captured,
                SnapshotMeta {
                    last_log_id: last_applied,
                    last_membership: membership,
                },
                wait_ms,
            )
        };
        let locked_ms = started.elapsed().as_millis() as u64;

        if !full {
            db.flush_frozen().map_err(|e| io_err(&e))?;
        }

        let snap = match captured {
            Some(c) => {
                let snap = c.into_bundle_file().map_err(|e| io_err(&e))?;
                self.remember(&db, &meta, &snap)?;
                snap
            }
            None => {
                self.persist_meta(&db, &meta)?;
                SnapshotFileData::empty()
            }
        };
        tracing::info!(
            group_id = self.group_id,
            full,
            wait_ms,
            locked_ms,
            total_ms = started.elapsed().as_millis() as u64,
            bytes = snap.len(),
            "raft snapshot built"
        );

        Ok(Snapshot {
            meta,
            snapshot: snap,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::meta_state_machine::MetaStateMachine;
    use crate::cluster::meta_types::{MetaRequest, METARAFT_GROUP_ID};
    use crate::cluster::storage::keys::DEFAULT_GROUP_ID;
    use crate::cluster::types::ThinWriteBatch;
    use crate::config::Options;
    use crate::DB;
    use openraft::{LogId, SnapshotMeta, StoredMembership};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_snapshot_install() {
        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let mut storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();

        // 先写入旧数据
        let mut batch = ThinWriteBatch::new();
        batch.put(b"old".to_vec(), b"gone".to_vec());
        storage.apply_batch_to_sm(&batch).unwrap();

        // 构建 snapshot bundle (模拟 SNAPSHOT 构建端).
        let bundle_data = prepare_snapshot_bundle(&storage.db.read()).unwrap();

        let log_id = LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(5), 5);
        let meta = SnapshotMeta {
            last_log_id: Some(log_id),
            last_membership: StoredMembership::default(),
        };
        storage
            .install_snapshot_atomic(&meta, &bundle_data)
            .unwrap();

        assert_eq!(
            storage.get_state_machine_value(b"old").unwrap(),
            Some(b"gone".to_vec())
        );
        assert_eq!(
            storage
                .read_last_applied_from_db()
                .unwrap()
                .map(|id| id.index),
            Some(5)
        );
    }

    #[test]
    fn test_install_snapshot_atomic_crash_safety() {
        let dir = TempDir::new().unwrap();
        let opts = Options::for_testing();

        // 源 DB: 写入数据, 构建 bundle.
        let src_dir = TempDir::new().unwrap();
        let snap_data = {
            let db = DB::open(src_dir.path(), opts.clone()).unwrap();
            let storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();
            let mut b = ThinWriteBatch::new();
            b.put(b"key_a".to_vec(), b"val_a".to_vec());
            b.put(b"key_b".to_vec(), b"val_b".to_vec());
            storage.apply_batch_to_sm(&b).unwrap();
            let db = storage.db.read();
            prepare_snapshot_bundle(&db).unwrap()
        };

        // 安装端: 安装到全新 DB 目录.
        {
            let db = DB::open(dir.path(), opts.clone()).unwrap();
            let mut storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();

            let log_id = LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(5), 5);
            let meta = SnapshotMeta {
                last_log_id: Some(log_id),
                last_membership: StoredMembership::default(),
            };
            storage.install_snapshot_atomic(&meta, &snap_data).unwrap();

            assert_eq!(
                storage.get_state_machine_value(b"key_a").unwrap(),
                Some(b"val_a".to_vec())
            );
            assert_eq!(
                storage.get_state_machine_value(b"key_b").unwrap(),
                Some(b"val_b".to_vec())
            );
            assert_eq!(
                storage
                    .read_last_applied_from_db()
                    .unwrap()
                    .map(|id| id.index),
                Some(5)
            );
        }

        // 重新打开 DB, 确认数据持久化.
        {
            let db = DB::open(dir.path(), opts).unwrap();
            let storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();
            assert_eq!(
                storage.get_state_machine_value(b"key_a").unwrap(),
                Some(b"val_a".to_vec())
            );
            assert_eq!(
                storage.get_state_machine_value(b"key_b").unwrap(),
                Some(b"val_b".to_vec())
            );
        }
    }

    #[test]
    fn test_meta_snapshot_roundtrip() {
        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let meta_sm = Arc::new(MetaStateMachine::new(db.clone()).unwrap());

        // Register a node via MetaStateMachine + persist ApplyOutput to DB
        let output = meta_sm
            .apply_meta_request(MetaRequest::RegisterNode {
                node_id: 1,
                rpc_addr: "http://127.0.0.1:1".into(),
                client_addr: None,
                tags: HashMap::new(),
            })
            .unwrap();
        {
            let mut wb = crate::engine::db::WriteBatch::new();
            for (k, v) in &output.kv_pairs {
                wb.put(k.clone(), v.clone());
            }
            db.write(&wb).unwrap();
        }

        let bundle_data = prepare_snapshot_bundle(&db).unwrap();

        // Fresh DB + install snapshot
        let dir2 = TempDir::new().unwrap();
        let db2 = DB::open(dir2.path(), Options::for_testing()).unwrap();
        let meta_sm2 = Arc::new(MetaStateMachine::new(db2.clone()).unwrap());
        let mut storage2 =
            OpenRaftStorage::new(db2.clone(), METARAFT_GROUP_ID, Some(meta_sm2.clone())).unwrap();

        let log_id = LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(3), 3);
        let snap_meta = SnapshotMeta {
            last_log_id: Some(log_id),
            last_membership: StoredMembership::default(),
        };
        storage2
            .install_snapshot_atomic(&snap_meta, &bundle_data)
            .unwrap();

        // Verify state reloaded correctly
        let recovered = storage2.meta_state.as_ref().unwrap().get_cluster_meta();
        assert_eq!(recovered.nodes.len(), 1);
        assert!(recovered.nodes.contains_key(&1));
        assert_eq!(recovered.cluster_id, "uninitialized");
        assert!(storage2
            .meta_state
            .as_ref()
            .unwrap()
            .get_migration_state()
            .is_none());
    }

    /// 回归: snapshot 的 `meta.last_log_id` 必须等于 bundle 内部记录的 `last_applied`.
    ///
    /// 修复前 `build_snapshot` 先 flush 再读 `last_applied`, 并发 apply 会在这个窗口
    /// 推进位点, 使 meta 领先于 bundle 内容. openraft 随后按 meta 位点 purge raft
    /// 日志, 重启后 (bundle 内容, meta] 既不在 SST 也不在日志里, 直接丢数据.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_snapshot_meta_matches_bundle_content_under_concurrent_apply() {
        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let storage = Arc::new(OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap());

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer = {
            let storage = Arc::clone(&storage);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut index = 0u64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    index += 1;
                    let mut ops = ThinWriteBatch::new();
                    ops.put(b"hot".to_vec(), index.to_be_bytes().to_vec());
                    let entry = crate::cluster::types::LogEntry {
                        log_id: LogId::new(
                            openraft::vote::leader_id_std::CommittedLeaderId::new(1),
                            index,
                        ),
                        payload: openraft::EntryPayload::Normal(
                            crate::cluster::types::Request::WriteBatch(ops),
                        ),
                    };
                    storage.apply_entries_internal(&[entry]).unwrap();
                }
            })
        };

        let mut checked = 0usize;
        for _ in 0..5 {
            let builder = OpenRaftSnapshotBuilder::new(
                Arc::clone(&storage.db),
                storage.group_id,
                Arc::clone(&storage.state),
                Arc::clone(&storage.built_snapshot),
                Arc::clone(&storage.capture_lock),
            );
            let snapshot = builder.build_full_snapshot().await.unwrap();

            let dir = TempDir::new().unwrap();
            snapshot.snapshot.extract_to_dir(dir.path()).unwrap();
            let bundle_db = DB::open(dir.path(), Options::for_testing()).unwrap();
            let bundle_last_applied: Option<LIdOf> = bundle_db
                .get(&crate::cluster::storage::keys::last_applied_key(
                    DEFAULT_GROUP_ID,
                ))
                .unwrap()
                .and_then(|d| rmp_serde::from_slice(&d).ok());

            assert_eq!(
                snapshot.meta.last_log_id, bundle_last_applied,
                "snapshot meta 位点必须等于 bundle 内容位点"
            );
            checked += 1;
        }

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        writer.join().unwrap();
        assert_eq!(checked, 5);
    }

    /// raft 快照 flush 之后数据全在 SST, bundle 不应再搬运历史 WAL.
    #[test]
    fn snapshot_bundle_excludes_wal_files() {
        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        db.put(b"k", b"v").unwrap();

        let bundle: SnapshotBundle =
            postcard::from_bytes(&prepare_snapshot_bundle(&db).unwrap()).unwrap();
        assert!(!bundle.files.is_empty());
        assert!(bundle
            .files
            .iter()
            .all(|f| !f.relative_path.contains("wal")));
    }

    /// 回归: 安装快照会关掉旧 DB 再打开新的. 日志存储是另一份克隆, 必须看到新 DB.
    ///
    /// 修复前 `close()` 打在共享的 `Arc<DB>` 上, 新 DB 只换进状态机那一份克隆.
    /// openraft 紧接着在日志那份上 `purge`, 得到 `database is closed`, group 进入 fatal
    /// 并被卸掉, 复制端就看到 `group not found` 然后重试.
    #[tokio::test]
    async fn log_store_can_purge_after_snapshot_install() {
        use crate::cluster::log_committer::{spawn_committer, LogCommitterConfig};

        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let slot = Arc::new(parking_lot::RwLock::new(db));
        let committer = spawn_committer(
            DEFAULT_GROUP_ID,
            Arc::clone(&slot),
            LogCommitterConfig::default(),
        );
        let mut storage =
            OpenRaftStorage::new_with_committer(slot, DEFAULT_GROUP_ID, None, Some(committer))
                .unwrap();
        let log_store = storage.clone();

        let bundle = prepare_snapshot_bundle(&storage.db.read()).unwrap();
        let log_id = LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(1), 3);
        let meta = SnapshotMeta {
            last_log_id: Some(log_id),
            last_membership: StoredMembership::default(),
        };
        storage.install_snapshot_atomic(&meta, &bundle).unwrap();

        log_store
            .committer
            .as_ref()
            .unwrap()
            .purge(0, log_id)
            .await
            .expect("安装完成后日志库必须还能 purge");
        log_store
            .db()
            .get(&crate::cluster::storage::keys::snapshot_meta_key(
                DEFAULT_GROUP_ID,
            ))
            .expect("日志库看到的必须是安装后的新 DB");
    }

    /// 回归: 本机 `build_snapshot` 之后, `get_current_snapshot` 必须能把同一份快照交出去.
    ///
    /// 修复前 meta 和字节都没留下, 这里返回 None. 日志一旦 purge, 新副本只能靠这份
    /// 快照追平, openraft 读到 None 就报 `snapshot not found` 并把 group 打成 fatal.
    #[tokio::test]
    async fn built_snapshot_can_be_sent_to_a_follower() {
        use openraft::storage::RaftStateMachine;
        use openraft::RaftSnapshotBuilder;

        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let mut storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();

        let mut ops = ThinWriteBatch::new();
        ops.put(b"k".to_vec(), b"v".to_vec());
        let entry = crate::cluster::types::LogEntry {
            log_id: LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(1), 7),
            payload: openraft::EntryPayload::Normal(crate::cluster::types::Request::WriteBatch(
                ops,
            )),
        };
        storage.apply_entries_internal(&[entry]).unwrap();

        let built = storage
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        let sent = storage
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("构建后的快照必须还能读到");
        assert_eq!(sent.meta.last_log_id, built.meta.last_log_id);
        assert_eq!(sent.meta.last_log_id.map(|id| id.index), Some(7));
        // openraft 丢弃 `build_snapshot` 的数据只取 meta, 所以日常清理不打包整库;
        // 真正要发给副本时 `get_current_snapshot` 才现做完整 bundle.
        assert!(built.snapshot.is_empty());
        let paths = sent.snapshot.list_relative_paths().unwrap();
        assert!(paths.iter().any(|p| p.ends_with(".sst")));
    }

    #[test]
    fn snapshot_file_data_cleans_up_temp_file_on_drop() {
        let snap = SnapshotFileData::from_bytes_temp(b"hello-stream").unwrap();
        let path = snap.path().unwrap().to_path_buf();
        assert!(path.exists());
        drop(snap);
        assert!(!path.exists(), "temp snapshot file must be cleaned on drop");
    }

    /// 日常快照在发布位点前必须把冻结的数据落盘, 否则 purge 之后重启会丢这段状态.
    #[tokio::test]
    async fn build_snapshot_flushes_frozen_data_before_publishing_meta() {
        use openraft::storage::RaftStateMachine;
        use openraft::RaftSnapshotBuilder;

        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let mut storage = OpenRaftStorage::new(Arc::clone(&db), DEFAULT_GROUP_ID, None).unwrap();

        let mut ops = ThinWriteBatch::new();
        ops.put(b"k".to_vec(), b"v".to_vec());
        let entry = crate::cluster::types::LogEntry {
            log_id: LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(1), 9),
            payload: openraft::EntryPayload::Normal(crate::cluster::types::Request::WriteBatch(
                ops,
            )),
        };
        storage.apply_entries_internal(&[entry]).unwrap();
        assert_eq!(db.level0_sstable_count(), 0);

        storage
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        assert!(db.level0_sstable_count() >= 1, "位点发布前数据必须已在 SST");
    }

    /// 日常 `build_snapshot` 只 flush 并记位点, 不读 SST, 也不留整库内存副本.
    #[tokio::test]
    async fn build_snapshot_is_meta_only_and_does_not_cache_bundle() {
        use openraft::storage::RaftStateMachine;
        use openraft::RaftSnapshotBuilder;

        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let mut storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();

        let mut ops = ThinWriteBatch::new();
        ops.put(b"k".to_vec(), b"v".to_vec());
        let entry = crate::cluster::types::LogEntry {
            log_id: LogId::new(openraft::vote::leader_id_std::CommittedLeaderId::new(1), 3),
            payload: openraft::EntryPayload::Normal(crate::cluster::types::Request::WriteBatch(
                ops,
            )),
        };
        storage.apply_entries_internal(&[entry]).unwrap();

        let built = storage
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        assert_eq!(built.meta.last_log_id.map(|id| id.index), Some(3));
        assert!(built.snapshot.is_empty());
        assert!(storage.built_snapshot.read().is_none());
        assert_eq!(
            storage
                .state
                .read()
                .snapshot_meta
                .as_ref()
                .and_then(|m| m.last_log_id)
                .map(|id| id.index),
            Some(3)
        );
    }
}
