//! 目录级 checkpoint: 将整个数据目录 (CURRENT + MANIFEST + WAL + SST) 复制为一致性快照
//! (BGSAVE 语义, 非 Redis RDB), 产物可被 `DB::open` 直接打开.
//!
//! `Checkpoint::create(db, dest)`: flush → `enter_checkpoint` → `pin_sstables` → 收集文件路径
//! → `link_or_copy` 到 tmp 目录 → fsync → 原子 rename 到 dest; `verify_openable` 做 smoke 校验.
//!
//! # Invariant
//!
//! - `checkpoint_in_progress` 使 `run_compaction_once` 直接返回 false, 阻止 compaction 并发改文件.
//! - `pin_sstables` 持有 SST reader 引用, 防止 compaction 在复制期间 unlink 文件.
//! - 跨设备 `hard_link` 失败时 fallback 为 `copy` (见 `docs/modules/02-engine-storage.md`).

use crate::engine::db::DB;
use crate::error::{Error, Result};
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

/// 目录快照 API (Kvrocks/RocksDB checkpoint 同类).
pub struct Checkpoint;

struct CheckpointGuard<'a>(&'a DB);

impl Drop for CheckpointGuard<'_> {
    fn drop(&mut self) {
        self.0.leave_checkpoint();
    }
}

impl Checkpoint {
    /// 创建一致性目录快照. 见 `docs/modules/02-engine-storage.md` 并发协议.
    pub fn create(db: &DB, dest: impl AsRef<Path>) -> Result<PathBuf> {
        let dest = dest.as_ref();

        db.flush()?;

        db.enter_checkpoint();
        let _guard = CheckpointGuard(db);

        // Pin SST readers so compaction cannot unlink while we link/copy.
        let _pinned = db.pin_sstables();

        // 优先在 dest 父目录构建 tmp, 完成后原子 rename 到 dest.
        // 若父目录不可写 (如 backup 目录是容器根挂载点, parent 落在根 /),
        // 退化为在 dest 内部构建, 完成后搬移内容再删除 tmp 目录.
        let parent_tmp = checkpoint_tmp_path(dest);
        let inside_tmp = dest.join(".checkpoint_tmp");
        let (tmp, inside) = match fs::create_dir_all(&parent_tmp) {
            Ok(_) => {
                if parent_tmp.exists() {
                    remove_dir_if_exists(&parent_tmp)?;
                }
                (parent_tmp, false)
            }
            Err(_) => {
                fs::create_dir_all(dest)?;
                if inside_tmp.exists() {
                    remove_dir_if_exists(&inside_tmp)?;
                }
                fs::create_dir_all(&inside_tmp)?;
                (inside_tmp, true)
            }
        };

        let files = db.collect_checkpoint_file_paths()?;
        let file_count = files.len();
        for src in &files {
            let rel = src
                .strip_prefix(db.path())
                .map_err(|_| Error::InvalidArgument("checkpoint path outside db dir".into()))?;
            let dst = tmp.join(rel);
            if let Some(parent) = dst.parent() {
                fs::create_dir_all(parent)?;
            }
            link_or_copy(src, &dst)?;
        }

        sync_dir(&tmp)?;

        if inside {
            // 清理 dest 内旧内容 (保留 tmp), 再把 tmp 内容搬移进 dest.
            for entry in fs::read_dir(dest)? {
                let path = entry?.path();
                if path == tmp {
                    continue;
                }
                if path.is_dir() {
                    fs::remove_dir_all(&path)?;
                } else {
                    fs::remove_file(&path)?;
                }
            }
            for entry in fs::read_dir(&tmp)? {
                let path = entry?.path();
                let rel = path.strip_prefix(&tmp).expect("under tmp");
                let target = dest.join(rel);
                if let Some(parent) = target.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::rename(&path, &target)?;
            }
            fs::remove_dir(&tmp)?;
            sync_dir(dest)?;
        } else {
            if dest.exists() {
                remove_dir_if_exists(dest)?;
            }
            fs::rename(&tmp, dest)?;
            if let Some(parent) = dest.parent() {
                sync_dir(parent)?;
            }
        }

        tracing::info!(
          target: "db",
          dest = %dest.display(),
          file_count,
          "checkpoint.create.complete"
        );
        Ok(dest.to_path_buf())
    }

    /// smoke: backup 目录可被 `DB::open`.
    pub fn verify_openable(
        backup_dir: impl AsRef<Path>,
        options: crate::config::Options,
    ) -> Result<()> {
        let _db = DB::open(backup_dir, options)?;
        Ok(())
    }
}

fn checkpoint_tmp_path(dest: &Path) -> PathBuf {
    let name = dest
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("backup");
    dest.parent()
        .map(|p| p.join(format!("{name}.tmp")))
        .unwrap_or_else(|| PathBuf::from(format!("{name}.tmp")))
}

fn remove_dir_if_exists(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    Ok(())
}

fn link_or_copy(src: &Path, dst: &Path) -> Result<()> {
    match fs::hard_link(src, dst) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(_) => {
            fs::copy(src, dst)?;
            Ok(())
        }
    }
}

fn sync_dir(path: &Path) -> Result<()> {
    let dir = File::open(path)?;
    dir.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Options;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use tempfile::tempdir;

    fn open_db(dir: &Path) -> Arc<DB> {
        DB::open(dir, Options::for_testing()).unwrap()
    }

    #[test]
    fn test_checkpoint_create_empty_db() {
        let dir = tempdir().unwrap();
        let db = open_db(dir.path());
        let backup = dir.path().join("backup");
        let path = Checkpoint::create(&db, &backup).unwrap();
        assert_eq!(path, backup);
        Checkpoint::verify_openable(&backup, Options::for_testing()).unwrap();
    }

    #[test]
    fn test_checkpoint_after_puts() {
        let dir = tempdir().unwrap();
        let db = open_db(dir.path());
        db.put(b"k1", b"v1").unwrap();
        db.put(b"k2", b"v2").unwrap();
        db.flush().unwrap();

        let backup = dir.path().join("backup");
        Checkpoint::create(&db, &backup).unwrap();

        let restored = DB::open(&backup, Options::for_testing()).unwrap();
        assert_eq!(restored.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(restored.get(b"k2").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_checkpoint_after_delete() {
        let dir = tempdir().unwrap();
        let db = open_db(dir.path());
        db.put(b"gone", b"x").unwrap();
        db.flush().unwrap();
        db.delete(b"gone").unwrap();
        db.flush().unwrap();

        let backup = dir.path().join("backup");
        Checkpoint::create(&db, &backup).unwrap();

        let restored = DB::open(&backup, Options::for_testing()).unwrap();
        assert_eq!(restored.get(b"gone").unwrap(), None);
    }

    #[test]
    fn test_checkpoint_overwrites_existing() {
        let dir = tempdir().unwrap();
        let db = open_db(dir.path());
        db.put(b"k1", b"v1").unwrap();
        db.flush().unwrap();

        let backup = dir.path().join("backup");
        Checkpoint::create(&db, &backup).unwrap();
        fs::write(backup.join("marker"), b"stale").unwrap();

        db.put(b"k2", b"v2").unwrap();
        db.flush().unwrap();
        Checkpoint::create(&db, &backup).unwrap();

        let restored = DB::open(&backup, Options::for_testing()).unwrap();
        assert_eq!(restored.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(restored.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        assert!(!backup.join("marker").exists());
    }

    #[test]
    fn test_checkpoint_during_compaction() {
        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        opts.background_compaction = true;
        opts.level0_compaction_trigger = 2;
        opts.memtable_size = 4096;
        let db = DB::open(dir.path(), opts).unwrap();

        for i in 0..200u32 {
            db.put(format!("key_{i:04}").as_bytes(), b"value").unwrap();
        }
        db.flush().unwrap();

        let backup = dir.path().join("backup");
        let db_clone = Arc::clone(&db);
        let backup_for_thread = backup.clone();
        let checkpoint_handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            Checkpoint::create(&db_clone, &backup_for_thread).unwrap();
        });

        for i in 200..400u32 {
            db.put(format!("key_{i:04}").as_bytes(), b"value").unwrap();
        }
        let _ = db.drain_compactions();

        checkpoint_handle.join().unwrap();
        Checkpoint::verify_openable(&backup, Options::for_testing()).unwrap();
    }

    #[test]
    fn test_checkpoint_fallback_unwritable_parent() {
        // 模拟 backup 目录是挂载点根: 父目录不可写, tmp 必须落在 dest 内部.
        // DB 与 backup 分属不同目录, 避免父目录只读影响 DB 自身写入.
        let db_dir = tempdir().unwrap();
        let backup_dir = tempdir().unwrap();
        let db = open_db(db_dir.path());
        db.put(b"k1", b"v1").unwrap();
        db.flush().unwrap();

        let backup = backup_dir.path().join("backup");
        let parent = backup_dir.path();
        // 模拟真实场景: backup 目录 (挂载点根) 已由部署脚本预创建.
        fs::create_dir_all(&backup).unwrap();
        // 父目录去掉写权限, 使 checkpoint_tmp_path (父目录下 backup.tmp) 创建失败.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(parent, fs::Permissions::from_mode(0o555)).unwrap();
        }

        let result = Checkpoint::create(&db, &backup);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(parent, fs::Permissions::from_mode(0o755)).unwrap();
        }
        // 作为 root 运行时父目录只读不生效, 走 rename 路径; 非 root 则走 fallback.
        // 无论哪条路径, 结果都必须是可 open 的 checkpoint.
        let path = result.expect("checkpoint create must succeed via fallback");
        assert_eq!(path, backup);
        Checkpoint::verify_openable(&backup, Options::for_testing()).unwrap();
    }
}
