//! RecoveryManager: 从备份恢复数据目录.

use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::backup::manager::{BackupId, BackupManifest};
use crate::backup::storage::BackupStorage;
use crate::backup::util;
use crate::config::Options;
use crate::error::{Error, Result};
use crate::statistics::{BackupOp, Statistics};
use crate::DB;

/// 恢复管理器.
pub struct RecoveryManager {
    storage: Arc<dyn BackupStorage>,
    stats: Option<Arc<Statistics>>,
}

impl RecoveryManager {
    pub fn new(storage: Arc<dyn BackupStorage>) -> Self {
        Self::new_with_stats(storage, None)
    }

    pub fn new_with_stats(storage: Arc<dyn BackupStorage>, stats: Option<Arc<Statistics>>) -> Self {
        Self { storage, stats }
    }

    /// 从备份恢复数据目录.
    ///
    /// 1. 读取 manifest 校验 checksum
    /// 2. 在 db_path 同级创建临时恢复目录
    /// 3. 逐文件恢复到临时目录, 验证 SHA256
    /// 4. 用 DB::open 验证 SSTable 完整性
    /// 5. 原子 rename 临时目录 → 目标目录
    pub fn restore(&self, id: BackupId, db_path: &Path) -> Result<()> {
        if db_path.exists() {
            let has_files = db_path
                .read_dir()
                .ok()
                .is_some_and(|mut it| it.next().is_some());
            if has_files {
                return Err(Error::InvalidArgument(
                    "restore target directory is not empty".into(),
                ));
            }
        }

        // 1. 读取 manifest
        let manifest_bytes = {
            let p = self.storage.backup_path(id).join("backup_manifest.json");
            if !p.exists() {
                return Err(Error::NotFound);
            }
            self.storage.read_to_string(&p)?
        };
        let manifest: BackupManifest = serde_json::from_str(&manifest_bytes)
            .map_err(|e| Error::Corruption(format!("invalid manifest: {e}")))?;

        // 验证 manifest checksum
        let stored_checksum = manifest.metadata.checksum.clone();
        let mut manifest_without_checksum = manifest.clone();
        manifest_without_checksum.metadata.checksum.clear();
        let computed = util::sha256_bytes(
            &serde_json::to_vec(&manifest_without_checksum)
                .map_err(|e| Error::Corruption(format!("manifest serialize: {e}")))?,
        );
        if computed != stored_checksum {
            return Err(Error::Corruption(
                "backup manifest checksum mismatch".into(),
            ));
        }

        // 2. 创建临时恢复目录
        let tmp_dir = db_path
            .parent()
            .unwrap_or(Path::new("."))
            .join(format!("restore_tmp_{id}"));
        if tmp_dir.exists() {
            std::fs::remove_dir_all(&tmp_dir)?;
        }
        std::fs::create_dir_all(&tmp_dir)?;

        // 3. 逐文件恢复
        for entry in &manifest.files {
            let src = self.storage.backup_path(id).join(&entry.relative_path);
            let dst = tmp_dir.join(&entry.relative_path);
            if let Some(parent) = dst.parent() {
                std::fs::create_dir_all(parent)?;
            }
            self.storage.load(&src, &dst)?;

            // 验证 SHA256
            let actual = util::sha256_file(&dst)?;
            if actual != entry.checksum {
                let _ = std::fs::remove_dir_all(&tmp_dir);
                return Err(Error::Corruption(format!(
                    "file checksum mismatch: {}",
                    entry.relative_path
                )));
            }
        }

        // 4. 用 DB::open 验证完整性
        let _db = DB::open(&tmp_dir, Options::for_testing()).inspect_err(|_e| {
            let _ = std::fs::remove_dir_all(&tmp_dir);
        })?;
        drop(_db);

        // 5. 原子 rename
        if db_path.exists() {
            std::fs::remove_dir_all(db_path)?;
        }
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        match std::fs::rename(&tmp_dir, db_path) {
            Ok(()) => {
                if let Some(parent) = db_path.parent() {
                    if let Ok(dir) = std::fs::File::open(parent) {
                        let _ = dir.sync_all();
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::CrossesDevices => {
                copy_dir_all(&tmp_dir, db_path)?;
                if let Ok(dir) = std::fs::File::open(db_path) {
                    let _ = dir.sync_all();
                }
                let _ = std::fs::remove_dir_all(&tmp_dir);
            }
            Err(e) => {
                let _ = std::fs::remove_dir_all(&tmp_dir);
                return Err(Error::Io(e));
            }
        }

        if let Some(ref stats) = self.stats {
            stats.backup_total[BackupOp::Restore as usize].fetch_add(1, Ordering::Relaxed);
        }
        tracing::info!(backup_id = id, "backup.restore.complete");

        Ok(())
    }

    /// 校验备份完整性 (不恢复).
    pub fn verify_backup(&self, id: BackupId) -> Result<bool> {
        let manifest_path = self.storage.backup_path(id).join("backup_manifest.json");
        if !manifest_path.exists() {
            return Ok(false);
        }
        let data = match self.storage.read_to_string(&manifest_path) {
            Ok(d) => d,
            Err(_) => return Ok(false),
        };
        let manifest: BackupManifest = match serde_json::from_str(&data) {
            Ok(m) => m,
            Err(_) => return Ok(false),
        };

        // 验证 manifest checksum
        let stored = manifest.metadata.checksum.clone();
        let mut without = manifest.clone();
        without.metadata.checksum.clear();
        let computed = match serde_json::to_vec(&without) {
            Ok(v) => util::sha256_bytes(&v),
            Err(_) => return Ok(false),
        };
        if computed != stored {
            return Ok(false);
        }

        // 验证每个文件的 SHA256
        for entry in &manifest.files {
            let path = self.storage.backup_path(id).join(&entry.relative_path);
            if !path.exists() {
                return Ok(false);
            }
            let actual = match util::sha256_file(&path) {
                Ok(h) => h,
                Err(_) => return Ok(false),
            };
            if actual != entry.checksum {
                return Ok(false);
            }
        }

        tracing::info!(backup_id = id, is_valid = true, "backup.verify.result");
        Ok(true)
    }
}

fn copy_dir_all(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_all(&entry.path(), &dst.join(entry.file_name()))?;
        } else {
            std::fs::copy(entry.path(), dst.join(entry.file_name()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::manager::BackupManager;
    use crate::backup::storage::LocalFileStorage;
    use crate::backup::RetentionPolicy;
    use crate::config::Options;
    use crate::DB;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test_verify_backup_integrity() {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        db.put(b"k1", b"v1").unwrap();
        db.flush().unwrap();

        let backup_root = dir.path().join("backups");
        let storage = Arc::new(LocalFileStorage::new(backup_root.clone()));
        let manager = BackupManager::new(storage.clone(), RetentionPolicy::default());
        let id = manager.create_backup(&db).unwrap();
        drop(db);

        let recovery = RecoveryManager::new(storage);
        assert!(recovery.verify_backup(id).unwrap());
    }

    #[test]
    fn test_verify_backup_corrupted() {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        db.put(b"k1", b"v1").unwrap();
        db.flush().unwrap();

        let backup_root = dir.path().join("backups");
        let storage = Arc::new(LocalFileStorage::new(backup_root.clone()));
        let manager = BackupManager::new(storage.clone(), RetentionPolicy::default());
        let id = manager.create_backup(&db).unwrap();
        drop(db);

        // 篡改备份文件
        if let Ok(entries) = std::fs::read_dir(storage.backup_path(id)) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().map(|e| e == "sst").unwrap_or(false) {
                    std::fs::write(&path, b"corrupted data").unwrap();
                    break;
                }
            }
        }

        let recovery = RecoveryManager::new(storage);
        assert!(!recovery.verify_backup(id).unwrap());
    }
}
