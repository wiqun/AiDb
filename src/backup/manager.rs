//! BackupManager: 备份创建、列举、删除、保留策略.

use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::backup::storage::BackupStorage;
use crate::backup::util;
use crate::engine::checkpoint::Checkpoint;
use crate::error::{Error, Result};
use crate::statistics::{BackupOp, Statistics};
use crate::DB;

/// 备份 ID, 基于时间戳生成.
pub type BackupId = u64;

/// 备份元信息.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupMetadata {
    pub id: BackupId,
    pub created_at: SystemTime,
    pub description: Option<String>,
    pub data_size: u64,
    pub backup_size: u64,
    pub file_count: u32,
    pub checksum: String,
    pub db_sequence: u64,
    pub version: String,
}

/// 备份清单.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    pub metadata: BackupMetadata,
    pub files: Vec<BackupFileEntry>,
}

/// 备份文件条目.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupFileEntry {
    pub relative_path: String,
    pub size: u64,
    pub checksum: String,
}

/// 保留策略.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    pub min_count: usize,
    pub max_count: usize,
    pub min_age: Duration,
    pub max_age: Duration,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            min_count: 3,
            max_count: 30,
            min_age: Duration::from_secs(86400u64),
            max_age: Duration::from_secs(86400u64 * 30),
        }
    }
}

impl RetentionPolicy {
    /// 返回应被删除的备份 ID 列表.
    ///
    /// 规则 (按优先级):
    /// 1. min_age 以内的备份不删
    /// 2. 至少保留 min_count 个
    /// 3. 不超过 max_count 个 (max_count >= min_count 时有意义)
    /// 4. max_age 硬过期: 超过 max_age 的备份无条件删除
    pub fn select_for_deletion(&self, backups: &[BackupMetadata]) -> Vec<BackupId> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let mut sorted: Vec<&BackupMetadata> = backups.iter().collect();
        // 按 created_at 升序排列 = 最旧在前
        sorted.sort_by_key(|b| b.created_at);

        // partition_point: 找到第一个 age < min_age 的位置 (即第一个"年轻"备份)
        let cutoff = sorted.partition_point(|b| {
            let age = now
                .checked_sub(b.created_at.duration_since(UNIX_EPOCH).unwrap_or_default())
                .unwrap_or_default();
            age >= self.min_age
        });
        let (old, young) = sorted.split_at(cutoff);

        // 2. 从 old 组中删除最旧的, 直到剩余不超过 max_count
        //    但必须保留至少 min_count 个
        let young_count = young.len();

        // 最多可删除 old 组中的多少个
        let minimum_keep = self.min_count.saturating_sub(young_count);
        let max_keep = self.max_count.saturating_sub(young_count);

        // 从 old 中选择保留的数量
        let keep = old.len().min(max_keep).max(minimum_keep.min(old.len()));
        let delete_count = old.len().saturating_sub(keep);

        let mut to_delete: Vec<BackupId> = old[..delete_count].iter().map(|b| b.id).collect();

        // 3. max_age 硬过期: 超过 max_age 的备份无条件删除 (不受 min_count 保护)
        for b in &sorted {
            let age = now
                .checked_sub(b.created_at.duration_since(UNIX_EPOCH).unwrap_or_default())
                .unwrap_or_default();
            if age >= self.max_age && !to_delete.contains(&b.id) {
                to_delete.push(b.id);
            }
        }

        to_delete
    }
}

/// 备份管理器.
pub struct BackupManager {
    storage: Arc<dyn BackupStorage>,
    policy: RetentionPolicy,
    stats: Option<Arc<Statistics>>,
}

impl BackupManager {
    pub fn new(storage: Arc<dyn BackupStorage>, policy: RetentionPolicy) -> Self {
        Self::new_with_stats(storage, policy, None)
    }

    pub fn new_with_stats(
        storage: Arc<dyn BackupStorage>,
        policy: RetentionPolicy,
        stats: Option<Arc<Statistics>>,
    ) -> Self {
        Self {
            storage,
            policy,
            stats,
        }
    }

    pub fn create_backup(&self, db: &DB) -> Result<BackupId> {
        self.create_backup_with_description(db, None)
    }

    pub fn create_backup_with_description(
        &self,
        db: &DB,
        description: Option<&str>,
    ) -> Result<BackupId> {
        let backup_id = timestamp_nanos();
        self.create_backup_inner(db, backup_id, description)
            .map_err(|e| {
                note_backup_failed(backup_id, &e.to_string());
                e
            })
    }

    fn create_backup_inner(
        &self,
        db: &DB,
        backup_id: BackupId,
        description: Option<&str>,
    ) -> Result<BackupId> {
        use std::fs;

        let start = Instant::now();

        // 在临时位置创建 checkpoint
        let checkpoint_dir = db.path().join(format!(".backup_tmp_{backup_id}"));
        let _ = fs::remove_dir_all(&checkpoint_dir);
        let cp_path = Checkpoint::create(db, &checkpoint_dir)?;

        // 收集 checkpoint 中的文件
        let mut file_entries = Vec::new();
        let mut total_data_size = 0u64;

        fn collect_files(dir: &Path) -> Result<Vec<std::path::PathBuf>> {
            let mut files = Vec::new();
            if dir.is_dir() {
                for entry in fs::read_dir(dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_dir() {
                        files.extend(collect_files(&path)?);
                    } else {
                        files.push(path);
                    }
                }
            }
            Ok(files)
        }

        let cp_files = collect_files(&cp_path)?;

        // 逐文件复制到备份存储
        for src_path in &cp_files {
            let rel = src_path
                .strip_prefix(&cp_path)
                .map_err(|e| Error::InvalidArgument(e.to_string()))?;
            let dest = self.storage.backup_path(backup_id).join(rel);
            let file_sha256 = self.storage.store(src_path, &dest)?;
            let size = fs::metadata(src_path)?.len();
            file_entries.push(BackupFileEntry {
                relative_path: rel.to_string_lossy().to_string(),
                size,
                checksum: file_sha256,
            });
            total_data_size += size;
        }

        // 清理临时 checkpoint 目录
        let _ = fs::remove_dir_all(&cp_path);

        // 构造 manifest
        let metadata = BackupMetadata {
            id: backup_id,
            created_at: SystemTime::now(),
            description: description.map(|s| s.to_string()),
            data_size: total_data_size,
            backup_size: total_data_size,
            file_count: file_entries.len() as u32,
            checksum: String::new(),
            db_sequence: db.current_sequence(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        };

        let mut manifest = BackupManifest {
            metadata,
            files: file_entries,
        };

        // 序列化并计算 checksum
        let manifest_bytes =
            serde_json::to_vec(&manifest).map_err(|e| Error::Corruption(e.to_string()))?;
        let manifest_checksum = util::sha256_bytes(&manifest_bytes);
        manifest.metadata.checksum = manifest_checksum.clone();
        let final_bytes =
            serde_json::to_vec(&manifest).map_err(|e| Error::Corruption(e.to_string()))?;

        // 写入 manifest
        self.storage.store_bytes(
            &self
                .storage
                .backup_path(backup_id)
                .join("backup_manifest.json"),
            &final_bytes,
        )?;

        // 应用保留策略
        self.apply_retention_policy()?;

        let stats = self.stats.clone().unwrap_or_else(|| db.statistics());
        stats.backup_total[BackupOp::Create as usize].fetch_add(1, Ordering::Relaxed);
        stats
            .backup_size_bytes
            .store(total_data_size, Ordering::Relaxed);
        stats
            .backup_duration
            .record(start.elapsed().as_micros() as u64);

        tracing::event!(
            tracing::Level::INFO,
            backup_id,
            file_count = manifest.metadata.file_count,
            total_size = manifest.metadata.data_size,
            "backup.create.complete"
        );

        Ok(backup_id)
    }

    pub fn list_backups(&self) -> Result<Vec<BackupMetadata>> {
        let entries = self.storage.list("backup_")?;
        let mut backups = Vec::new();
        for name in entries {
            let backup_id: u64 = name
                .strip_prefix("backup_")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let manifest_path = self
                .storage
                .backup_path(backup_id)
                .join("backup_manifest.json");
            if manifest_path.exists() {
                let data = self.storage.read_to_string(&manifest_path)?;
                match serde_json::from_str::<BackupManifest>(&data) {
                    Ok(manifest) => backups.push(manifest.metadata),
                    Err(e) => tracing::warn!(backup_id, error = %e, "corrupt backup manifest"),
                }
            }
        }
        backups.sort_by_key(|b| std::cmp::Reverse(b.created_at));
        Ok(backups)
    }

    pub fn get_backup_info(&self, id: BackupId) -> Result<BackupMetadata> {
        let manifest_path = self.storage.backup_path(id).join("backup_manifest.json");
        let data = self.storage.read_to_string(&manifest_path)?;
        let manifest: BackupManifest =
            serde_json::from_str(&data).map_err(|e| Error::Corruption(e.to_string()))?;
        Ok(manifest.metadata)
    }

    pub fn delete_backup(&self, id: BackupId) -> Result<()> {
        let path = self.storage.backup_path(id);
        let result = self.storage.delete(&path);
        if let Some(ref s) = self.stats {
            s.backup_total[BackupOp::Delete as usize].fetch_add(1, Ordering::Relaxed);
        }
        tracing::info!(backup_id = id, "backup.delete");
        result
    }

    pub fn apply_retention_policy(&self) -> Result<Vec<BackupId>> {
        let backups = self.list_backups()?;
        let to_delete = self.policy.select_for_deletion(&backups);
        for id in &to_delete {
            self.delete_backup(*id)?;
        }
        Ok(to_delete)
    }
}

fn timestamp_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn note_backup_failed(backup_id: u64, error: &str) {
    tracing::error!(backup_id, error, "backup failed");
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::Subscriber;

    fn make_backup(id: u64, age_secs: u64) -> BackupMetadata {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        BackupMetadata {
            id,
            created_at: UNIX_EPOCH + Duration::from_secs(now.saturating_sub(age_secs)),
            description: None,
            data_size: 100,
            backup_size: 100,
            file_count: 1,
            checksum: "abc".into(),
            db_sequence: id,
            version: "0.11.1".into(),
        }
    }

    #[test]
    fn test_retention_policy_under_max() {
        let policy = RetentionPolicy {
            min_count: 3,
            max_count: 10,
            min_age: Duration::from_secs(0),
            max_age: Duration::from_secs(u64::MAX),
        };
        let backups: Vec<BackupMetadata> = (0..5).map(|i| make_backup(i, 1000 + i * 10)).collect();
        let to_delete = policy.select_for_deletion(&backups);
        assert_eq!(to_delete.len(), 0, "5 <= max=10, keep all");
    }

    #[test]
    fn test_retention_policy_over_max() {
        let policy = RetentionPolicy {
            min_count: 1,
            max_count: 3,
            min_age: Duration::from_secs(0),
            max_age: Duration::from_secs(u64::MAX),
        };
        // 10 backups, ages increase with id: id=oldest(age 2009) ... id=0(newest, age 2000)
        let backups: Vec<BackupMetadata> = (0..10).map(|i| make_backup(i, 2000 + i)).collect();
        let to_delete = policy.select_for_deletion(&backups);
        assert_eq!(to_delete.len(), 7, "10 > max=3, delete 7");
        // 最旧的 7 个应当是 id 9..3
        for i in 3..10 {
            assert!(
                to_delete.contains(&(i as u64)),
                "backup {i} should be deleted (oldest)"
            );
        }
    }

    #[test]
    fn test_retention_policy_min_count_respected() {
        let policy = RetentionPolicy {
            min_count: 3,
            max_count: 4,
            min_age: Duration::from_secs(0),
            max_age: Duration::from_secs(u64::MAX),
        };
        // 5 backups, id 4 is oldest (age 1040), id 0 is newest (age 1000)
        let backups: Vec<BackupMetadata> = (0..5).map(|i| make_backup(i, 1000 + i * 10)).collect();
        let to_delete = policy.select_for_deletion(&backups);
        assert_eq!(to_delete.len(), 1, "5>max=4, min=3: delete 1, keep 4");
        assert_eq!(to_delete[0], 4, "oldest backup (id=4) should be deleted");
    }

    #[test]
    fn test_retention_policy_max_age_expiry() {
        let policy = RetentionPolicy {
            min_count: 10,
            max_count: 20,
            min_age: Duration::from_secs(0),
            max_age: Duration::from_secs(500),
        };
        // 4 backups: ids 0,1 old (>500s), 2,3 young (<500s)
        let backups = vec![
            make_backup(0, 1000),
            make_backup(1, 600),
            make_backup(2, 400),
            make_backup(3, 200),
        ];
        let to_delete = policy.select_for_deletion(&backups);
        // min_count=10 > total=4, normally nothing deleted,
        // but max_age overrides: 0 and 1 are older than 500s
        assert!(to_delete.contains(&0), "backup 0 exceeds max_age");
        assert!(to_delete.contains(&1), "backup 1 exceeds max_age");
        assert_eq!(to_delete.len(), 2, "only old backups deleted by max_age");
    }

    #[test]
    fn test_retention_policy_min_age_protection() {
        let policy = RetentionPolicy {
            min_count: 1,
            max_count: 3,
            min_age: Duration::from_secs(1000),
            max_age: Duration::from_secs(u64::MAX),
        };
        // 5 backups: ids 0,1 are old (>1000s), 2,3,4 are young (<1000s)
        let backups = vec![
            make_backup(0, 2000),
            make_backup(1, 1500),
            make_backup(2, 500),
            make_backup(3, 400),
            make_backup(4, 300),
        ];
        let to_delete = policy.select_for_deletion(&backups);
        // 5 > max=3, min=1: delete 2 oldest (must keep 3 total, but 3 are in min_age)
        assert_eq!(to_delete.len(), 2);
        assert_eq!(to_delete[0], 0);
        assert_eq!(to_delete[1], 1);
    }

    #[test]
    fn backup_failure_logs_error() {
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::prelude::*;
        use tracing_subscriber::Registry;

        let hits = Arc::new(Mutex::new(Vec::new()));
        let stored = Arc::clone(&hits);
        tracing::subscriber::with_default(
            Registry::default().with(ErrLayer { hits: stored }),
            || {
                super::note_backup_failed(7, "disk full");
            },
        );
        let hits = hits.lock().unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, tracing::Level::ERROR);
        assert_eq!(hits[0].1, "backup failed");
        assert!(hits[0].2.contains("backup_id=7"));
        assert!(!hits[0].2.contains("user-key"));
    }

    struct ErrLayer {
        hits: std::sync::Arc<std::sync::Mutex<Vec<(tracing::Level, String, String)>>>,
    }

    impl<S: Subscriber> tracing_subscriber::Layer<S> for ErrLayer {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut message = String::new();
            let mut fields = String::new();
            event.record(&mut Grab {
                message: &mut message,
                fields: &mut fields,
            });
            self.hits
                .lock()
                .unwrap()
                .push((*event.metadata().level(), message, fields));
        }
    }

    struct Grab<'a> {
        message: &'a mut String,
        fields: &'a mut String,
    }

    impl tracing::field::Visit for Grab<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                *self.message = format!("{value:?}").trim_matches('"').to_string();
            } else {
                self.fields
                    .push_str(&format!("{}={value:?} ", field.name()));
            }
        }

        fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
            self.fields.push_str(&format!("{}={value} ", field.name()));
        }

        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            self.fields.push_str(&format!("{}={value} ", field.name()));
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "message" {
                *self.message = value.to_string();
            } else {
                self.fields.push_str(&format!("{}={value} ", field.name()));
            }
        }
    }
}
