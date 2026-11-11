//! Per-group DB storage management.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::cluster::log_committer::DbSlot;
use crate::config::Options;
use crate::engine::compaction::{CompactionFilter, CompactionRemovalListener};
use crate::error::Result;
use crate::DB;

/// 单 Group 存储统计 (预留, 当前 DB 引擎尚未暴露全部指标)
#[derive(Debug, Clone)]
pub struct StorageStats {
    pub group_id: u64,
    pub key_count: u64,
    pub estimated_size: u64,
    pub memtable_size: u64,
    pub wal_size: u64,
}

/// 聚合存储统计 (预留)
#[derive(Debug, Clone, Default)]
pub struct AggregateStats {
    pub total_key_count: u64,
    pub total_estimated_size: u64,
    pub total_memtable_size: u64,
    pub total_wal_size: u64,
    pub group_count: usize,
}

/// 分片存储: 每个 Group 独立的 DB 引擎实例
#[derive(Clone)]
pub struct ShardedStorage {
    db: DbSlot,
    group_id: u64,
    path: PathBuf,
}

impl ShardedStorage {
    /// 创建新的分片存储
    pub fn open(data_dir: &Path, group_id: u64, options: Options) -> Result<Self> {
        let group_path = data_dir.join(format!("group_{}", group_id));
        std::fs::create_dir_all(&group_path)?;
        let db = Arc::new(RwLock::new(DB::open(&group_path, options)?));
        Ok(Self {
            db,
            group_id,
            path: group_path,
        })
    }

    /// 关闭存储 (Arc<DB> drop 自动关闭引擎).
    /// 显式 close 确保 compaction/flush 线程安全终止且所有数据刷盘.
    pub fn close(self) -> Result<()> {
        self.db.read().close()?;
        Ok(())
    }

    pub fn db(&self) -> parking_lot::RwLockReadGuard<'_, Arc<DB>> {
        self.db.read()
    }

    pub fn db_slot(&self) -> DbSlot {
        Arc::clone(&self.db)
    }

    pub fn group_id(&self) -> u64 {
        self.group_id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 设置 compaction 过滤器, 在下次 compaction 时生效.
    pub fn set_compaction_filter(&self, filter: Option<Arc<dyn CompactionFilter>>) {
        self.db.read().set_compaction_filter(filter);
    }

    /// 设置 filter Remove (最新 Put) 监听器.
    pub fn set_compaction_removal_listener(
        &self,
        listener: Option<Arc<dyn CompactionRemovalListener>>,
    ) {
        self.db.read().set_compaction_removal_listener(listener);
    }
}
