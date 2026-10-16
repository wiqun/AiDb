use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::cluster::meta_raft_node::MetaRaftNode;
use crate::cluster::meta_types::MetaRequest;
use crate::cluster::multi_raft_node::MultiRaftNode;
use crate::cluster::types::{ClusterError, Request};
use crate::config::MigrationConfig;
use crate::error::Result;

use super::{ActiveMigration, BatchMigrateResult};

// ---------------------------------------------------------------------------
// SlotMigrationExecutor
// ---------------------------------------------------------------------------

pub struct SlotMigrationExecutor {
    meta_raft: Arc<MetaRaftNode>,
    multi_raft: Arc<MultiRaftNode>,
    checkpoint_dir: PathBuf,
    cancellation: Arc<AtomicBool>,
    config: MigrationConfig,
}

impl SlotMigrationExecutor {
    pub fn new(
        meta_raft: Arc<MetaRaftNode>,
        multi_raft: Arc<MultiRaftNode>,
        checkpoint_dir: PathBuf,
        config: MigrationConfig,
    ) -> Self {
        std::fs::create_dir_all(&checkpoint_dir).ok();
        Self {
            meta_raft,
            multi_raft,
            checkpoint_dir,
            cancellation: Arc::new(AtomicBool::new(false)),
            config,
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.load(Ordering::Relaxed)
    }

    pub fn request_cancellation(&self) {
        self.cancellation.store(true, Ordering::Relaxed);
    }

    pub async fn execute(&self, migration: ActiveMigration) -> Result<BatchMigrateResult> {
        let last_key = self.load_checkpoint(migration.migration_id);

        // Scan source group keys
        let all_keys = self
            .multi_raft
            .scan_keys(migration.source_group, None)
            .await?;
        // Filter by slot range
        let slot_set: std::collections::HashSet<u16> = migration.slots.iter().copied().collect();
        let mut filtered: Vec<Vec<u8>> = all_keys
            .into_iter()
            .filter(|k| slot_set.contains(&crate::cluster::router::key_to_slot(k)))
            .collect();
        filtered.sort();

        // Apply checkpoint resume
        let start_idx = last_key.as_ref().map_or(0, |lk| {
            filtered
                .iter()
                .position(|k| k >= lk)
                .unwrap_or(filtered.len())
        });

        let total_keys = filtered.len() as u64;
        let mut migrated_count = 0u64;
        let mut last_migrated_key: Option<Vec<u8>> = None;

        for chunk in filtered[start_idx..].chunks(self.config.max_batch_size) {
            if self.is_cancelled() {
                self.save_checkpoint(migration.migration_id, &last_migrated_key)?;
                return Ok(BatchMigrateResult {
                    migrated_count,
                    failed_count: 0,
                    last_migrated_key: last_migrated_key.clone(),
                    is_completed: false,
                });
            }

            for key in chunk {
                let value = self
                    .multi_raft
                    .get_key_from_group(migration.source_group, key)
                    .await?;
                if let Some(v) = value {
                    self.multi_raft
                        .propose_group(
                            migration.target_group,
                            Request::PutConditional {
                                key: key.clone(),
                                value: v,
                                // FIX-0056-A1: 让 apply 内查 Del tombstone,
                                // 不复活迁移期已被客户端 DEL 掉的 key.
                                migration_epoch: Some(migration.migration_id),
                            },
                        )
                        .await?;
                }
                migrated_count += 1;
                last_migrated_key = Some(key.clone());

                if migrated_count.is_multiple_of(self.config.progress_report_interval) {
                    let _ = self
                        .meta_raft
                        .propose(MetaRequest::UpdateMigrationProgress {
                            progress: migrated_count,
                            total: total_keys,
                        })
                        .await;
                }
                self.save_checkpoint(migration.migration_id, &last_migrated_key)?;
            }
        }

        // Verify (deterministic step sampling — no rand dependency needed)
        self.verify_migration(
            migration.source_group,
            migration.target_group,
            &migration.slots,
        )
        .await?;

        // Cleanup checkpoint
        self.delete_checkpoint(migration.migration_id)?;

        tracing::info!(
            migrated_count,
            is_completed = true,
            "migration execution completed"
        );

        Ok(BatchMigrateResult {
            migrated_count,
            failed_count: 0,
            last_migrated_key,
            is_completed: true,
        })
    }

    async fn verify_migration(
        &self,
        source_group: u64,
        target_group: u64,
        slots: &[u16],
    ) -> Result<()> {
        // Deterministic step sampling: pick every N-th key in sorted order
        let source_keys = self.multi_raft.scan_keys(source_group, None).await?;
        let slot_set: std::collections::HashSet<u16> = slots.iter().copied().collect();
        let mut slot_keys: Vec<&Vec<u8>> = source_keys
            .iter()
            .filter(|k| slot_set.contains(&crate::cluster::router::key_to_slot(k)))
            .collect();
        slot_keys.sort();

        if slot_keys.is_empty() {
            return Ok(());
        }

        let sample_count =
            ((slot_keys.len() as f64).sqrt() * self.config.verify_sample_factor) as usize;
        let sample_count = sample_count.max(1).min(slot_keys.len());
        let step = slot_keys.len() / sample_count;

        for i in 0..sample_count {
            let key = slot_keys[i * step];
            let src_val = self
                .multi_raft
                .get_key_from_group(source_group, key)
                .await?;
            let tgt_val = self
                .multi_raft
                .get_key_from_group(target_group, key)
                .await?;

            match (src_val, tgt_val) {
                (Some(sv), Some(tv)) if sv != tv => {
                    return Err(ClusterError::Internal(format!(
                        "migration verification failed for key {:?}",
                        key
                    ))
                    .into());
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn save_checkpoint(&self, migration_id: u64, last_key: &Option<Vec<u8>>) -> Result<()> {
        if let Some(key) = last_key {
            let tmp = self
                .checkpoint_dir
                .join(format!("migration_{}.tmp", migration_id));
            let final_path = self
                .checkpoint_dir
                .join(format!("migration_{}.ckpt", migration_id));
            std::fs::write(&tmp, key).map_err(ClusterError::Io)?;
            std::fs::rename(&tmp, &final_path).map_err(ClusterError::Io)?;
        }
        Ok(())
    }

    fn load_checkpoint(&self, migration_id: u64) -> Option<Vec<u8>> {
        let path = self
            .checkpoint_dir
            .join(format!("migration_{}.ckpt", migration_id));
        std::fs::read(&path).ok()
    }

    fn delete_checkpoint(&self, migration_id: u64) -> Result<()> {
        let path = self
            .checkpoint_dir
            .join(format!("migration_{}.ckpt", migration_id));
        let _ = std::fs::remove_file(&path);
        Ok(())
    }
}
