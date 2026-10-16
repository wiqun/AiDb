/// AiDb 备份/恢复示例.
///
/// 运行: cargo run --example backup
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = Path::new("/tmp/aidb-backup-example");
    let _ = std::fs::remove_dir_all(dir);

    // 创建数据库并写入数据
    let db = aidb::DB::open(dir, aidb::config::Options::default())?;
    for i in 0u32..100 {
        db.put(format!("key_{i:04}").as_bytes(), b"value")?;
    }
    db.flush()?;
    println!("✅ 写入 100 个 key 并 flush");

    // 创建备份
    let storage = Arc::new(aidb::backup::LocalFileStorage::new(dir.join("backups")));
    let manager =
        aidb::backup::BackupManager::new(storage.clone(), aidb::backup::RetentionPolicy::default());
    let id = manager.create_backup(&db)?;
    println!("✅ 创建备份, id = {id}");

    // 列举备份
    let backups = manager.list_backups()?;
    println!("✅ 备份列表: {} 个", backups.len());
    for b in &backups {
        println!(
            "   id={}, created_at={:?}, files={}",
            b.id, b.created_at, b.file_count
        );
    }

    // 验证备份完整性
    let recovery = aidb::backup::RecoveryManager::new(storage.clone());
    assert!(recovery.verify_backup(id)?);
    println!("✅ 备份完整性验证通过");

    // 恢复到新目录
    let restore_dir = dir.join("restored");
    recovery.restore(id, &restore_dir)?;
    println!("✅ 恢复完成");

    // 验证恢复数据
    let restored = aidb::DB::open(&restore_dir, aidb::config::Options::default())?;
    let val = restored.get(b"key_0000")?;
    assert_eq!(val, Some(b"value".to_vec()));
    println!("✅ 恢复数据验证通过");

    Ok(())
}

use std::path::Path;
