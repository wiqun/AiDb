/// 外部数据库嵌入 AiDb 时从配置文件反序列化配置示例.
///
/// 运行: cargo run --example config_from_json --all-features
use std::path::Path;

use aidb::cluster::log_committer::LogCommitterConfig;
use aidb::config::{ClusterConfig, Options};
use aidb::DB;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct EngineConfigFile {
    #[serde(default)]
    options: Options,
    #[serde(default)]
    cluster: ClusterConfig,
    #[serde(default)]
    log_committer: LogCommitterConfig,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let raw = include_str!("configs/aidb.example.json");
    let cfg: EngineConfigFile = serde_json::from_str(raw)?;
    cfg.options.validate()?;

    let dir = Path::new("/tmp/aidb-config-example");
    let _ = std::fs::remove_dir_all(dir);

    let db = DB::open(dir, cfg.options.clone())?;
    db.put(b"hello", b"aidb-config")?;
    assert_eq!(db.get(b"hello")?, Some(b"aidb-config".to_vec()));
    db.close()?;

    println!(
        "Loaded Options (memtable_size={}, compression={:?}), Cluster (groups={}), LogCommitter (max_entries={})",
        cfg.options.memtable_size,
        cfg.options.compression,
        cfg.cluster.group_count,
        cfg.log_committer.max_entries,
    );
    Ok(())
}
