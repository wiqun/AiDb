/// AiDb 基础用法示例.
///
/// 运行: cargo run --example basic
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = Path::new("/tmp/aidb-example");
    let _ = std::fs::remove_dir_all(dir);

    // 打开/创建数据库
    let db = aidb::DB::open(dir, aidb::config::Options::default())?;
    println!("✅ 数据库已创建");

    // 写入
    db.put(b"hello", b"world")?;
    db.put(b"foo", b"bar")?;
    println!("✅ 写入 2 个 key");

    // 读取
    let val = db.get(b"hello")?.unwrap();
    assert_eq!(val, b"world");
    println!("✅ 读取 hello = {:?}", String::from_utf8_lossy(&val));

    // 删除
    db.delete(b"foo")?;
    println!("✅ 删除 foo");

    // 确认删除
    assert_eq!(db.get(b"foo")?, None);
    println!("✅ 确认 foo 不存在");

    // 批量写入
    let mut batch = aidb::WriteBatch::new();
    batch.put(b"batch1", b"value1");
    batch.put(b"batch2", b"value2");
    batch.put(b"batch3", b"value3");
    db.write(&batch)?;
    println!("✅ 批量写入 3 个 key");

    // 范围扫描
    println!("  范围扫描 [a, z):");
    let iter = db.scan(Some(b"a"), Some(b"z"))?;
    for entry in iter {
        let (k, v) = entry?;
        println!(
            "    {} => {}",
            String::from_utf8_lossy(&k),
            String::from_utf8_lossy(&v)
        );
    }

    // 快照读
    let snapshot = db.snapshot()?;
    println!("✅ 创建快照, sequence = {}", snapshot.sequence());

    // 关闭
    db.close()?;
    println!("✅ 数据库已关闭");

    Ok(())
}
