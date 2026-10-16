//! Span 契约: 源码里只允许 `db_flush`, `cmp_run`, `raft_propose`.
//! @component aidb-observability

use std::path::{Path, PathBuf};

fn collect_rs(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_rs(&path, out);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

#[test]
fn only_allowed_spans() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    collect_rs(&root.join("src"), &mut files);
    let allowed = ["db_flush", "cmp_run", "raft_propose"];
    let mut violations = Vec::new();
    for path in files {
        let content = std::fs::read_to_string(&path).unwrap();
        let rel = path.strip_prefix(root).unwrap().display().to_string();
        if content.contains("#[instrument") || content.contains("#[tracing::instrument") {
            violations.push(format!("{rel} still has an instrument attribute"));
        }
        if !rel.ends_with("observability.rs") {
            for macro_name in ["debug_span!", "info_span!", "trace_span!"] {
                if content.contains(macro_name) {
                    violations.push(format!("{rel} has {macro_name}"));
                }
            }
        }
        let mut rest = content.as_str();
        while let Some(index) = rest.find("optional_span(\"") {
            let after = &rest[index + "optional_span(\"".len()..];
            let end = after.find('"').unwrap();
            let name = &after[..end];
            if !allowed.contains(&name) {
                violations.push(format!("{rel} optional_span {name}"));
            }
            rest = &after[end..];
        }
    }
    let block = std::fs::read_to_string(root.join("src/engine/sstable/block.rs")).unwrap();
    assert!(!block.contains("sst_block_seek"));
    for rel in ["src/engine/wal/writer.rs", "src/engine/wal/manager.rs"] {
        let content = std::fs::read_to_string(root.join(rel)).unwrap();
        assert!(!content.contains("name = \"wal_write\""), "{rel}");
    }
    assert!(
        violations.is_empty(),
        "disallowed spans:\n{}",
        violations.join("\n")
    );
}

#[test]
fn test_hot_path_span_level_is_debug() {
    only_allowed_spans();
}
