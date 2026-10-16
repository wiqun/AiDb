//! BlockCache 模块级 dataflow — cache hit / miss event
//! @component aidb-cache

use aidb::engine::cache::{BlockCache, CacheKey};
use bytes::Bytes;

use crate::common::dataflow::capture_spans_under_lock;
use crate::common::observability::{capture_events_under_lock, tracing_test_lock};

/// 验证 BlockCache Hit 与 Miss 的可观测性跟踪日志
#[test]
fn test_cache_observability() {
    let _lock = tracing_test_lock();
    let cache = BlockCache::new(4096);
    let key = CacheKey {
        file_number: 1,
        offset: 128,
    };
    cache.insert(key.clone(), Bytes::from_static(b"block-data"));

    let miss_caps = capture_spans_under_lock(|| {
        assert!(cache
            .get(CacheKey {
                file_number: 2,
                offset: 0,
            })
            .is_none());
    });
    assert!(miss_caps.spans_named("cache_get").is_empty());

    let hit_caps = capture_spans_under_lock(|| {
        assert_eq!(
            cache.get(key.clone()).unwrap(),
            Bytes::from_static(b"block-data")
        );
    });
    assert!(hit_caps.spans_named("cache_get").is_empty());

    let events = capture_events_under_lock(|| {
        cache.reset_stats();
        let _ = cache.get(key.clone());
        let _ = cache.get(key);
    });
    assert!(
        events.iter().any(|e| e.contains("cache.hit")),
        "missing cache.hit event, got: {events:?}"
    );
}
