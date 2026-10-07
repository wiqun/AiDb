use std::sync::atomic::{AtomicBool, Ordering};

use tracing::Span;

static TRACES_ENABLED: AtomicBool = AtomicBool::new(false);

pub fn set_traces_enabled(on: bool) {
    TRACES_ENABLED.store(on, Ordering::Relaxed);
}

pub fn traces_enabled() -> bool {
    TRACES_ENABLED.load(Ordering::Relaxed)
}

/// 关闭时返回空 span, 不进入 `info_span!`. tracing 的 span 宏只接受字面量名字,
/// 所以打开时按允许的四个名字展开.
pub fn optional_span(name: &'static str) -> Span {
    if !traces_enabled() {
        return Span::none();
    }
    match name {
        "db_flush" => tracing::info_span!("db_flush"),
        "cmp_run" => tracing::info_span!("cmp_run"),
        "raft_propose" => tracing::info_span!("raft_propose"),
        "kv_command" => tracing::info_span!("kv_command", cmd = tracing::field::Empty),
        _ => Span::none(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use tracing::span::{Attributes, Id};
    use tracing::Subscriber;
    use tracing_subscriber::layer::Context;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::Layer;
    use tracing_subscriber::Registry;

    struct CountingKey {
        hits: Arc<AtomicUsize>,
    }

    impl std::fmt::Display for CountingKey {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.hits.fetch_add(1, Ordering::Relaxed);
            write!(f, "user-key")
        }
    }

    struct NameLayer {
        names: Arc<Mutex<Vec<String>>>,
    }

    impl<S: Subscriber> Layer<S> for NameLayer {
        fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
            self.names
                .lock()
                .unwrap()
                .push(attrs.metadata().name().to_string());
        }
    }

    #[test]
    fn disabled_trace_does_not_format_fields() {
        let hits = Arc::new(AtomicUsize::new(0));
        let key = CountingKey {
            hits: Arc::clone(&hits),
        };
        super::set_traces_enabled(false);
        let _span = super::optional_span("db_flush");
        assert_eq!(hits.load(Ordering::Relaxed), 0);

        let subscriber = Registry::default().with(tracing_subscriber::filter::LevelFilter::WARN);
        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!(key = %key, "skipped");
        });
        assert_eq!(hits.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn enabled_trace_enters_the_named_span() {
        super::set_traces_enabled(true);
        let names = Arc::new(Mutex::new(Vec::new()));
        let subscriber = Registry::default().with(NameLayer {
            names: Arc::clone(&names),
        });
        tracing::subscriber::with_default(subscriber, || {
            let span = super::optional_span("cmp_run");
            let _entered = span.entered();
        });
        assert!(names.lock().unwrap().iter().any(|name| name == "cmp_run"));
        super::set_traces_enabled(false);
    }
}
