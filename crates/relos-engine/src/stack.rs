use std::sync::Arc;

use relos_core::Result;
use relos_log::Loglet;
use relos_store::LocalStore;

use crate::{Applicator, BaseEngine, Engine};

/// A middleware that wraps both an engine and an applicator.
///
/// Implementations provide the dual wrapping needed to insert a layer into the
/// engine stack: one function wraps the downstream engine (for propose/sync),
/// and the other wraps the upstream applicator (for apply).
pub trait MiddleEngine: Send + Sync {
    /// Wrap the downstream engine.
    fn wrap_engine(&self, inner: Arc<dyn Engine>) -> Arc<dyn Engine>;

    /// Wrap the upstream applicator.
    fn wrap_applicator(&self, inner: Box<dyn Applicator>) -> Box<dyn Applicator>;
}

/// A builder that composes engines bottom-up into a layered stack.
///
/// Middlewares are pushed in order from bottom to top. The [`build`](EngineStackBuilder::build)
/// method creates a [`BaseEngine`] at the bottom, wraps the applicator top-down
/// (so the outermost middleware's applicator is the first to see entries), and
/// wraps the engine bottom-up (so the outermost middleware's engine is the one
/// callers interact with).
pub struct EngineStackBuilder {
    middlewares: Vec<Box<dyn MiddleEngine>>,
}

impl EngineStackBuilder {
    /// Create a new empty builder.
    pub fn new() -> Self {
        Self {
            middlewares: Vec::new(),
        }
    }

    /// Push a middleware onto the stack.
    ///
    /// Middlewares are applied in push order: the first pushed sits closest
    /// to the BaseEngine, the last pushed is the outermost layer.
    pub fn push(mut self, middleware: Box<dyn MiddleEngine>) -> Self {
        self.middlewares.push(middleware);
        self
    }

    /// Build the complete engine stack.
    ///
    /// Order: BaseEngine at the bottom, middlewares applied in push order,
    /// the provided applicator at the top.
    pub async fn build(
        self,
        log: Arc<dyn Loglet>,
        store: Arc<dyn LocalStore>,
        applicator: Box<dyn Applicator>,
    ) -> Result<Arc<dyn Engine>> {
        // Wrap the applicator top-down: last-pushed middleware wraps first,
        // so it is the outermost applicator layer.
        let mut app = applicator;
        for middleware in self.middlewares.iter().rev() {
            app = middleware.wrap_applicator(app);
        }

        // Create the BaseEngine at the bottom with the fully-wrapped applicator.
        let mut engine: Arc<dyn Engine> =
            BaseEngine::new(log, store, Arc::from(app)).await?;

        // Wrap the engine bottom-up: first-pushed middleware wraps first
        // (closest to BaseEngine), last-pushed is outermost.
        for middleware in &self.middlewares {
            engine = middleware.wrap_engine(engine);
        }

        Ok(engine)
    }
}

impl Default for EngineStackBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ReturnValue;
    use relos_core::{Entry, LogPos};
    use relos_log::{
        LogChain, Loglet, LogletFactory, MemoryLoglet, MemoryMetaStore, MetaStore, VirtualLog,
    };
    use relos_store::{MemoryStore, WriteTransaction};

    struct MemoryLogletFactory;

    #[async_trait::async_trait]
    impl LogletFactory for MemoryLogletFactory {
        async fn create(&self, _loglet_id: &str) -> Result<Arc<dyn Loglet>> {
            Ok(Arc::new(MemoryLoglet::new()))
        }
    }

    async fn make_virtual_log() -> Arc<dyn Loglet> {
        let chain = LogChain::new("loglet-0".to_string());
        let meta_store = Arc::new(MemoryMetaStore::new(chain)) as Arc<dyn MetaStore>;
        let factory = Arc::new(MemoryLogletFactory) as Arc<dyn LogletFactory>;
        Arc::new(VirtualLog::new(meta_store, factory).await.unwrap())
    }

    struct TestApplicator;

    impl Applicator for TestApplicator {
        fn apply(
            &self,
            txn: &mut dyn WriteTransaction,
            entry: &Entry,
            pos: LogPos,
        ) -> Result<ReturnValue> {
            let key = format!("entry:{}", pos);
            txn.put(key.as_bytes(), &entry.payload)?;
            Ok(ReturnValue::Data(entry.payload.clone()))
        }
    }

    #[tokio::test]
    async fn test_build_minimal_stack() {
        let vlog = make_virtual_log().await;
        let store = Arc::new(MemoryStore::new());

        let engine = EngineStackBuilder::new()
            .build(vlog, store.clone(), Box::new(TestApplicator))
            .await
            .unwrap();

        // Propose and verify.
        let rv = engine
            .propose(Entry::new(b"minimal".to_vec()))
            .await
            .unwrap();
        match rv {
            ReturnValue::Data(d) => assert_eq!(d, b"minimal"),
            _ => panic!("expected Data return value"),
        }

        let tail = engine.sync().await.unwrap();
        assert_eq!(tail, 2);

        let txn = store.begin_read_txn().unwrap();
        assert_eq!(txn.get(b"entry:1").unwrap(), Some(b"minimal".to_vec()));
    }

    #[tokio::test]
    async fn test_build_with_middleware() {
        use crate::view_tracking::ViewTrackingEngine;
        use std::collections::HashMap;
        use tokio::sync::RwLock;

        let vlog = make_virtual_log().await;
        let store = Arc::new(MemoryStore::new());

        // Create a ViewTracking middleware via the MiddleEngine trait.
        let views = Arc::new(RwLock::new(HashMap::new()));
        let server_id = 1u64;

        struct ViewTrackingMiddleware {
            server_id: u64,
            views: Arc<RwLock<HashMap<u64, u64>>>,
        }

        impl MiddleEngine for ViewTrackingMiddleware {
            fn wrap_engine(&self, inner: Arc<dyn Engine>) -> Arc<dyn Engine> {
                Arc::new(ViewTrackingEngine {
                    inner,
                    server_id: self.server_id,
                    views: self.views.clone(),
                })
            }

            fn wrap_applicator(&self, inner: Box<dyn Applicator>) -> Box<dyn Applicator> {
                Box::new(crate::view_tracking::ViewTrackingApplicator {
                    inner,
                    views: self.views.clone(),
                })
            }
        }

        let middleware = ViewTrackingMiddleware {
            server_id,
            views: views.clone(),
        };

        let engine = EngineStackBuilder::new()
            .push(Box::new(middleware))
            .build(vlog.clone(), store.clone(), Box::new(TestApplicator))
            .await
            .unwrap();

        // Propose an entry.
        let rv = engine
            .propose(Entry::new(b"stacked".to_vec()))
            .await
            .unwrap();
        match rv {
            ReturnValue::Data(d) => assert_eq!(d, b"stacked"),
            _ => panic!("expected Data return value"),
        }

        // The view-tracking header should have been injected.
        let log_entry = vlog.read(1).await.unwrap().expect("entry should exist");
        assert!(log_entry.get_header("view_tracking").is_some());

        // The view map should have been updated during apply.
        let v = views.read().await;
        assert!(v.contains_key(&server_id));
    }
}
