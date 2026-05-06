//! `BrokerProvisioner` — abstraction the response rewriter calls when
//! it discovers a new upstream broker.
//!
//! Phase 2: a single concrete impl on `ProxyInner` (the proxy's shared
//! state) does the atomic "allocate port + bind listener + spawn accept
//! loop + record in `BrokerMap`" dance. Tests use `BrokerMap` directly
//! via the dumb impl below — it only allocates the port (no accept
//! loop), which is enough for the rewriter unit tests that don't drive
//! traffic through the lazy listener.
//!
//! Split out of `proxy.rs` so that file stays under the size hook
//! ceiling (~1000 lines).
//!
//! # Errors / listener ownership
//! The real impl in `ProxyInner::ensure` uses
//! `BrokerMap::ensure_bound_listener`, which returns the still-open
//! loopback listener for newly discovered brokers. The accept loop takes
//! ownership of that socket directly, so there is no drop/rebind window
//! for another local process to claim the advertised proxy port.

use std::io;

use async_trait::async_trait;

use crate::proxy_broker_map::BrokerMap;

/// Strategy contract used by `proxy_rewrite::rewrite_response` to
/// resolve `(upstream_host, upstream_port) → local_port`. Implementors
/// must be safe to share behind an `Arc<dyn BrokerProvisioner>` —
/// hence the `Send + Sync + 'static` bounds.
#[async_trait]
pub trait BrokerProvisioner: Send + Sync {
    /// Resolve (or create) the local proxy port that fronts the given
    /// upstream broker. The first call binds + spawns; subsequent
    /// calls return the cached port.
    async fn ensure(&self, host: &str, port: u16) -> io::Result<u16>;
}

/// Bare `BrokerMap` provisioner — *port allocation only, no accept
/// loop*. Useful in unit tests of the rewriter where we just want to
/// observe that the map gets populated. Production code uses
/// `ProxyInner` instead.
#[async_trait]
impl BrokerProvisioner for BrokerMap {
    async fn ensure(&self, host: &str, port: u16) -> io::Result<u16> {
        self.ensure_listener(host, port).await
    }
}
