//! Bidirectional `(upstream_host, upstream_port) ↔ local_port` map
//! used by proxy mode to route client traffic back through Kapture
//! after a Metadata / `FindCoordinator` / `DescribeCluster` rewrite.
//!
//! Split out of `proxy.rs` so the file stays under the size budget;
//! this module is otherwise a leaf — no cycles back into `proxy.rs`.

use std::collections::HashMap;
use std::io;

use parking_lot::RwLock;
use tokio::net::TcpListener;

/// Hard cap on broker endpoints the proxy will provision for one run.
/// Normal Kafka clusters are far below this; the cap prevents a malicious
/// Metadata response from consuming thousands of loopback ports and tasks.
pub const MAX_BROKER_MAP_ENTRIES: usize = 1024;

#[derive(Debug)]
pub enum BrokerListener {
    Existing(u16),
    Created {
        local_port: u16,
        listener: std::net::TcpListener,
    },
}

impl BrokerListener {
    #[must_use]
    pub const fn local_port(&self) -> u16 {
        match self {
            Self::Existing(local_port) | Self::Created { local_port, .. } => *local_port,
        }
    }
}

/// Map between upstream Kafka brokers `(host, port)` and the local
/// loopback ports we've bound for them. The first entry is the
/// bootstrap broker the user configured; subsequent entries are
/// lazily added as Metadata / `FindCoordinator` / `DescribeCluster`
/// responses reveal new brokers.
///
/// Bidirectional: `ensure_bound_listener(host, port)` allocates (or
/// returns the cached) local port. `upstream_for_local(local)` is used
/// by the per-listener pump to know where to forward bytes to.
#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct BrokerMap {
    inner: RwLock<BrokerMapInner>,
}

#[derive(Debug, Default)]
struct BrokerMapInner {
    by_upstream: HashMap<(String, u16), u16>,
    by_local: HashMap<u16, (String, u16)>,
}

#[allow(dead_code)]
impl BrokerMap {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Ensure we have a bound local listener for the given upstream
    /// broker. If one exists already, return its local port; if not,
    /// bind a new ephemeral listener on `127.0.0.1`, stash it, and
    /// return the still-open listener to the caller.
    ///
    /// The returned `Created` listener closes the drop/rebind race:
    /// production provisioning passes the socket directly into the
    /// accept-loop task instead of dropping it and binding the same
    /// port again later.
    ///
    /// # Errors
    /// Bubbles up the `io::Error` if the bind fails or if the broker
    /// cap has already been reached.
    pub async fn ensure_bound_listener(&self, host: &str, port: u16) -> io::Result<BrokerListener> {
        {
            let inner = self.inner.read();
            if let Some(&local) = inner.by_upstream.get(&(host.to_owned(), port)) {
                return Ok(BrokerListener::Existing(local));
            }
            if inner.by_upstream.len() >= MAX_BROKER_MAP_ENTRIES {
                return Err(io::Error::other(format!(
                    "proxy broker map limit reached ({MAX_BROKER_MAP_ENTRIES})"
                )));
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_port = listener.local_addr()?.port();
        let listener = listener.into_std()?;

        {
            let mut inner = self.inner.write();
            if let Some(&local) = inner.by_upstream.get(&(host.to_owned(), port)) {
                return Ok(BrokerListener::Existing(local));
            }
            if inner.by_upstream.len() >= MAX_BROKER_MAP_ENTRIES {
                return Err(io::Error::other(format!(
                    "proxy broker map limit reached ({MAX_BROKER_MAP_ENTRIES})"
                )));
            }
            inner
                .by_upstream
                .insert((host.to_owned(), port), local_port);
            inner.by_local.insert(local_port, (host.to_owned(), port));
        }
        Ok(BrokerListener::Created {
            local_port,
            listener,
        })
    }

    /// Ensure we have a local port for the given upstream broker.
    ///
    /// Production code uses `ensure_bound_listener` to avoid dropping
    /// the socket before the accept loop starts. This port-only helper
    /// exists for tests and bare `BrokerMap` provisioner callers that
    /// only need deterministic rewrite output.
    ///
    /// # Errors
    /// Bubbles up the `io::Error` if the bind fails.
    pub async fn ensure_listener(&self, host: &str, port: u16) -> io::Result<u16> {
        self.ensure_bound_listener(host, port)
            .await
            .map(|listener| listener.local_port())
    }

    /// Reserve a specific local port for an upstream — used to seed the
    /// map with the bootstrap broker when the user configures a fixed
    /// listen port.
    pub fn reserve(&self, host: String, port: u16, local_port: u16) {
        let mut inner = self.inner.write();
        inner.by_upstream.insert((host.clone(), port), local_port);
        inner.by_local.insert(local_port, (host, port));
    }

    #[must_use]
    pub fn upstream_for_local(&self, local: u16) -> Option<(String, u16)> {
        self.inner.read().by_local.get(&local).cloned()
    }

    #[must_use]
    pub fn snapshot(&self) -> Vec<((String, u16), u16)> {
        self.inner
            .read()
            .by_upstream
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn broker_map_returns_same_local_port_for_same_upstream() {
        let map = BrokerMap::new();
        let p1 = map.ensure_listener("kafka-mb-2", 39093).await.unwrap();
        let p2 = map.ensure_listener("kafka-mb-2", 39093).await.unwrap();
        assert_eq!(p1, p2);
        let p3 = map.ensure_listener("kafka-mb-3", 39094).await.unwrap();
        assert_ne!(p1, p3);
    }

    #[tokio::test]
    async fn broker_map_lookup_returns_upstream_for_local_port() {
        let map = BrokerMap::new();
        let local = map
            .ensure_listener("upstream.example.com", 9092)
            .await
            .unwrap();
        let upstream = map.upstream_for_local(local).unwrap();
        assert_eq!(upstream, ("upstream.example.com".to_owned(), 9092));
    }
}
