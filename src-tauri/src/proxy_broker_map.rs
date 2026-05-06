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

/// Map between upstream Kafka brokers `(host, port)` and the local
/// loopback ports we've bound for them. The first entry is the
/// bootstrap broker the user configured; subsequent entries are
/// lazily added as Metadata / `FindCoordinator` / `DescribeCluster`
/// responses reveal new brokers.
///
/// Bidirectional: `ensure_listener(host, port)` allocates (or returns
/// the cached) local port. `upstream_for_local(local)` is used by
/// the per-listener pump to know where to forward bytes to.
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

    /// Ensure we have a local listener for the given upstream broker;
    /// returns the local port. If one exists already, return it; if
    /// not, bind a new ephemeral listener on `127.0.0.1` and stash it.
    ///
    /// NOTE: this only allocates the *port* (via `TcpListener::bind`
    /// followed by an immediate drop and rebind). The actual accept
    /// loop is spawned by `ProxyHandle::ensure_listener_running`.
    ///
    /// # Errors
    /// Bubbles up the `io::Error` if the bind fails.
    pub async fn ensure_listener(&self, host: &str, port: u16) -> io::Result<u16> {
        {
            let inner = self.inner.read();
            if let Some(&local) = inner.by_upstream.get(&(host.to_owned(), port)) {
                return Ok(local);
            }
        }
        // Bind ephemeral, read the assigned port, drop the listener.
        // The caller spawns the real accept loop separately.
        let temp = TcpListener::bind("127.0.0.1:0").await?;
        let local_port = temp.local_addr()?.port();
        drop(temp);
        {
            let mut inner = self.inner.write();
            inner
                .by_upstream
                .insert((host.to_owned(), port), local_port);
            inner.by_local.insert(local_port, (host.to_owned(), port));
        }
        Ok(local_port)
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
