//! Kapture proxy mode.
//!
//! A TCP intermediary that accepts Kafka client connections, forwards
//! every byte to a real upstream broker, and taps each frame to the
//! `ProtoCorrelator` so the Protocol tab shows the wire-level traffic
//! of the *client*, not of Kapture itself. See `docs/specs/proxy-mode.md`.
//!
//! Phase 1: single broker, plain TCP, no SASL, no TLS.

use std::net::SocketAddr;

// Skeleton type for Phase 1 — wired into AppState/commands in later tasks
// of the proxy-mode plan (Tasks 6–8). Allowing dead_code locally so the
// `-D warnings` gate passes while the module is still inert.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    /// `host:port` of the upstream Kafka broker we forward to.
    pub upstream: String,
    /// TCP port we bind on `127.0.0.1` for clients to connect to.
    pub listen_port: u16,
}

#[allow(dead_code)] // see note on `ProxyConfig`
impl ProxyConfig {
    #[must_use]
    pub const fn new(upstream: String, listen_port: u16) -> Self {
        Self {
            upstream,
            listen_port,
        }
    }

    #[must_use]
    pub fn listen_addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.listen_port))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn proxy_config_normalises_listen_addr() {
        let cfg = ProxyConfig::new("upstream:9092".to_owned(), 9092);
        assert_eq!(cfg.upstream, "upstream:9092");
        assert_eq!(cfg.listen_addr().to_string(), "127.0.0.1:9092");
    }
}
