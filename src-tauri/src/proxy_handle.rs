//! `ProxyHandle` + `ProxyInner` + listener-fleet plumbing.
//!
//! Split out of `proxy.rs` to stay under the file-size hook ceiling.
//! `proxy.rs` keeps the wire codec, header peek, correlation map,
//! pump implementations, and `build_proto_event`. This module owns
//! the multi-listener accept-loop fleet that grows on demand as the
//! response rewriter discovers new upstream brokers.

use std::collections::HashMap;
use std::io;
use std::mem;
use std::net::SocketAddr;
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::correlator::ProtoCorrelator;
use crate::proxy::{
    next_connection_id, run_pump_with_rewrite, BrokerMap, CorrelationMap, ProxyConfig,
};
use crate::proxy_broker_map::BrokerListener;
use crate::proxy_provisioner::BrokerProvisioner;

/// Shared proxy state. Wrapped in an `Arc` by `ProxyHandle` so the
/// per-listener accept loops AND the rewriter (via
/// `BrokerProvisioner`) see the same `BrokerMap`, `correlator`,
/// `stop_tx`, and listeners table.
pub struct ProxyInner {
    stop_tx: watch::Sender<bool>,
    /// Per-listener accept-loop join handles, keyed by the local port.
    /// Locked synchronously: NEVER `.await` while holding this lock.
    listeners: Mutex<HashMap<u16, JoinHandle<()>>>,
    /// Per-connection pump join handles, keyed by `ConnectionId`.
    /// Drained and aborted on stop/drop so established upstream sockets
    /// do not outlive proxy mode.
    active_pumps: Mutex<HashMap<u64, JoinHandle<()>>>,
    bootstrap_addr: SocketAddr,
    bootstrap_upstream: String,
    correlator: Arc<ProtoCorrelator>,
    broker_map: Arc<BrokerMap>,
    /// Serialises lazy listener provisioning so a discovered broker is
    /// inserted into `BrokerMap` and has its accept loop spawned as one
    /// cancellation-safe operation.
    provision_lock: TokioMutex<()>,
    /// Self-reference so `BrokerProvisioner::ensure` (which only sees
    /// `&self`) can promote to an `Arc<Self>` and spawn the accept
    /// loop for newly-discovered brokers. Set once, after the
    /// `Arc<ProxyInner>` is constructed.
    weak_self: Mutex<Weak<Self>>,
}

impl std::fmt::Debug for ProxyInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyInner")
            .field("bootstrap_addr", &self.bootstrap_addr)
            .field("bootstrap_upstream", &self.bootstrap_upstream)
            .field("listener_count", &self.listeners.lock().len())
            .field("active_pump_count", &self.active_pumps.lock().len())
            .finish_non_exhaustive()
    }
}

/// A running proxy. Drop / `stop()` to tear down listener accept loops
/// and active connection pumps.
pub struct ProxyHandle(Arc<ProxyInner>);

impl std::fmt::Debug for ProxyHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[async_trait]
impl BrokerProvisioner for ProxyInner {
    /// Atomically: check the broker map; if absent, bind a loopback
    /// listener and spawn its accept loop without dropping the socket
    /// between allocation and task ownership. The per-listener spawn
    /// step is idempotent — `spawn_listener` no-ops if there's already
    /// a `JoinHandle` in the listeners table.
    async fn ensure(&self, host: &str, port: u16) -> io::Result<u16> {
        let _guard = self.provision_lock.lock().await;
        let broker_listener = self.broker_map.ensure_bound_listener(host, port).await?;
        let local_port = broker_listener.local_port();
        let arc_self = self.weak_self.lock().upgrade().ok_or_else(|| {
            io::Error::other("ProxyInner self-reference dropped (proxy stopped?)")
        })?;
        match broker_listener {
            BrokerListener::Existing(_) => arc_self.spawn_listener(local_port)?,
            BrokerListener::Created { listener, .. } => {
                arc_self.spawn_bound_listener(local_port, listener)?;
            }
        }
        Ok(local_port)
    }
}

impl ProxyInner {
    /// Idempotent: bind a `127.0.0.1:local_port` listener and spawn
    /// its accept loop, recording the `JoinHandle`. If a `JoinHandle`
    /// is already recorded for this port, this is a no-op.
    ///
    /// `local_port` must already be present in `broker_map` (either
    /// via `BrokerMap::reserve` for the bootstrap or via
    /// `BrokerMap::ensure_bound_listener` for a discovered broker).
    fn spawn_listener(self: &Arc<Self>, local_port: u16) -> io::Result<()> {
        // Fast path: already have a JoinHandle.
        {
            let guard = self.listeners.lock();
            if guard.contains_key(&local_port) {
                return Ok(());
            }
        }

        // Synchronous bind is only used for an already-reserved port
        // (bootstrap/future manual warm-up). Lazy response provisioning
        // passes an already-bound socket through `spawn_bound_listener`.
        let std_listener = std::net::TcpListener::bind(("127.0.0.1", local_port))?;
        self.spawn_bound_listener(local_port, std_listener)
    }

    fn spawn_bound_listener(
        self: &Arc<Self>,
        local_port: u16,
        std_listener: std::net::TcpListener,
    ) -> io::Result<()> {
        {
            let guard = self.listeners.lock();
            if guard.contains_key(&local_port) {
                return Ok(());
            }
        }

        let Some((upstream_host, upstream_port)) = self.broker_map.upstream_for_local(local_port)
        else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("no broker_map entry for local port {local_port}"),
            ));
        };
        let upstream_target = format!("{upstream_host}:{upstream_port}");

        std_listener.set_nonblocking(true)?;
        let listener = TcpListener::from_std(std_listener)?;

        let task = spawn_accept_loop(Arc::clone(self), listener, upstream_target, local_port);

        // Re-check under the lock — between the contains_key fast
        // path and here, a parallel call may have raced us. If so,
        // abort our just-spawned task and use the existing one.
        let mut guard = self.listeners.lock();
        if guard.contains_key(&local_port) {
            drop(guard);
            task.abort();
            return Ok(());
        }
        guard.insert(local_port, task);
        drop(guard);
        Ok(())
    }
}

/// Spawn the accept loop for one listener. Each accepted connection
/// becomes one `run_pump_with_rewrite` task. The loop exits when
/// `stop_rx.changed()` flips to `true`.
fn spawn_accept_loop(
    inner: Arc<ProxyInner>,
    listener: TcpListener,
    upstream_target: String,
    local_port: u16,
) -> JoinHandle<()> {
    let mut stop_rx = inner.stop_tx.subscribe();
    let correlator = Arc::clone(&inner.correlator);
    let provisioner: Arc<dyn BrokerProvisioner> = inner.clone();

    tokio::spawn(async move {
        info!(listen_port = local_port, upstream = %upstream_target, "proxy listener up");
        loop {
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_ok() && *stop_rx.borrow() {
                        info!(listen_port = local_port, "proxy listener stopping");
                        break;
                    }
                }
                accepted = listener.accept() => {
                    match accepted {
                            Ok((client_sock, peer)) => {
                                let conn_id = next_connection_id();
                                let upstream_target = upstream_target.clone();
                                let correlator = Arc::clone(&correlator);
                                let corr_map = Arc::new(CorrelationMap::default());
                                let provisioner = Arc::clone(&provisioner);
                                let pump_inner = Arc::clone(&inner);
                                let (start_tx, start_rx) = oneshot::channel();
                                let task = tokio::spawn(async move {
                                    let _ = start_rx.await;
                                    let upstream_sock = match TcpStream::connect(&upstream_target).await {
                                        Ok(s) => s,
                                    Err(err) => {
                                        warn!(conn = conn_id.0, error = %err, "upstream connect failed");
                                        pump_inner.active_pumps.lock().remove(&conn_id.0);
                                        return;
                                    }
                                };
                                info!(conn = conn_id.0, peer = %peer, "proxy connection opened");
                                let result = run_pump_with_rewrite(
                                    conn_id,
                                    client_sock,
                                    upstream_sock,
                                    correlator,
                                    corr_map,
                                    provisioner,
                                )
                                .await;
                                    if let Err(err) = result {
                                        warn!(conn = conn_id.0, error = %err, "proxy pump error");
                                    }
                                    info!(conn = conn_id.0, "proxy connection closed");
                                    pump_inner.active_pumps.lock().remove(&conn_id.0);
                                });
                                inner.active_pumps.lock().insert(conn_id.0, task);
                                let _ = start_tx.send(());
                            }
                        Err(err) => {
                            warn!(error = %err, "proxy accept failed");
                        }
                    }
                }
            }
        }
    })
}

impl ProxyHandle {
    /// Bind the bootstrap listener, seed the broker map with
    /// `(upstream → bootstrap_local_port)`, spawn the bootstrap accept
    /// loop, and return.
    ///
    /// # Errors
    /// - `InvalidInput` if `config.upstream` doesn't parse as `host:port`.
    /// - Underlying `io::Error` if the listener bind fails.
    pub async fn start(config: ProxyConfig, correlator: Arc<ProtoCorrelator>) -> io::Result<Self> {
        let (host, port) = parse_host_port(&config.upstream)?;

        let listener = TcpListener::bind(config.listen_addr()).await?;
        let local_addr = listener.local_addr()?;
        let bootstrap_local_port = local_addr.port();
        let (stop_tx, _stop_rx) = watch::channel(false);

        let broker_map = Arc::new(BrokerMap::new());
        broker_map.reserve(host, port, bootstrap_local_port);

        let inner = Arc::new(ProxyInner {
            stop_tx,
            listeners: Mutex::new(HashMap::new()),
            active_pumps: Mutex::new(HashMap::new()),
            bootstrap_addr: local_addr,
            bootstrap_upstream: config.upstream.clone(),
            correlator,
            broker_map,
            provision_lock: TokioMutex::new(()),
            weak_self: Mutex::new(Weak::new()),
        });
        *inner.weak_self.lock() = Arc::downgrade(&inner);

        let task = spawn_accept_loop(
            Arc::clone(&inner),
            listener,
            config.upstream.clone(),
            bootstrap_local_port,
        );
        inner.listeners.lock().insert(bootstrap_local_port, task);

        info!(listen = %local_addr, upstream = %config.upstream, "proxy listening");
        Ok(Self(inner))
    }

    /// Idempotent: ensure an accept loop is running for the given
    /// local port. The port must already be reserved in the broker
    /// map (via `reserve` or a prior `ensure_listener`).
    ///
    /// # Errors
    /// Bubbles up the bind error if `127.0.0.1:local_port` can't be
    /// claimed.
    #[allow(dead_code)] // exposed for tests / future explicit warm-up
    pub fn ensure_listener_running(&self, local_port: u16) -> io::Result<()> {
        self.0.spawn_listener(local_port)
    }

    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.0.bootstrap_addr
    }

    /// Diagnostic accessor — exposed for future `SidePanel` summary
    /// ("proxy :9092 → upstream:9092"). Phase 1 doesn't render this.
    #[allow(dead_code)]
    #[must_use]
    pub fn upstream(&self) -> &str {
        &self.0.bootstrap_upstream
    }

    /// Shared `BrokerMap`. Tests reach in to assert post-rewrite state.
    #[allow(dead_code)]
    #[must_use]
    pub fn broker_map(&self) -> Arc<BrokerMap> {
        Arc::clone(&self.0.broker_map)
    }

    pub async fn stop(self) {
        let _ = self.0.stop_tx.send(true);
        // Drain the listeners table under the lock, then await
        // outside it (await-while-locked is forbidden — see the
        // `Mutex<HashMap>` doc comment).
        let drained: HashMap<u16, JoinHandle<()>> = mem::take(&mut self.0.listeners.lock());
        for (_, task) in drained {
            let _ = task.await;
        }
        let drained_pumps: HashMap<u64, JoinHandle<()>> =
            mem::take(&mut self.0.active_pumps.lock());
        for (_, task) in drained_pumps {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for ProxyHandle {
    fn drop(&mut self) {
        let _ = self.0.stop_tx.send(true);
        // Best-effort abort on listener and pump tables. If another Arc
        // owner outlives us, listener tasks will still observe the stop
        // signal and exit cleanly.
        let drained: HashMap<u16, JoinHandle<()>> = mem::take(&mut self.0.listeners.lock());
        for (_, task) in drained {
            task.abort();
        }
        let drained_pumps: HashMap<u64, JoinHandle<()>> =
            mem::take(&mut self.0.active_pumps.lock());
        for (_, task) in drained_pumps {
            task.abort();
        }
    }
}

/// Parse `host:port`. Rejects empty host, missing colon, or non-u16
/// port. Returns `InvalidInput` on any failure so the Tauri command
/// layer can surface it as a config error.
#[allow(clippy::missing_errors_doc)]
fn parse_host_port(upstream: &str) -> io::Result<(String, u16)> {
    let (host, port) = upstream.rsplit_once(':').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("upstream missing port: {upstream}"),
        )
    })?;
    if host.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("upstream missing host: {upstream}"),
        ));
    }
    let port: u16 = port.parse().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("upstream port `{port}` is invalid: {err}"),
        )
    })?;
    Ok((host.to_owned(), port))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    use bytes::BytesMut;
    use futures::{SinkExt, StreamExt};
    use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
    use kafka_protocol::messages::{ApiKey, BrokerId, MetadataResponse, ResponseHeader};
    use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
    use tokio::io::AsyncWriteExt;

    use crate::correlator::ProtoCorrelator;
    use crate::proxy::framed_kafka;

    fn build_metadata_response_bytes(version: i16, brokers: Vec<(i32, &str, i32)>) -> Vec<u8> {
        let mut resp = MetadataResponse::default();
        resp.brokers = brokers
            .into_iter()
            .map(|(node_id, host, port)| {
                let mut b = MetadataResponseBroker::default();
                b.node_id = BrokerId(node_id);
                b.host = StrBytes::from_string(host.to_owned());
                b.port = port;
                b
            })
            .collect();
        let header_version = ApiKey::Metadata.response_header_version(version);
        let mut out = BytesMut::new();
        ResponseHeader::default()
            .encode(&mut out, header_version)
            .unwrap();
        resp.encode(&mut out, version).unwrap();
        out.to_vec()
    }

    /// One Metadata response advertising two distinct upstream brokers
    /// (excluding the bootstrap). After the rewriter runs, the proxy
    /// must have lazily bound a listener for each — verified by both
    /// `BrokerMap` state AND a real TCP `connect` on the rewritten
    /// port for broker 2.
    #[tokio::test]
    async fn proxy_handle_provisions_a_listener_per_upstream_broker_observed() {
        // Two fake "satellite" upstream brokers — each accepts one
        // connection and immediately drops it. Their existence on a
        // real port lets us assert the rewriter routed to a working
        // local listener (which forwards to a working upstream).
        let broker1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let broker1_port = broker1.local_addr().unwrap().port();
        let broker2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let broker2_port = broker2.local_addr().unwrap().port();
        tokio::spawn(async move {
            // Best-effort: accept one and drop. The test asserts the
            // listener-side connect; what happens upstream after that
            // doesn't matter for this test.
            let _ = broker1.accept().await;
        });
        tokio::spawn(async move {
            let _ = broker2.accept().await;
        });

        // Bootstrap upstream: on the first client request, replies
        // with a Metadata v12 response advertising broker1 + broker2.
        let bootstrap = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bootstrap_addr = bootstrap.local_addr().unwrap();
        tokio::spawn(async move {
            let (sock, _) = bootstrap.accept().await.unwrap();
            let mut framed = framed_kafka(sock);
            let _req = framed.next().await.unwrap().unwrap();
            let body = build_metadata_response_bytes(
                12,
                vec![
                    (1, "127.0.0.1", i32::from(broker1_port)),
                    (2, "127.0.0.1", i32::from(broker2_port)),
                ],
            );
            // Splice the request's corr_id (42) — the test sends it.
            let mut buf = BytesMut::from(&body[..]);
            buf[0..4].copy_from_slice(&42i32.to_be_bytes());
            framed.send(buf.freeze()).await.unwrap();
            // Linger a bit so the proxy finishes forwarding the
            // rewritten response before we drop the upstream socket.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        });

        // Start the proxy pointing at the bootstrap.
        let correlator = Arc::new(ProtoCorrelator::new());
        let cfg = ProxyConfig::new(bootstrap_addr.to_string(), 0);
        let handle = ProxyHandle::start(cfg, Arc::clone(&correlator))
            .await
            .unwrap();
        let listen_addr = handle.local_addr();

        // Drive a Metadata v12 request through the proxy.
        let mut client = tokio::net::TcpStream::connect(listen_addr).await.unwrap();
        let mut req = Vec::new();
        req.extend_from_slice(&3i16.to_be_bytes()); // api_key = Metadata
        req.extend_from_slice(&12i16.to_be_bytes()); // api_version = 12
        req.extend_from_slice(&42i32.to_be_bytes()); // corr_id = 42
        req.extend_from_slice(&(-1i16).to_be_bytes()); // client_id = null
        req.push(0); // tagged fields = 0
        req.push(0xFF); // null topics array (flexible)
        req.push(0); // tagged fields
        let len = u32::try_from(req.len()).unwrap();
        client.write_all(&len.to_be_bytes()).await.unwrap();
        client.write_all(&req).await.unwrap();

        let mut framed_client = framed_kafka(client);
        let resp = framed_client.next().await.unwrap().unwrap();
        let mut buf = resp.freeze();

        let corr_id = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(corr_id, 42);
        let header_version = ApiKey::Metadata.response_header_version(12);
        let _hdr = ResponseHeader::decode(&mut buf, header_version).unwrap();
        let decoded = MetadataResponse::decode(&mut buf, 12).unwrap();
        assert_eq!(decoded.brokers.len(), 2);

        // BrokerMap must hold the bootstrap (pre-seeded) PLUS the two
        // advertised brokers — three entries total.
        let broker_map = handle.broker_map();
        let snapshot = broker_map.snapshot();
        assert!(
            snapshot.len() >= 3,
            "expected ≥ 3 broker_map entries (bootstrap + 2 satellites), got {}: {:?}",
            snapshot.len(),
            snapshot,
        );

        // Find broker 2's local port and verify the listener is up
        // by opening a real TCP connection to it.
        let local_port_for_broker2 = decoded
            .brokers
            .iter()
            .find(|b| b.node_id.0 == 2)
            .map(|b| u16::try_from(b.port).unwrap())
            .unwrap();
        let probe = tokio::net::TcpStream::connect(("127.0.0.1", local_port_for_broker2)).await;
        assert!(
            probe.is_ok(),
            "expected to connect to rewritten broker-2 listener at 127.0.0.1:{local_port_for_broker2}: {:?}",
            probe.err(),
        );

        handle.stop().await;
    }
}
