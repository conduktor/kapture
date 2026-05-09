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
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{info, warn};

use crate::correlator::ProtoCorrelator;
use crate::message::CapturedMessage;
use crate::proxy::{
    build_proto_event, framed_kafka, next_connection_id, run_pump_with_rewrite, BrokerMap,
    ConnectionId, CorrelationMap, ProxyConfig, ProxyDirection,
};
use crate::proxy_broker_map::BrokerListener;
use crate::proxy_provisioner::BrokerProvisioner;
use crate::proxy_topic_ids::TopicIdMap;
use crate::proxy_upstream::{
    open_upstream, resolve_server_name, UpstreamSaslConfig, UpstreamTlsConfig,
};

/// Sink for `CapturedMessage` instances extracted from Produce
/// requests / Fetch responses traversing the proxy. Invoked
/// synchronously from the per-connection pump — must not block.
pub type RecordSink = Arc<dyn Fn(CapturedMessage) + Send + Sync + 'static>;

/// Diagnostic snapshot of proxy state. Returned by
/// `ProxyHandle::summary` and surfaced both via the Tauri command
/// `proxy_status` (for the `SidePanel`) and via the
/// `kapture_proxy_status` MCP tool (for AI agents).
#[derive(Debug, Clone)]
pub struct ProxySummary {
    pub listen_addr: String,
    pub upstream: String,
    pub active_connections: usize,
    /// `((upstream_host, upstream_port), local_port)` triples sorted
    /// by local port so the order is stable across polls.
    pub broker_mappings: Vec<((String, u16), u16)>,
}

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
    /// TLS config to use when opening upstream connections (bootstrap
    /// plus every lazily-bound satellite broker). Same config for all
    /// brokers; Kafka deployments share TLS server certs cluster-wide.
    upstream_tls: Option<UpstreamTlsConfig>,
    /// SASL credentials to use when opening upstream connections. Same
    /// credentials for all brokers; they're cluster-wide in standard
    /// Kafka deployments.
    upstream_sasl: Option<UpstreamSaslConfig>,
    correlator: Arc<ProtoCorrelator>,
    broker_map: Arc<BrokerMap>,
    topic_id_map: Arc<TopicIdMap>,
    record_sink: RecordSink,
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
        let broker_listener = self.broker_map.ensure_bound_listener(host, port).await
            .inspect_err(|err| {
                // Bind failures are the silent-bypass culprit: if a
                // MetadataResponse advertises a broker we can't bind a
                // listener for, the rewriter forwards the original
                // (upstream) host:port to the client, which then
                // connects directly — bypassing Kapture for that
                // broker. Surface loudly.
                warn!(host, port, %err, "broker listener bind FAILED — clients will bypass proxy for this broker");
            })?;
        let local_port = broker_listener.local_port();
        let arc_self = self.weak_self.lock().upgrade().ok_or_else(|| {
            io::Error::other("ProxyInner self-reference dropped (proxy stopped?)")
        })?;
        match broker_listener {
            BrokerListener::Existing(_) => arc_self.spawn_listener(local_port)?,
            BrokerListener::Created { listener, .. } => {
                arc_self.spawn_bound_listener(local_port, listener)?;
                info!(host, port, local_port, "broker listener bound (lazy)");
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
    let record_sink = Arc::clone(&inner.record_sink);
    let topic_id_map = Arc::clone(&inner.topic_id_map);
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
                                let record_sink = Arc::clone(&record_sink);
                                let topic_id_map = Arc::clone(&topic_id_map);
                                let upstream_tls = pump_inner.upstream_tls.clone();
                                let upstream_sasl = pump_inner.upstream_sasl.clone();
                                let (start_tx, start_rx) = oneshot::channel();
                                let task = tokio::spawn(async move {
                                    let _ = start_rx.await;
                                    let (upstream_host, upstream_port) =
                                        match parse_host_port(&upstream_target) {
                                            Ok(hp) => hp,
                                            Err(err) => {
                                                warn!(conn = conn_id.0, error = %err, "upstream target invalid");
                                                pump_inner.active_pumps.lock().remove(&conn_id.0);
                                                return;
                                            }
                                        };
                                    // Per-broker SNI fallback. If the user left
                                    // server_name blank in the dialog, derive it
                                    // from THIS broker's host — multi-broker
                                    // clusters where each broker advertises its
                                    // own DNS name still get the correct SNI.
                                    let resolved_tls = upstream_tls
                                        .as_ref()
                                        .map(|cfg| resolve_server_name(&upstream_host, cfg));
                                    // Lazy upstream open. Wrap the client in
                                    // the Kafka codec up front so we can
                                    // surface any frames the client sends
                                    // while we're still trying to reach the
                                    // broker. Each failed connect → drain one
                                    // client frame, emit it as a Send event
                                    // tagged with `frame_error`, retry. The
                                    // moment upstream comes up, we hand the
                                    // (already-framed) client to the normal
                                    // pump and proceed transparently.
                                    let mut client_framed = framed_kafka(client_sock);
                                    let Some(upstream_sock) = drain_until_upstream(
                                        &upstream_host,
                                        upstream_port,
                                        resolved_tls.as_ref(),
                                        upstream_sasl.as_ref(),
                                        &mut client_framed,
                                        conn_id,
                                        local_port,
                                        &correlator,
                                        &corr_map,
                                    )
                                    .await
                                    else {
                                        // Client gave up while we were
                                        // retrying upstream — pump never
                                        // started, just clean up.
                                        pump_inner.active_pumps.lock().remove(&conn_id.0);
                                        return;
                                    };
                                info!(conn = conn_id.0, peer = %peer, "proxy connection opened");
                                let result = run_pump_with_rewrite(
                                    conn_id,
                                    local_port,
                                    client_framed,
                                    upstream_sock,
                                    correlator,
                                    corr_map,
                                    provisioner,
                                    record_sink,
                                    topic_id_map,
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

/// Connect-loop run **before** entering the main pump.
///
/// Tries `open_upstream` with a per-attempt 1.5 s ceiling — fast-fails
/// on `ECONNREFUSED`, bounded on hung-route / dropped-firewall cases.
/// While upstream is unreachable, frames the client sends are decoded
/// and pushed to the correlator with `frame_error` set, so the user
/// sees the client's request burst (and retry pattern) in the
/// Protocol tab even though Kapture has nothing to forward to.
///
/// Returns `Some(stream)` once upstream is up, or `None` if the client
/// disconnected before that ever happened.
#[allow(clippy::too_many_arguments)]
async fn drain_until_upstream(
    host: &str,
    port: u16,
    tls: Option<&UpstreamTlsConfig>,
    sasl: Option<&UpstreamSaslConfig>,
    client_framed: &mut tokio_util::codec::Framed<
        tokio::net::TcpStream,
        tokio_util::codec::LengthDelimitedCodec,
    >,
    conn_id: ConnectionId,
    local_port: u16,
    correlator: &Arc<ProtoCorrelator>,
    corr_map: &Arc<CorrelationMap>,
) -> Option<crate::proxy_upstream::UpstreamStream> {
    loop {
        let attempt = timeout(
            Duration::from_millis(1500),
            open_upstream(host, port, tls, sasl),
        )
        .await;
        let connect_err = match attempt {
            Ok(Ok(stream)) => return Some(stream),
            Ok(Err(err)) => err.to_string(),
            Err(_) => format!("upstream connect timed out after 1500ms ({host}:{port})"),
        };
        // Block on the next client frame. If the client gives up
        // (TCP close / read error), exit — no more frames to surface.
        let frame_bytes = match client_framed.next().await {
            Some(Ok(bytes)) => bytes.freeze(),
            Some(Err(err)) => {
                warn!(
                    conn = conn_id.0,
                    error = %err,
                    "client read errored while upstream unreachable"
                );
                return None;
            }
            None => return None,
        };
        match build_proto_event(
            ProxyDirection::ClientToUpstream,
            conn_id,
            local_port,
            &frame_bytes,
            corr_map,
        ) {
            Ok(mut event) => {
                event.frame_error = Some(connect_err.clone());
                correlator.record_event(&event);
            }
            Err(err) => {
                warn!(
                    conn = conn_id.0,
                    error = %err,
                    "build_proto_event failed while upstream unreachable"
                );
            }
        }
        warn!(
            conn = conn_id.0,
            error = %connect_err,
            "upstream unreachable; surfaced client frame as error, retrying"
        );
    }
}

impl ProxyHandle {
    /// Bind the bootstrap listener, seed the broker map with
    /// `(upstream → bootstrap_local_port)`, spawn the bootstrap accept
    /// loop, and return.
    ///
    /// # Errors
    /// - `InvalidInput` if `config.upstream` doesn't parse as `host:port`.
    /// - Underlying `io::Error` if the listener bind fails.
    pub async fn start(
        config: ProxyConfig,
        correlator: Arc<ProtoCorrelator>,
        record_sink: RecordSink,
    ) -> io::Result<Self> {
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
            upstream_tls: config.upstream_tls.clone(),
            upstream_sasl: config.upstream_sasl.clone(),
            correlator,
            broker_map,
            topic_id_map: Arc::new(TopicIdMap::new()),
            record_sink,
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

    /// Snapshot of proxy runtime for the `SidePanel` summary and the
    /// `kapture_proxy_status` MCP tool: bootstrap listen address,
    /// configured upstream, count of currently-active connection
    /// pumps, and the `(upstream_host:port, local_port)` mapping
    /// inferred from observed Metadata responses.
    #[must_use]
    pub fn summary(&self) -> ProxySummary {
        let active_connections = self.0.active_pumps.lock().len();
        let mut broker_mappings: Vec<((String, u16), u16)> = self.0.broker_map.snapshot();
        // Stable order (sort by local port) so the UI doesn't reshuffle
        // the list every poll.
        broker_mappings.sort_by_key(|((_, _), local)| *local);
        ProxySummary {
            listen_addr: self.0.bootstrap_addr.to_string(),
            upstream: self.0.bootstrap_upstream.clone(),
            active_connections,
            broker_mappings,
        }
    }

    /// Shared `TopicIdMap`. Examples (`proxy_smoke`) and tests use this
    /// to assert that the proxy observed Metadata responses and can
    /// resolve `topic_id → name` for Fetch v13+ records.
    #[allow(dead_code)]
    #[must_use]
    pub fn topic_id_map(&self) -> Arc<TopicIdMap> {
        Arc::clone(&self.0.topic_id_map)
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
        let no_op_sink: RecordSink = Arc::new(|_msg| {});
        let handle = ProxyHandle::start(cfg, Arc::clone(&correlator), no_op_sink)
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

    /// `summary()` exposes the bootstrap addr/upstream and observes a
    /// freshly-started proxy as having one broker mapping (the
    /// bootstrap) and zero active connection pumps.
    #[tokio::test]
    async fn proxy_handle_summary_reports_bootstrap_state() {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        // Hold the upstream listener so its port is taken — the proxy
        // doesn't connect until a client speaks, which we don't do here.
        drop(tokio::spawn(async move {
            let _ = upstream.accept().await;
        }));

        let correlator = Arc::new(ProtoCorrelator::new());
        let cfg = ProxyConfig::new(upstream_addr.to_string(), 0);
        let no_op_sink: RecordSink = Arc::new(|_msg| {});
        let handle = ProxyHandle::start(cfg, correlator, no_op_sink)
            .await
            .unwrap();

        let summary = handle.summary();
        assert_eq!(summary.upstream, upstream_addr.to_string());
        assert_eq!(summary.listen_addr, handle.local_addr().to_string());
        assert_eq!(summary.active_connections, 0);
        assert_eq!(summary.broker_mappings.len(), 1);

        handle.stop().await;
    }

    /// With `upstream_sasl` set on `ProxyConfig`, the bootstrap accept
    /// loop must route the upstream connect through `open_upstream` —
    /// observed indirectly by the fake broker receiving an
    /// `ApiVersions` request as the FIRST frame on the upstream TCP
    /// connection (the SASL handshake's first leg), before any
    /// client-driven traffic flows. If the accept loop still went
    /// straight to `TcpStream::connect`, the first bytes seen by the
    /// fake broker would be the client's request, not the SASL
    /// pre-roll.
    #[tokio::test]
    async fn proxy_handle_connects_to_upstream_via_open_upstream() {
        use crate::proxy::{UpstreamSaslConfig, UpstreamSaslMechanism};
        use crate::proxy_upstream::test_support::{
            build_api_versions_response, build_sasl_authenticate_response,
            build_sasl_handshake_response, decode_request_header, server_read_frame,
            server_write_frame,
        };
        use kafka_protocol::messages::ApiKey;

        // Fake broker that drives the SASL handshake and then idles.
        // If `open_upstream` was bypassed the first frame would not be
        // ApiVersions and `decode_request_header` would assert-fail.
        let bootstrap = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bootstrap_addr = bootstrap.local_addr().unwrap();
        let server_observed = Arc::new(parking_lot::Mutex::new(false));
        let server_observed_clone = Arc::clone(&server_observed);
        tokio::spawn(async move {
            let (mut sock, _) = bootstrap.accept().await.unwrap();
            // Frame 1: ApiVersions request from open_upstream.
            let f1 = server_read_frame(&mut sock).await.unwrap();
            let (h1, _) = decode_request_header(&f1, ApiKey::ApiVersions, 0);
            *server_observed_clone.lock() = true;
            server_write_frame(&mut sock, &build_api_versions_response(h1.correlation_id))
                .await
                .unwrap();
            // Frame 2: SaslHandshake.
            let f2 = server_read_frame(&mut sock).await.unwrap();
            let (h2, _) = decode_request_header(&f2, ApiKey::SaslHandshake, 1);
            server_write_frame(
                &mut sock,
                &build_sasl_handshake_response(h2.correlation_id, 0),
            )
            .await
            .unwrap();
            // Frame 3: SaslAuthenticate.
            let f3 = server_read_frame(&mut sock).await.unwrap();
            let (h3, _) = decode_request_header(&f3, ApiKey::SaslAuthenticate, 2);
            server_write_frame(
                &mut sock,
                &build_sasl_authenticate_response(h3.correlation_id, 0),
            )
            .await
            .unwrap();
            // Linger so the proxy's pump task can finish wiring up.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        });

        let correlator = Arc::new(ProtoCorrelator::new());
        let cfg = ProxyConfig::new(bootstrap_addr.to_string(), 0).with_sasl(UpstreamSaslConfig {
            mechanism: UpstreamSaslMechanism::Plain,
            username: "alice".to_owned(),
            password: "s3cret".to_owned(),
        });
        let no_op_sink: RecordSink = Arc::new(|_msg| {});
        let handle = ProxyHandle::start(cfg, correlator, no_op_sink)
            .await
            .unwrap();
        let listen_addr = handle.local_addr();

        // Drive a client into the proxy so the bootstrap accept loop
        // actually connects upstream.
        let mut client = tokio::net::TcpStream::connect(listen_addr).await.unwrap();
        // Send a length-prefixed dummy frame; we don't expect a useful
        // reply because the fake server doesn't pump traffic after
        // the SASL exchange.
        AsyncWriteExt::write_all(&mut client, &4u32.to_be_bytes())
            .await
            .unwrap();
        AsyncWriteExt::write_all(&mut client, &[0u8, 0, 0, 0])
            .await
            .unwrap();
        // Allow the upstream handshake to finish.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        assert!(
            *server_observed.lock(),
            "fake broker did not observe ApiVersions handshake → open_upstream was not used",
        );

        handle.stop().await;
    }
}
