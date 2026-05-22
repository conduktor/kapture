//! JVM tap mode — UDS listener that consumes plaintext Kafka wire
//! bytes from the Kapture JVM agent and feeds them through the same
//! `ProtoCorrelator` pipeline the proxy uses.
//!
//! Wire contract with `agents/jvm-tap` (`TapPublisher.java`):
//!
//! ```text
//!   u8   direction      (0 = outgoing/write, 1 = incoming/read)
//!   u64  nanos_since    (System.nanoTime() — monotonic, not wall-clock)
//!   u32  connection_id  (per agent process; not globally unique)
//!   u32  payload_len
//!   ...  payload bytes
//! ```
//!
//! All fields little-endian — matches `ByteBuffer.LITTLE_ENDIAN` in
//! `TapPublisher`. The payload is a slice of plaintext bytes from the
//! Java client's `SslTransportLayer.read/write` boundary; it is *not*
//! aligned with Kafka frame boundaries (one SSL record can carry a
//! fragment of a Kafka frame, and one Kafka frame can be split across
//! multiple SSL records). This module reassembles per `(uds-connection,
//! agent-connection-id, direction)` and emits `ProtoEvent`s when full
//! Kafka frames are available.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use tokio::io::AsyncReadExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::correlator::ProtoCorrelator;
use crate::proxy::{build_proto_event, ConnectionId, CorrelationMap, ProxyDirection};

/// Maximum bytes we are willing to hold per `(connection, direction)`
/// reassembly buffer. A well-behaved client never has more than one
/// in-flight Kafka frame's worth of unread bytes; this cap exists to
/// drop malformed streams instead of `OOM`-ing the inspector.
const MAX_REASSEMBLY_BUFFER: usize = 8 * 1024 * 1024;

/// Header on every UDS frame from the agent. See module-level docs.
const FRAME_HEADER_LEN: usize = 1 + 8 + 4 + 4;

/// Per-Kafka-frame length cap. Defensive: matches the proxy's
/// `PROTO_PAYLOAD_CAP` order of magnitude. Anything bigger than this
/// in a single Kafka length prefix is treated as a desync and the
/// connection is closed.
const MAX_KAFKA_FRAME_LEN: usize = 16 * 1024 * 1024;

/// Configuration for starting a JVM tap listener.
#[derive(Debug, Clone)]
pub struct JvmTapConfig {
    /// Unix domain socket path the JVM agent will connect to. The
    /// agent looks up this path through its
    /// `-Dkapture.tap.socket=...` system property (defaults to
    /// `/tmp/kapture-tap.sock` in the agent for parity with the
    /// experiment baseline).
    pub socket_path: PathBuf,
}

impl JvmTapConfig {
    #[must_use]
    pub fn new<P: Into<PathBuf>>(socket_path: P) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }
}

/// Active JVM tap session. Hold this in `AppState` for the lifetime of
/// the capture; drop / `stop()` to tear down the listener and all
/// active per-agent reader tasks.
#[derive(Debug)]
pub struct JvmTapHandle {
    socket_path: PathBuf,
    listener_task: JoinHandle<()>,
    stop: Arc<Notify>,
}

impl JvmTapHandle {
    /// Start a UDS listener at `config.socket_path` and feed every
    /// captured Kafka frame into `correlator`.
    ///
    /// If a stale socket file already exists at the path, it is
    /// removed first — this matches the proxy's "free the port if a
    /// previous run left it bound" behaviour. Any other I/O failure
    /// (permission denied, parent dir missing) bubbles up to the
    /// caller without starting the listener.
    ///
    /// # Errors
    /// Returns the underlying `io::Error` if the stale-socket removal
    /// fails with anything other than `NotFound`, or if `bind` fails
    /// (path collision with a non-socket file, missing parent
    /// directory, permission denied).
    pub async fn start(config: JvmTapConfig, correlator: Arc<ProtoCorrelator>) -> io::Result<Self> {
        // Clean up a stale socket if present. We tolerate ENOENT but
        // not other errors — a permission failure here would silently
        // re-fail at `bind` time with a less helpful message.
        match tokio::fs::remove_file(&config.socket_path).await {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }

        let listener = UnixListener::bind(&config.socket_path)?;
        info!(path = %config.socket_path.display(), "jvm-tap listening");

        let stop = Arc::new(Notify::new());
        let stop_for_task = Arc::clone(&stop);
        let socket_path_for_task = config.socket_path.clone();

        let listener_task = tokio::spawn(async move {
            run_listener(listener, correlator, stop_for_task, socket_path_for_task).await;
        });

        Ok(Self {
            socket_path: config.socket_path,
            listener_task,
            stop,
        })
    }

    /// Path the listener is bound to. Useful for tests that need to
    /// hand this socket to a child JVM via `-Dkapture.tap.socket=...`.
    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Stop the listener and await the per-agent reader tasks. Safe to
    /// call once; subsequent calls would no-op but `stop` consumes
    /// `self`, so the type system enforces that.
    pub async fn stop(self) {
        self.stop.notify_waiters();
        // Best-effort join. If the listener task panicked we still
        // want to clean up the socket file below.
        let _ = self.listener_task.await;
        if let Err(err) = tokio::fs::remove_file(&self.socket_path).await {
            if err.kind() != io::ErrorKind::NotFound {
                warn!(
                    path = %self.socket_path.display(),
                    error = %err,
                    "jvm-tap: failed to remove socket file on shutdown"
                );
            }
        }
    }
}

async fn run_listener(
    listener: UnixListener,
    correlator: Arc<ProtoCorrelator>,
    stop: Arc<Notify>,
    socket_path: PathBuf,
) {
    // Monotonic id stamped on every Kafka frame this session emits.
    // Each new agent connection gets its own base so that two agents
    // running against the same Kapture do not collide their per-agent
    // `connection_id` namespaces.
    let next_session_id = Arc::new(AtomicU64::new(1));

    loop {
        tokio::select! {
            biased;
            () = stop.notified() => {
                debug!(path = %socket_path.display(), "jvm-tap listener stopping");
                return;
            }
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _addr)) => {
                        let session_id = next_session_id.fetch_add(1, Ordering::Relaxed);
                        let correlator = Arc::clone(&correlator);
                        let stop = Arc::clone(&stop);
                        tokio::spawn(async move {
                            if let Err(err) = run_agent_session(stream, correlator, session_id, stop).await {
                                debug!(session_id, error = %err, "jvm-tap agent session ended");
                            }
                        });
                    }
                    Err(err) => {
                        // EBADF / closed listener — bail out. Anything
                        // else (transient EAGAIN) is logged and the
                        // loop continues.
                        if err.kind() == io::ErrorKind::Other {
                            warn!(error = %err, "jvm-tap: listener accept failed, stopping");
                            return;
                        }
                        warn!(error = %err, "jvm-tap: transient accept error");
                    }
                }
            }
        }
    }
}

/// Per-`(session_id, agent_conn_id, direction)` reassembly buffer +
/// per-`(session_id, agent_conn_id)` correlation map. Lives for the
/// length of one UDS connection from a JVM agent.
struct AgentSession {
    session_id: u64,
    /// Reassembly buffer keyed by `(agent_conn_id, direction)`. Each
    /// entry holds bytes received but not yet split into a full Kafka
    /// frame.
    buffers: HashMap<(u32, u8), BytesMut>,
    /// Per `(agent_conn_id)` Kafka correlation map — same role as the
    /// proxy's per-TCP-connection `CorrelationMap`: pair request
    /// `corr_id` with response `corr_id` to compute RTT.
    corr_maps: HashMap<u32, Arc<CorrelationMap>>,
    /// Stable `ConnectionId` per `agent_conn_id`. The agent's u32 id
    /// is only unique within one agent process; we combine
    /// `(session_id, agent_conn_id)` into a 64-bit composite so two
    /// agents speaking the same `conn_id` don't collide on the
    /// inspector side.
    conn_ids: HashMap<u32, ConnectionId>,
}

impl AgentSession {
    fn new(session_id: u64) -> Self {
        Self {
            session_id,
            buffers: HashMap::new(),
            corr_maps: HashMap::new(),
            conn_ids: HashMap::new(),
        }
    }

    fn conn_id_for(&mut self, agent_conn_id: u32) -> ConnectionId {
        *self.conn_ids.entry(agent_conn_id).or_insert_with(|| {
            // Compose 64-bit id: high 32 bits = session, low 32 bits =
            // agent_conn_id. `build_proto_event` masks to the
            // positive i32 range internally; the masking still yields
            // distinct ids for distinct `(session, agent_conn)` pairs
            // within one session because session_id starts at 1 and
            // increments — collisions would require 2^31 sessions.
            ConnectionId((self.session_id << 32) | u64::from(agent_conn_id))
        })
    }

    fn corr_map_for(&mut self, agent_conn_id: u32) -> Arc<CorrelationMap> {
        Arc::clone(
            self.corr_maps
                .entry(agent_conn_id)
                .or_insert_with(|| Arc::new(CorrelationMap::default())),
        )
    }
}

async fn run_agent_session(
    mut stream: UnixStream,
    correlator: Arc<ProtoCorrelator>,
    session_id: u64,
    stop: Arc<Notify>,
) -> io::Result<()> {
    debug!(session_id, "jvm-tap: agent connected");
    let mut session = AgentSession::new(session_id);
    let mut header_buf = [0u8; FRAME_HEADER_LEN];

    loop {
        tokio::select! {
            biased;
            () = stop.notified() => {
                debug!(session_id, "jvm-tap: stop signal — closing agent session");
                return Ok(());
            }
            read = stream.read_exact(&mut header_buf) => {
                match read {
                    Ok(_) => {}
                    Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                        debug!(session_id, "jvm-tap: agent disconnected");
                        return Ok(());
                    }
                    Err(err) => return Err(err),
                }
            }
        }

        let direction = header_buf[0];
        let payload_len = u32::from_le_bytes([
            header_buf[13],
            header_buf[14],
            header_buf[15],
            header_buf[16],
        ]) as usize;
        let agent_conn_id = u32::from_le_bytes([
            header_buf[9],
            header_buf[10],
            header_buf[11],
            header_buf[12],
        ]);

        if payload_len == 0 {
            // Empty frame: skip without touching the reassembly state.
            // The agent should not emit these, but tolerating it costs
            // nothing.
            continue;
        }
        if payload_len > MAX_KAFKA_FRAME_LEN {
            warn!(
                session_id,
                agent_conn_id, payload_len, "jvm-tap: payload too large, dropping connection"
            );
            return Ok(());
        }

        let mut payload = vec![0u8; payload_len];
        stream.read_exact(&mut payload).await?;

        if let Err(err) = process_payload(
            &mut session,
            &correlator,
            agent_conn_id,
            direction,
            &payload,
        ) {
            warn!(
                session_id,
                agent_conn_id,
                error = %err,
                "jvm-tap: stream parse error, closing agent"
            );
            return Ok(());
        }
    }
}

fn process_payload(
    session: &mut AgentSession,
    correlator: &Arc<ProtoCorrelator>,
    agent_conn_id: u32,
    direction: u8,
    payload: &[u8],
) -> io::Result<()> {
    let proxy_dir = match direction {
        0 => ProxyDirection::ClientToUpstream,
        1 => ProxyDirection::UpstreamToClient,
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("jvm-tap: unknown direction byte {other}"),
            ));
        }
    };

    let conn_id = session.conn_id_for(agent_conn_id);
    let corr_map = session.corr_map_for(agent_conn_id);

    let buf = session
        .buffers
        .entry((agent_conn_id, direction))
        .or_default();
    if buf.len().saturating_add(payload.len()) > MAX_REASSEMBLY_BUFFER {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "jvm-tap: reassembly buffer exceeded MAX_REASSEMBLY_BUFFER",
        ));
    }
    buf.extend_from_slice(payload);

    // Pull complete Kafka frames out of the reassembly buffer. Each
    // frame is a 4-byte BE length prefix followed by that many body
    // bytes. `build_proto_event` takes the body slice (no prefix)
    // and reassembles the wire payload internally for the inspector
    // copy.
    loop {
        if buf.len() < 4 {
            break;
        }
        let frame_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if frame_len > MAX_KAFKA_FRAME_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("jvm-tap: kafka frame length {frame_len} exceeds cap"),
            ));
        }
        if buf.len() < 4 + frame_len {
            break;
        }
        buf.advance(4);
        let body = buf.split_to(frame_len).freeze();
        // local_port = 0: the agent path is not behind a proxy
        // listener. The `ProtoEvent` docstring carries the same
        // defensive default for non-proxy sources.
        let event = build_proto_event(proxy_dir, conn_id, 0, &body, &corr_map)?;
        correlator.record_event(&event);
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    /// Write one agent-format frame (header + payload) to `stream`.
    async fn write_agent_frame(
        stream: &mut UnixStream,
        direction: u8,
        agent_conn_id: u32,
        payload: &[u8],
    ) -> io::Result<()> {
        let mut header = Vec::with_capacity(FRAME_HEADER_LEN);
        header.push(direction);
        header.extend_from_slice(&0u64.to_le_bytes()); // nanos — ignored by the listener
        header.extend_from_slice(&agent_conn_id.to_le_bytes());
        let len = u32::try_from(payload.len()).unwrap();
        header.extend_from_slice(&len.to_le_bytes());
        stream.write_all(&header).await?;
        stream.write_all(payload).await
    }

    /// Build a minimal Kafka request frame: 4-byte BE length prefix
    /// followed by the smallest valid request header
    /// `(api_key=18 ApiVersions, api_version=3, corr_id, client_id="")`.
    fn make_api_versions_request_frame(corr_id: i32) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&18i16.to_be_bytes()); // api_key
        body.extend_from_slice(&3i16.to_be_bytes()); // api_version
        body.extend_from_slice(&corr_id.to_be_bytes());
        body.extend_from_slice(&(-1i16).to_be_bytes()); // client_id length = -1 (nullable)
        let mut frame = Vec::with_capacity(4 + body.len());
        let body_len = u32::try_from(body.len()).unwrap();
        frame.extend_from_slice(&body_len.to_be_bytes());
        frame.extend_from_slice(&body);
        frame
    }

    async fn fresh_tap() -> (JvmTapHandle, Arc<ProtoCorrelator>, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jvm-tap.sock");
        std::mem::forget(dir); // keep the temp dir alive for the test
        let correlator = Arc::new(ProtoCorrelator::new());
        let handle = JvmTapHandle::start(JvmTapConfig::new(path.clone()), Arc::clone(&correlator))
            .await
            .unwrap();
        (handle, correlator, path)
    }

    #[tokio::test]
    async fn complete_frame_in_single_payload_is_decoded() {
        let (handle, correlator, path) = fresh_tap().await;
        let mut stream = UnixStream::connect(&path).await.unwrap();

        let frame = make_api_versions_request_frame(42);
        write_agent_frame(&mut stream, 0, 1, &frame).await.unwrap();

        // Give the listener task a moment to drain the bytes.
        for _ in 0..50 {
            if correlator.frame_count() >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(correlator.frame_count(), 1);
        let summaries = correlator.summaries(10);
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].api_name, "ApiVersionsRequest");
        assert_eq!(summaries[0].corr_id, 42);

        drop(stream);
        handle.stop().await;
    }

    #[tokio::test]
    async fn kafka_frame_split_across_two_ssl_writes_is_reassembled() {
        let (handle, correlator, path) = fresh_tap().await;
        let mut stream = UnixStream::connect(&path).await.unwrap();

        let frame = make_api_versions_request_frame(7);
        let split_at = frame.len() / 2;
        write_agent_frame(&mut stream, 0, 5, &frame[..split_at])
            .await
            .unwrap();
        // No frame should be produced yet — only half the bytes.
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(correlator.frame_count(), 0);

        write_agent_frame(&mut stream, 0, 5, &frame[split_at..])
            .await
            .unwrap();
        for _ in 0..50 {
            if correlator.frame_count() >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(correlator.frame_count(), 1);
        assert_eq!(correlator.summaries(10)[0].corr_id, 7);

        drop(stream);
        handle.stop().await;
    }

    #[tokio::test]
    async fn two_kafka_frames_concatenated_in_one_payload_both_decode() {
        let (handle, correlator, path) = fresh_tap().await;
        let mut stream = UnixStream::connect(&path).await.unwrap();

        let mut payload = Vec::new();
        payload.extend_from_slice(&make_api_versions_request_frame(100));
        payload.extend_from_slice(&make_api_versions_request_frame(101));
        write_agent_frame(&mut stream, 0, 9, &payload)
            .await
            .unwrap();

        for _ in 0..50 {
            if correlator.frame_count() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(correlator.frame_count(), 2);
        let summaries = correlator.summaries(10);
        let ids: Vec<i32> = summaries.iter().map(|s| s.corr_id).collect();
        assert!(ids.contains(&100));
        assert!(ids.contains(&101));

        drop(stream);
        handle.stop().await;
    }

    #[tokio::test]
    async fn malformed_direction_byte_closes_session_without_panic() {
        let (handle, correlator, path) = fresh_tap().await;
        let mut stream = UnixStream::connect(&path).await.unwrap();

        write_agent_frame(&mut stream, 99, 1, b"garbage")
            .await
            .unwrap();
        // Give the listener time to reject and close.
        tokio::time::sleep(Duration::from_millis(80)).await;
        // Sending more bytes on the now-half-closed stream is OK from
        // our side; the listener has already returned.
        assert_eq!(correlator.frame_count(), 0);

        drop(stream);
        handle.stop().await;
    }
}
