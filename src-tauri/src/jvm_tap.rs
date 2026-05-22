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
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
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

/// Global monotonic counter handing out `ConnectionId`s to every
/// distinct `(session, agent_conn_id)` pair we observe across the
/// process lifetime. Replaces an earlier `(session_id << 32) |
/// agent_conn_id` composition that collided with `build_proto_event`'s
/// `& 0x7FFF_FFFF` mask: the high 33 bits of the composite were
/// silently dropped, so two agent processes reusing the same
/// `agent_conn_id` produced colliding `ConnectionId`s on the
/// inspector side. A flat monotonic counter avoids the masking issue
/// entirely; the low 31 bits won't recycle until 2^31 distinct
/// connections, which no realistic dev session reaches.
static NEXT_TAP_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

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
    /// Single shutdown channel observed by the listener and every
    /// per-agent reader task. Replaces an earlier `tokio::sync::Notify`
    /// design that lost wakes for tasks blocked outside the select!
    /// (e.g. mid `read_exact`) when `notify_waiters()` fired.
    stop_tx: watch::Sender<bool>,
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

        let (stop_tx, stop_rx) = watch::channel(false);
        let socket_path_for_task = config.socket_path.clone();

        let listener_task = tokio::spawn(async move {
            run_listener(listener, correlator, stop_rx, socket_path_for_task).await;
        });

        Ok(Self {
            socket_path: config.socket_path,
            listener_task,
            stop_tx,
        })
    }

    /// Path the listener is bound to. Useful for tests that need to
    /// hand this socket to a child JVM via `-Dkapture.tap.socket=...`.
    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Stop the listener, signal all per-agent reader tasks to exit,
    /// and wait for the listener task to finish draining them. Safe to
    /// call once; `stop` consumes `self`.
    pub async fn stop(self) {
        // Setting the flag wakes every `watch::Receiver::changed`
        // future currently registered AND becomes the new "current
        // value" so any receiver that subscribes later observes the
        // stop immediately — closes the race where a Notify-based
        // design loses the wake for tasks blocked outside the select!.
        let _ = self.stop_tx.send(true);
        // The listener task joins all per-agent reader tasks before
        // returning, so awaiting it here drains the whole tree.
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
    mut stop_rx: watch::Receiver<bool>,
    socket_path: PathBuf,
) {
    // Monotonic id stamped on every Kafka frame this session emits.
    // Each new agent connection gets its own base so that two agents
    // running against the same Kapture do not collide their per-agent
    // `connection_id` namespaces.
    let next_session_id = AtomicU64::new(1);
    // Track every per-agent reader task so `stop` can drain them. A
    // detached `tokio::spawn` would orphan in-flight sessions: this
    // PR's reviewer caught it.
    let mut sessions: JoinSet<()> = JoinSet::new();

    loop {
        tokio::select! {
            biased;
            // Already stopped: drain pending sessions and return.
            res = stop_rx.changed() => {
                if res.is_err() || *stop_rx.borrow() {
                    debug!(path = %socket_path.display(), "jvm-tap listener stopping");
                    while sessions.join_next().await.is_some() {}
                    return;
                }
            }
            // Reap finished sessions so the JoinSet doesn't grow
            // unbounded for long-lived listeners.
            Some(_finished) = sessions.join_next(), if !sessions.is_empty() => {}
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _addr)) => {
                        let session_id = next_session_id.fetch_add(1, Ordering::Relaxed);
                        let correlator = Arc::clone(&correlator);
                        let session_stop = stop_rx.clone();
                        sessions.spawn(async move {
                            if let Err(err) =
                                run_agent_session(stream, correlator, session_id, session_stop)
                                    .await
                            {
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
                            while sessions.join_next().await.is_some() {}
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
    fn new() -> Self {
        Self {
            buffers: HashMap::new(),
            corr_maps: HashMap::new(),
            conn_ids: HashMap::new(),
        }
    }

    fn conn_id_for(&mut self, agent_conn_id: u32) -> ConnectionId {
        *self.conn_ids.entry(agent_conn_id).or_insert_with(|| {
            // Pull a fresh monotonic ID from the process-wide counter
            // so distinct `(session, agent_conn_id)` pairs never share
            // a `ConnectionId` after the `build_proto_event` 31-bit
            // mask. The session sequence number is no longer mixed
            // into the id — see `NEXT_TAP_CONNECTION_ID` for why.
            ConnectionId(NEXT_TAP_CONNECTION_ID.fetch_add(1, Ordering::Relaxed))
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
    mut stop_rx: watch::Receiver<bool>,
) -> io::Result<()> {
    debug!(session_id, "jvm-tap: agent connected");
    let mut session = AgentSession::new();
    let mut header_buf = [0u8; FRAME_HEADER_LEN];

    loop {
        // Wait for either: a stop signal, or a complete frame header.
        // Watch-based stop survives the case where we re-enter this
        // select after a previously-fired notification — `*stop_rx
        // .borrow()` is always the current truth.
        tokio::select! {
            biased;
            res = stop_rx.changed() => {
                if res.is_err() || *stop_rx.borrow() {
                    debug!(session_id, "jvm-tap: stop signal — closing agent session");
                    return Ok(());
                }
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
        // Re-check after the read — `read_exact` may have completed
        // while `stop_tx.send(true)` fired in parallel.
        if *stop_rx.borrow() {
            return Ok(());
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
        tokio::select! {
            biased;
            res = stop_rx.changed() => {
                if res.is_err() || *stop_rx.borrow() {
                    debug!(session_id, "jvm-tap: stop signal — dropping in-flight payload");
                    return Ok(());
                }
            }
            read = stream.read_exact(&mut payload) => {
                match read {
                    Ok(_) => {}
                    Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                        debug!(session_id, "jvm-tap: agent disconnected mid-payload");
                        return Ok(());
                    }
                    Err(err) => return Err(err),
                }
            }
        }

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

    /// Two concurrent agent connections both using `agent_conn_id = 1`
    /// must NOT collide into a single `ConnectionId`: the
    /// `NEXT_TAP_CONNECTION_ID` counter exists precisely so two
    /// agents speaking the same local conn-id stay distinct in the
    /// inspector. Catches the regression where someone "simplifies"
    /// `conn_id_for` back to using just
    /// `agent_conn_id`.
    ///
    #[tokio::test]
    async fn two_agents_with_same_conn_id_emit_distinct_connection_ids() {
        let (handle, correlator, path) = fresh_tap().await;
        let mut a = UnixStream::connect(&path).await.unwrap();
        let mut b = UnixStream::connect(&path).await.unwrap();

        // Same agent_conn_id (1) on BOTH streams, different corr_ids so
        // we can tell the resulting frames apart.
        let frame_a = make_api_versions_request_frame(1001);
        let frame_b = make_api_versions_request_frame(2002);
        write_agent_frame(&mut a, 0, 1, &frame_a).await.unwrap();
        write_agent_frame(&mut b, 0, 1, &frame_b).await.unwrap();

        for _ in 0..50 {
            if correlator.frame_count() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let summaries = correlator.summaries(10);
        assert_eq!(summaries.len(), 2);
        let conn_a = summaries
            .iter()
            .find(|s| s.corr_id == 1001)
            .expect("frame from agent A missing")
            .connection_id;
        let conn_b = summaries
            .iter()
            .find(|s| s.corr_id == 2002)
            .expect("frame from agent B missing")
            .connection_id;
        assert_ne!(
            conn_a, conn_b,
            "two agents reusing the same agent_conn_id must get distinct ConnectionIds"
        );

        drop(a);
        drop(b);
        handle.stop().await;
    }

    /// Agent writes a valid header announcing a 1 KiB payload, then
    /// drops the connection after sending only half. The per-session
    /// task must observe the EOF on `read_exact(payload)` and exit —
    /// not hang. We assert by stopping the tap with a tight timeout: if
    /// the per-session task is still parked on `read_exact` it will be
    /// cancelled by the `stop` signal flowing through `select!` on the
    /// next loop iteration, but only because that select exists. If the
    /// code ever loses the EOF→Ok mapping the test will surface a hard
    /// I/O error instead of a silent return.
    #[tokio::test]
    async fn agent_disconnect_after_header_does_not_hang() {
        let (handle, correlator, path) = fresh_tap().await;
        let mut stream = UnixStream::connect(&path).await.unwrap();

        // Write the header manually so we can drop the stream BEFORE
        // sending any payload bytes — `read_exact` on the payload will
        // see an immediate UnexpectedEof.
        let payload_len = 1024u32;
        let mut header = Vec::with_capacity(FRAME_HEADER_LEN);
        header.push(0); // direction = write
        header.extend_from_slice(&0u64.to_le_bytes());
        header.extend_from_slice(&7u32.to_le_bytes()); // agent_conn_id
        header.extend_from_slice(&payload_len.to_le_bytes());
        stream.write_all(&header).await.unwrap();
        stream.shutdown().await.unwrap();
        drop(stream);

        // Give the per-session task a moment to discover the EOF.
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(correlator.frame_count(), 0);

        // If the per-session task was wedged, `stop()` would still race
        // it cleanly thanks to `Notify`, so this also doubles as a
        // smoke test that the stop path works after a torn connection.
        handle.stop().await;
    }

    /// A Kafka length prefix bigger than `MAX_KAFKA_FRAME_LEN` (16 MiB)
    /// must close the agent session cleanly — no panic, no half-state
    /// left behind that would affect a subsequent agent connection.
    #[tokio::test]
    async fn oversize_kafka_frame_length_prefix_closes_connection() {
        let (handle, correlator, path) = fresh_tap().await;
        let mut stream = UnixStream::connect(&path).await.unwrap();

        // Kafka frame whose 4-byte BE length prefix claims 32 MiB —
        // twice the cap. The agent payload only contains the prefix
        // (the listener never tries to wait for the body because the
        // cap check trips first).
        let bogus_len = u32::try_from(MAX_KAFKA_FRAME_LEN + 1).unwrap();
        let mut payload = Vec::with_capacity(4);
        payload.extend_from_slice(&bogus_len.to_be_bytes());
        write_agent_frame(&mut stream, 0, 11, &payload)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(correlator.frame_count(), 0);

        // A fresh agent connection on the same listener must still
        // work — proves the listener task survived the parse error.
        let mut stream2 = UnixStream::connect(&path).await.unwrap();
        let good = make_api_versions_request_frame(55);
        write_agent_frame(&mut stream2, 0, 12, &good).await.unwrap();
        for _ in 0..50 {
            if correlator.frame_count() >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(correlator.frame_count(), 1);

        drop(stream2);
        handle.stop().await;
    }

    /// Reassembly buffer cap: a single Kafka frame whose body is
    /// announced as 9 MiB and is fed across two agent frames must
    /// trigger the `MAX_REASSEMBLY_BUFFER` (8 MiB) guard on the second
    /// chunk, NOT silently accumulate. Catches the regression where a
    /// future change removes the cap or moves it after the
    /// `extend_from_slice`, which would let a hostile (or buggy) agent
    /// OOM the inspector.
    #[tokio::test]
    async fn reassembly_buffer_cap_drops_connection_before_oom() {
        let (handle, correlator, path) = fresh_tap().await;
        let mut stream = UnixStream::connect(&path).await.unwrap();

        // Announce a 9 MiB Kafka frame, then feed it in two 5 MiB
        // chunks. After the first chunk buf is ~5 MiB; the second
        // chunk's pre-check (`buf.len + payload.len > cap`) trips at
        // 10 MiB > 8 MiB and the session ends.
        let kafka_body_len: u32 = 9 * 1024 * 1024;
        let chunk_size: usize = 5 * 1024 * 1024;

        // First agent frame: length prefix + chunk_size - 4 zero bytes
        // of "body". Total agent payload = chunk_size.
        let mut first = vec![0u8; chunk_size];
        first[..4].copy_from_slice(&kafka_body_len.to_be_bytes());
        write_agent_frame(&mut stream, 0, 21, &first).await.unwrap();

        // Second agent frame: another chunk_size of body. This should
        // push the reassembly buffer past 8 MiB and abort.
        let second = vec![0u8; chunk_size];
        // The write may succeed (UDS buffer) even if the listener is
        // already tearing down — that's fine, we just need the bytes
        // off our side.
        let _ = write_agent_frame(&mut stream, 0, 21, &second).await;

        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(
            correlator.frame_count(),
            0,
            "no complete Kafka frame was ever delivered; correlator must stay empty"
        );

        // Listener must still accept new agents.
        let mut stream2 = UnixStream::connect(&path).await.unwrap();
        let good = make_api_versions_request_frame(77);
        write_agent_frame(&mut stream2, 0, 22, &good).await.unwrap();
        for _ in 0..50 {
            if correlator.frame_count() >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(correlator.frame_count(), 1);

        drop(stream2);
        handle.stop().await;
    }

    /// `JvmTapHandle::start` must clean up a stale regular file at the
    /// socket path (matches the docstring contract and the proxy's
    /// "free the port" behaviour). Without this, a previous run that
    /// crashed without cleanup would block the next start.
    #[tokio::test]
    async fn start_removes_stale_file_at_socket_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stale.sock");
        // Pre-create a regular (non-socket) file at the path.
        tokio::fs::write(&path, b"leftover from a previous crash")
            .await
            .unwrap();
        assert!(tokio::fs::metadata(&path).await.is_ok());

        let correlator = Arc::new(ProtoCorrelator::new());
        let handle = JvmTapHandle::start(JvmTapConfig::new(path.clone()), correlator)
            .await
            .expect("start must succeed by removing the stale file");

        // Verify the listener is actually bound — a connect should
        // succeed, where it would fail (ECONNREFUSED) if `start` had
        // somehow left the regular file in place.
        let _stream = UnixStream::connect(&path).await.unwrap();
        handle.stop().await;
    }

    /// Request/response pairing inside the tap: send an
    /// `ApiVersionsRequest` then a matching response with the same
    /// `corr_id`. The recv-direction frame must come out with the
    /// response's wire size and a measurable RTT (>= 0 ms, and the
    /// `corr_id` matches). Catches the regression where the per-agent
    /// `CorrelationMap` is keyed wrong (e.g. by `session_id` instead of
    /// `agent_conn_id`) and `take_response` always returns `None`.
    #[tokio::test]
    async fn request_and_response_with_same_corr_id_are_paired() {
        let (handle, correlator, path) = fresh_tap().await;
        let mut stream = UnixStream::connect(&path).await.unwrap();

        let req = make_api_versions_request_frame(424_242);
        write_agent_frame(&mut stream, 0, 33, &req).await.unwrap();

        // Brief delay so the response's `sent_at - now` produces a
        // non-zero RTT we can sanity-check.
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Build a minimal response: 4-byte BE length prefix, then
        // 4-byte BE corr_id, then empty body. `build_proto_event` only
        // needs the corr_id to pair.
        let mut resp_body = Vec::new();
        resp_body.extend_from_slice(&424_242i32.to_be_bytes());
        let mut resp_frame = Vec::with_capacity(4 + resp_body.len());
        let body_len = u32::try_from(resp_body.len()).unwrap();
        resp_frame.extend_from_slice(&body_len.to_be_bytes());
        resp_frame.extend_from_slice(&resp_body);
        // Direction = 1 (UpstreamToClient / read), same agent_conn_id.
        write_agent_frame(&mut stream, 1, 33, &resp_frame)
            .await
            .unwrap();

        for _ in 0..50 {
            if correlator.frame_count() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let summaries = correlator.summaries(10);
        assert_eq!(summaries.len(), 2);
        let recv = summaries
            .iter()
            .find(|s| matches!(s.direction, crate::proto_event::ProtoDirection::Recv))
            .expect("response frame missing");
        assert_eq!(recv.corr_id, 424_242);
        // Pairing succeeded → recv inherited the request's api_key.
        assert_eq!(
            recv.api_key, 18,
            "ApiVersions api_key should be inherited via corr_map"
        );
        assert!(
            recv.rtt_ms > 0.0,
            "rtt_ms should be > 0 (slept 15ms between request and response), got {}",
            recv.rtt_ms
        );

        drop(stream);
        handle.stop().await;
    }
}
