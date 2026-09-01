//! JVM tap mode — UDS listener that consumes plaintext Kafka wire
//! bytes from the Kapture JVM agent and feeds them through the same
//! `ProtoCorrelator` pipeline the proxy uses.
//!
//! Wire contract with `agents/jvm-tap` (`TapPublisher.java`):
//!
//! ```text
//!   u8   direction      (0 = outgoing/write, 1 = incoming/read)
//!   u64  observed_nanos (System.nanoTime() at Kafka read/write advice)
//!   u64  emitted_nanos  (System.nanoTime() on the UDS writer thread)
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
use crate::proxy::{build_proto_event_at, ConnectionId, CorrelationMap, ProxyDirection};

/// Maximum bytes we are willing to hold per `(connection, direction)`
/// reassembly buffer. A well-behaved client never has more than one
/// in-flight Kafka frame's worth of unread bytes; this cap exists to
/// drop malformed streams instead of `OOM`-ing the inspector.
const MAX_REASSEMBLY_BUFFER: usize = 8 * 1024 * 1024;

/// Header on every UDS frame from the agent. See module-level docs.
const FRAME_HEADER_LEN: usize = 1 + 8 + 8 + 4 + 4;
const DIRECTION_HEALTH: u8 = 2;

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

/// Hard cap on distinct `agent_conn_id` values we track per UDS
/// session. The agent has no "this Kafka connection closed" signal,
/// so entries in `AgentSession::{buffers, corr_maps, conn_ids}`
/// accumulate for the life of the agent. Realistic JVMs have 1-10
/// broker connections lifetime; pathological apps (rebalance churn,
/// short-lived admin clients) can creep higher but 4096 leaves
/// plenty of headroom. Past the cap, new `agent_conn_id`s are
/// logged and dropped — chosen over LRU eviction because evicting
/// an entry with in-progress reassembly bytes would desync the
/// stream and corrupt subsequent frames.
const MAX_AGENT_CONN_IDS_PER_SESSION: usize = 4096;

/// When a reassembly buffer drains to empty AND its peak capacity
/// exceeded this threshold, swap it for a fresh `BytesMut`. The
/// `bytes` crate's `split_to(at).freeze()` advances the buffer's
/// internal pointer past `at` but keeps the same underlying
/// allocation alive — so a one-off 8 MiB frame keeps an 8 MiB
/// allocation around forever even though `BytesMut::capacity()`
/// reports a smaller number afterwards. Replacing with a fresh
/// `BytesMut::new()` once we know there are no in-flight bytes
/// (`buf.is_empty()`) reclaims the allocation in steady-state.
const REASSEMBLY_BUFFER_SHRINK_THRESHOLD: usize = 64 * 1024;

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
        // Lock the socket file to owner-only (mode 0600). UnixListener
        // creates the socket with default umask perms which on most
        // Linuxes is 0755 — any local user could connect and inject
        // forged Kafka frames into the inspector. This sets perms
        // immediately after bind; the TOCTOU window between bind and
        // chmod is tiny (no `await` in between) and an attacker would
        // need to be already polling the parent directory to exploit
        // it. macOS does not strictly enforce UDS file perms for
        // connect() on all kernels, but the chmod still provides
        // defense-in-depth and matches the user's expectation.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&config.socket_path, std::fs::Permissions::from_mode(0o600))?;
        }
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

#[allow(clippy::too_many_lines)]
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
        let observed_nanos = u64::from_le_bytes([
            header_buf[1],
            header_buf[2],
            header_buf[3],
            header_buf[4],
            header_buf[5],
            header_buf[6],
            header_buf[7],
            header_buf[8],
        ]);
        let emitted_nanos = u64::from_le_bytes([
            header_buf[9],
            header_buf[10],
            header_buf[11],
            header_buf[12],
            header_buf[13],
            header_buf[14],
            header_buf[15],
            header_buf[16],
        ]);
        let payload_len = u32::from_le_bytes([
            header_buf[21],
            header_buf[22],
            header_buf[23],
            header_buf[24],
        ]) as usize;
        let agent_conn_id = u32::from_le_bytes([
            header_buf[17],
            header_buf[18],
            header_buf[19],
            header_buf[20],
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

        if direction == DIRECTION_HEALTH {
            if payload.len() != 8 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "jvm-tap: health payload must be one u64",
                ));
            }
            let drops = u64::from_le_bytes(payload.as_slice().try_into().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "jvm-tap: invalid health payload",
                )
            })?);
            correlator.record_agent_drops(drops);
            continue;
        }

        let capture_lag_ms = emitted_nanos.saturating_sub(observed_nanos) as f64 / 1_000_000.0;

        if let Err(err) = process_payload(
            &mut session,
            &correlator,
            agent_conn_id,
            direction,
            observed_nanos,
            capture_lag_ms,
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
    observed_nanos: u64,
    capture_lag_ms: f64,
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

    // Hard cap on agent_conn_id breadth per UDS session. An agent
    // process with no close-signal can churn through new
    // agent_conn_ids on every leader move; left unbounded, the
    // per-session HashMaps would leak. Refuse the new conn beyond
    // the cap (do NOT LRU-evict — evicting an in-flight reassembly
    // buffer would desync subsequent frames). Existing entries are
    // unaffected.
    if !session.conn_ids.contains_key(&agent_conn_id)
        && session.conn_ids.len() >= MAX_AGENT_CONN_IDS_PER_SESSION
    {
        warn!(
            agent_conn_id,
            cap = MAX_AGENT_CONN_IDS_PER_SESSION,
            "jvm-tap: per-session connection cap reached, dropping frame"
        );
        return Ok(());
    }

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
        let event = build_proto_event_at(
            proxy_dir,
            conn_id,
            0,
            &body,
            &corr_map,
            (observed_nanos != 0).then_some(observed_nanos),
            capture_lag_ms,
        )?;
        correlator.enqueue_event(event);
    }

    // Reclaim the allocation when reassembly fully drains. `split_to`
    // advances the internal pointer past the consumed bytes but
    // keeps the original allocation alive — without this swap, a
    // single 8 MiB frame leaves an 8 MiB allocation pinned for the
    // life of the per-conn entry. The body Bytes goes out of scope
    // at the end of every iteration above, so by here the only
    // remaining ref is `buf` itself; replacing it drops the alloc.
    if buf.is_empty() && buf.capacity() > REASSEMBLY_BUFFER_SHRINK_THRESHOLD {
        *buf = BytesMut::new();
    }

    Ok(())
}

#[cfg(test)]
mod tests;
