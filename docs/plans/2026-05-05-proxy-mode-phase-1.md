# Proxy Mode — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a single-broker plain-TCP Kafka proxy. `kafkacat -b localhost:9092 -L` (or any client) points at Kapture, Kapture forwards to a real upstream broker, every frame in both directions appears in the Protocol tab.

**Architecture:** New `proxy` module owns a `tokio::net::TcpListener` and one async task per accepted client socket. Each task opens an upstream TCP socket and runs two `frame_splitter` halves driving `LengthDelimitedCodec`-style framing; on each frame it taps a copy to the existing `ProtoCorrelator` (which already drives the Protocol tab) before forwarding the bytes verbatim. Per-connection state holds a `corr_id → (api_key, api_version, sent_at)` map so we can decode responses with the right schema (responses don't carry api_key on the wire) and compute true RTT. Proxy mode and rdkafka client mode are mutually exclusive in `AppState` — exactly one capture surface is live at a time. No SASL, no TLS, single broker — the spec defers those to Phases 2-4.

**Tech Stack:** Rust + tokio (already a dep), `tokio_util::codec::LengthDelimitedCodec` (new), reuse `kafka-protocol` decoder, reuse `ProtoCorrelator` ring buffer + `ProtoFrame` shape, reuse the React Protocol tab.

**Out of scope for Phase 1:** multi-broker advertised.listeners rewrite (Phase 2), SASL pass-through (Phase 3), TLS (Phase 4), Messages tab record extraction (Phase 1.5 — done after the Protocol tab is proven with kafkacat).

---

## File Structure

| File                                  | Status | Responsibility                                                                                                                                                                                                |
| ------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src-tauri/src/proxy.rs`              | Create | Listener, per-connection pump, frame splitter, request-header parser, api-key correlation map, `ProxyHandle`, `ProxyConfig`. Calls into `ProtoCorrelator::record_event`.                                      |
| `src-tauri/src/proto_hook.rs`         | Modify | No code change — but `ProtoEvent` is reused by the proxy; document that the type is now also produced from the proxy path (not only librdkafka FFI).                                                          |
| `src-tauri/src/state.rs`              | Modify | Add `proxy: Option<ProxyHandle>` to `Inner`, with `install_proxy` / `take_proxy` / `is_proxying`. Mutually exclusive with `capture` (a `try_claim_*` for whichever surface).                                  |
| `src-tauri/src/lib.rs`                | Modify | Register `proxy` module + new Tauri commands.                                                                                                                                                                 |
| `src-tauri/src/commands.rs`           | Modify | Two new commands: `start_proxy(upstream, listen_port)`, `stop_proxy()`. Also: `connect` / `disconnect` already exist for client mode — left alone.                                                            |
| `src-tauri/src/error.rs`              | Modify | One new error variant: `ProxyError(String)` (or reuse `Config` if string-based).                                                                                                                              |
| `src/types.ts`                        | Modify | Add `ProxyConfig`, `ProxyState`, `ProxyStatus` shapes. Expose new commands' typed wrappers.                                                                                                                   |
| `src/components/ConnectionDialog.tsx` | Modify | Add a Mode toggle (radio: Client / Proxy). When Proxy is selected, the form collects `upstreamHost:port` + `listenPort` (default `127.0.0.1:9092`). Test-connection button still works (probes the upstream). |
| `src/components/TopBar.tsx`           | Modify | Cluster pill shows "proxy :9092 → upstream:9092" in proxy mode.                                                                                                                                               |
| `src/App.tsx`                         | Modify | Branch on the active mode for connect / disconnect dispatch.                                                                                                                                                  |

Files NOT touched in Phase 1:

- `capture.rs`, `proto_hook.rs` (FFI), the librdkafka build — proxy mode reuses the correlator; the rdkafka client path stays as a sub-mode.
- `mcp.rs` — proxy MCP surface comes in Phase 5 polish.
- `filter.rs`, `ring_buffer.rs`, `correlator.rs` — reused as-is.

---

## Task 1: Proxy module skeleton — types only

**Files:**

- Create: `src-tauri/src/proxy.rs`
- Modify: `src-tauri/src/lib.rs:1` (add `mod proxy;`)
- Test: `src-tauri/src/proxy.rs` (inline `#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing test**

```rust
// src-tauri/src/proxy.rs

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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd src-tauri && cargo test --lib proxy::tests::proxy_config_normalises_listen_addr 2>&1 | tail -20`
Expected: FAIL — `ProxyConfig` undefined.

- [ ] **Step 3: Write minimal implementation**

```rust
// src-tauri/src/proxy.rs

//! Kapture proxy mode.
//!
//! A TCP intermediary that accepts Kafka client connections, forwards
//! every byte to a real upstream broker, and taps each frame to the
//! `ProtoCorrelator` so the Protocol tab shows the wire-level traffic
//! of the *client*, not of Kapture itself. See `docs/specs/proxy-mode.md`.
//!
//! Phase 1: single broker, plain TCP, no SASL, no TLS.

use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct ProxyConfig {
    /// `host:port` of the upstream Kafka broker we forward to.
    pub upstream: String,
    /// TCP port we bind on `127.0.0.1` for clients to connect to.
    pub listen_port: u16,
}

impl ProxyConfig {
    #[must_use]
    pub const fn new(upstream: String, listen_port: u16) -> Self {
        Self { upstream, listen_port }
    }

    #[must_use]
    pub fn listen_addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.listen_port))
    }
}
```

Then in `src-tauri/src/lib.rs` after the `mod profiles;` line, add:

```rust
mod proxy;
```

(Place alphabetically: between `profiles` and `proto_decode`.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cd src-tauri && cargo test --lib proxy::tests 2>&1 | tail -10`
Expected: `test result: ok. 1 passed`.

- [ ] **Step 5: Run the full lints to make sure the new module passes our gates**

Run: `cd src-tauri && cargo clippy --all-targets --message-format=short 2>&1 | tail -30`
Expected: no warnings, no errors. Our `pedantic + nursery = deny` config will catch anything sloppy.

- [ ] **Step 6: Commit**

```bash
cd /Users/sderosiaux/code/personal/kapture
git add src-tauri/src/proxy.rs src-tauri/src/lib.rs
git commit -m "proxy: skeleton ProxyConfig + module wiring"
```

---

## Task 2: Frame splitter — length-prefix codec

**Files:**

- Modify: `src-tauri/src/proxy.rs`
- Test: `src-tauri/src/proxy.rs` (extend the existing `mod tests`)

The Kafka wire format is a 4-byte big-endian length prefix followed by `length` bytes of frame body. `tokio_util::codec::LengthDelimitedCodec` handles this exactly. We need to add `tokio-util` to deps.

- [ ] **Step 1: Add tokio-util to Cargo.toml**

In `src-tauri/Cargo.toml` `[dependencies]`, after the `tokio = ...` line, add:

```toml
tokio-util = { version = "0.7", features = ["codec"] }
futures = "0.3"
```

`futures` is needed for the `StreamExt`/`SinkExt` traits used by `Framed`.

- [ ] **Step 2: Write the failing test**

Append to `src-tauri/src/proxy.rs` `mod tests`:

```rust
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn frame_codec_decodes_length_prefixed_payloads() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let mut framed = framed_kafka(sock);
            use futures::StreamExt;
            let frame = framed.next().await.unwrap().unwrap();
            assert_eq!(frame.as_ref(), b"hello");
            let frame = framed.next().await.unwrap().unwrap();
            assert_eq!(frame.as_ref(), b"world!");
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        // Two frames back-to-back: 4-byte BE length + body.
        client.write_all(&5u32.to_be_bytes()).await.unwrap();
        client.write_all(b"hello").await.unwrap();
        client.write_all(&6u32.to_be_bytes()).await.unwrap();
        client.write_all(b"world!").await.unwrap();
        client.shutdown().await.unwrap();

        server.await.unwrap();
    }
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd src-tauri && cargo test --lib proxy::tests::frame_codec_decodes_length_prefixed_payloads 2>&1 | tail -20`
Expected: FAIL — `framed_kafka` undefined.

- [ ] **Step 4: Implement `framed_kafka`**

Append to `src-tauri/src/proxy.rs` (above `#[cfg(test)]`):

```rust
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// Wrap a `TcpStream` in the Kafka wire-frame codec: 4-byte big-endian
/// length prefix followed by `length` body bytes. The codec hands us
/// one `Bytes` per frame on the read side, and accepts a `Bytes` per
/// frame on the write side (it prepends the length itself).
///
/// Max frame size is 100 MiB. The Kafka default `socket.request.max.bytes`
/// is 100 MiB, and Kafka brokers reject anything larger, so this is the
/// effective wire ceiling. Anything bigger than that and a `kafkacat -L`
/// against a 10k-topic cluster would still parse, while a malicious peer
/// can't OOM us with a 4 GiB `len` field.
pub(crate) fn framed_kafka(socket: TcpStream) -> Framed<TcpStream, LengthDelimitedCodec> {
    let codec = LengthDelimitedCodec::builder()
        .length_field_offset(0)
        .length_field_length(4)
        .max_frame_length(100 * 1024 * 1024)
        .big_endian()
        .new_codec();
    Framed::new(socket, codec)
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd src-tauri && cargo test --lib proxy::tests::frame_codec 2>&1 | tail -20`
Expected: `test result: ok. 1 passed`. (The existing `proxy_config_normalises_listen_addr` should also still pass — run `cargo test --lib proxy::tests` to verify both.)

- [ ] **Step 6: Commit**

```bash
cd /Users/sderosiaux/code/personal/kapture
git add src-tauri/Cargo.toml src-tauri/Cargo.lock src-tauri/src/proxy.rs
git commit -m "proxy: length-prefix Kafka frame codec"
```

---

## Task 3: Request-header peek + correlation map

Responses on the wire don't carry `ApiKey` or `ApiVersion` — the client and broker know which one to expect because they remember the corresponding request. We need the same trick: when we observe a request frame, peek at its header to extract `(api_key, api_version, corr_id, sent_at)`, stash it under `corr_id`. When the matching response comes back from upstream, look it up to (a) tag the `ProtoFrame`, (b) compute RTT, (c) feed the right schema version to the decoder.

**Files:**

- Modify: `src-tauri/src/proxy.rs`
- Test: `src-tauri/src/proxy.rs` (extend `mod tests`)

- [ ] **Step 1: Write failing tests for the header peek**

Append to `src-tauri/src/proxy.rs` `mod tests`:

```rust
    #[test]
    fn peek_request_header_reads_api_key_version_corr_id() {
        // Wire shape (size prefix already stripped by the codec):
        //   api_key (i16 BE) | api_version (i16 BE) | corr_id (i32 BE) | rest...
        let mut buf = Vec::new();
        buf.extend_from_slice(&3i16.to_be_bytes()); // Metadata
        buf.extend_from_slice(&12i16.to_be_bytes()); // v12
        buf.extend_from_slice(&777i32.to_be_bytes()); // corr id
        buf.extend_from_slice(b"...remaining header + body...");

        let header = peek_request_header(&buf).unwrap();
        assert_eq!(header.api_key, 3);
        assert_eq!(header.api_version, 12);
        assert_eq!(header.corr_id, 777);
    }

    #[test]
    fn peek_request_header_rejects_short_buffer() {
        assert!(peek_request_header(&[0u8; 7]).is_none());
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd src-tauri && cargo test --lib proxy::tests::peek_request_header 2>&1 | tail -10`
Expected: FAIL — `peek_request_header` undefined.

- [ ] **Step 3: Implement `peek_request_header`**

Append to `src-tauri/src/proxy.rs`:

```rust
/// Minimum bytes needed to peek the (api_key, api_version, corr_id)
/// triple at the start of every Kafka request, regardless of header
/// version. The remainder of the header (client_id, tagged fields)
/// varies by version and we don't need it for routing / correlation.
const REQUEST_HEADER_PREFIX_LEN: usize = 8;

#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestHeaderPeek {
    pub api_key: i16,
    pub api_version: i16,
    pub corr_id: i32,
}

/// Read the fixed-shape request header prefix without consuming the
/// buffer. Returns `None` if the buffer is too short.
#[must_use]
pub(crate) fn peek_request_header(frame: &[u8]) -> Option<RequestHeaderPeek> {
    if frame.len() < REQUEST_HEADER_PREFIX_LEN {
        return None;
    }
    let api_key = i16::from_be_bytes([frame[0], frame[1]]);
    let api_version = i16::from_be_bytes([frame[2], frame[3]]);
    let corr_id = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
    Some(RequestHeaderPeek { api_key, api_version, corr_id })
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd src-tauri && cargo test --lib proxy::tests::peek_request_header 2>&1 | tail -10`
Expected: `test result: ok. 2 passed`.

- [ ] **Step 5: Write failing test for the per-connection correlation map**

Append to `src-tauri/src/proxy.rs` `mod tests`:

```rust
    #[test]
    fn correlation_map_pairs_request_and_response() {
        let map = CorrelationMap::default();
        map.record_request(42, RequestHeaderPeek { api_key: 1, api_version: 13, corr_id: 42 });
        let pending = map.take_response(42).unwrap();
        assert_eq!(pending.header.api_key, 1);
        assert_eq!(pending.header.api_version, 13);
        // RTT is positive (some elapsed time, even if tiny).
        let rtt = pending.rtt_at(std::time::Instant::now());
        assert!(rtt >= 0.0);
        // Subsequent take returns None — entries are consumed.
        assert!(map.take_response(42).is_none());
    }

    #[test]
    fn correlation_map_returns_none_for_unknown_corr_id() {
        let map = CorrelationMap::default();
        assert!(map.take_response(999).is_none());
    }
```

- [ ] **Step 6: Run tests to verify they fail**

Run: `cd src-tauri && cargo test --lib proxy::tests::correlation_map 2>&1 | tail -10`
Expected: FAIL — `CorrelationMap` undefined.

- [ ] **Step 7: Implement `CorrelationMap`**

Append to `src-tauri/src/proxy.rs`:

```rust
use std::collections::HashMap;
use std::time::Instant;

use parking_lot::Mutex;

/// One in-flight request awaiting its matching response on the same
/// TCP connection. The `sent_at` timestamp powers RTT measurement —
/// strictly per-connection, not per-broker, since corr_id uniqueness
/// is only guaranteed within one TCP connection (Kafka spec).
#[derive(Debug, Clone, Copy)]
pub(crate) struct PendingRequest {
    pub header: RequestHeaderPeek,
    pub sent_at: Instant,
}

impl PendingRequest {
    #[must_use]
    pub fn rtt_at(&self, now: Instant) -> f64 {
        let elapsed = now.saturating_duration_since(self.sent_at);
        // ms with fractional precision, like the proto-hook path.
        elapsed.as_secs_f64() * 1000.0
    }
}

/// Per-connection map `corr_id → in-flight request`.
///
/// Bounded implicitly by the number of in-flight Kafka requests on
/// one TCP connection — Kafka clients pipeline but cap at a few
/// hundred. We rely on the response take to drain entries; if a
/// connection drops mid-flight any leftovers are released when the
/// owning task exits and drops the map.
#[derive(Debug, Default)]
pub(crate) struct CorrelationMap {
    inner: Mutex<HashMap<i32, PendingRequest>>,
}

impl CorrelationMap {
    pub fn record_request(&self, corr_id: i32, header: RequestHeaderPeek) {
        self.inner.lock().insert(
            corr_id,
            PendingRequest { header, sent_at: Instant::now() },
        );
    }

    pub fn take_response(&self, corr_id: i32) -> Option<PendingRequest> {
        self.inner.lock().remove(&corr_id)
    }
}
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `cd src-tauri && cargo test --lib proxy::tests 2>&1 | tail -10`
Expected: all proxy tests pass (4-5 by now).

- [ ] **Step 9: Run lints**

Run: `cd src-tauri && cargo clippy --all-targets --message-format=short 2>&1 | tail -20`
Expected: no warnings.

- [ ] **Step 10: Commit**

```bash
cd /Users/sderosiaux/code/personal/kapture
git add src-tauri/src/proxy.rs
git commit -m "proxy: request-header peek + per-connection corr-id map"
```

---

## Task 4: Per-connection pump — bidirectional copy with tap

The pump is what makes Kapture a real proxy. For each accepted client TCP socket: open an upstream socket, run two halves driving the codec; on each frame in either direction, tap a copy to a callback (the inspector tap) before forwarding the frame verbatim.

We use a callback rather than a hard-coded `ProtoCorrelator` reference so the pump is unit-testable without the rest of the app.

**Files:**

- Modify: `src-tauri/src/proxy.rs`
- Test: `src-tauri/src/proxy.rs`

- [ ] **Step 1: Write the failing integration test**

Append to `src-tauri/src/proxy.rs` `mod tests`:

```rust
    use std::sync::Arc;
    use parking_lot::Mutex as PMutex;

    /// End-to-end: spin up a fake upstream broker that echoes each
    /// frame with its bytes reversed, run the per-connection pump
    /// against it, send a frame from the "client" side, and assert
    /// (a) the client gets the reversed echo and (b) the inspector
    /// tap saw both frames with the right direction.
    #[tokio::test]
    async fn per_connection_pump_taps_both_directions() {
        // Fake upstream — accepts one connection, reads one frame,
        // writes back the reversed bytes (still as a length-prefixed
        // frame), then closes.
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (sock, _) = upstream.accept().await.unwrap();
            let mut framed = framed_kafka(sock);
            use futures::{SinkExt, StreamExt};
            let frame = framed.next().await.unwrap().unwrap();
            let mut reply = frame.to_vec();
            reply.reverse();
            framed.send(reply.into()).await.unwrap();
        });

        // Tap collector.
        type Tap = Arc<PMutex<Vec<(ProxyDirection, Vec<u8>)>>>;
        let tap: Tap = Arc::new(PMutex::new(Vec::new()));
        let tap_for_pump = Arc::clone(&tap);

        // Client side of the pump: a paired in-memory socket would be
        // ideal but we use a real loopback TCP for simplicity.
        let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client_addr = client_listener.local_addr().unwrap();
        let upstream_target = upstream_addr.to_string();
        let pump_task = tokio::spawn(async move {
            let (client_sock, _) = client_listener.accept().await.unwrap();
            let upstream_sock = TcpStream::connect(upstream_target).await.unwrap();
            run_pump(
                ConnectionId(1),
                client_sock,
                upstream_sock,
                move |dir, conn, payload| {
                    assert_eq!(conn, ConnectionId(1));
                    tap_for_pump.lock().push((dir, payload.to_vec()));
                },
            )
            .await
            .unwrap();
        });

        // Drive the client.
        let mut client = TcpStream::connect(client_addr).await.unwrap();
        client.write_all(&8u32.to_be_bytes()).await.unwrap();
        client.write_all(b"helloKKK").await.unwrap();
        // Read the echoed reply.
        let mut framed_client = framed_kafka(client);
        use futures::StreamExt;
        let reply = framed_client.next().await.unwrap().unwrap();
        assert_eq!(reply.as_ref(), b"KKKolleh");

        upstream_task.await.unwrap();
        pump_task.await.unwrap();

        let captured = tap.lock().clone();
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].0, ProxyDirection::ClientToUpstream);
        assert_eq!(captured[0].1, b"helloKKK");
        assert_eq!(captured[1].0, ProxyDirection::UpstreamToClient);
        assert_eq!(captured[1].1, b"KKKolleh");
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd src-tauri && cargo test --lib proxy::tests::per_connection_pump 2>&1 | tail -30`
Expected: FAIL — `run_pump`, `ConnectionId`, `ProxyDirection` undefined.

- [ ] **Step 3: Implement `run_pump`**

Append to `src-tauri/src/proxy.rs`:

```rust
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

/// Monotonic, never-zero connection identifier. Used as the pairing
/// key for `(corr_id, connection_id)` in the inspector — replaces
/// the `broker_id` semantics from the rdkafka-client mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u64);

/// Direction of a tapped frame, from the proxy's point of view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyDirection {
    /// Frame came in from the connecting Kafka client → going to upstream.
    ClientToUpstream,
    /// Frame came back from upstream → going to the connecting client.
    UpstreamToClient,
}

/// Atomic monotonic generator for `ConnectionId`. One global counter
/// is fine — these are session-scoped and never persisted.
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

#[must_use]
pub fn next_connection_id() -> ConnectionId {
    ConnectionId(NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed))
}

/// Drive both directions of one client/upstream pair. Returns when
/// either side closes its half. Errors short-circuit and propagate to
/// the caller — the spawn site logs and drops the pump.
///
/// `tap` is invoked synchronously per frame, *before* forwarding, so
/// the inspector observes frames in arrival order. The callback must
/// not block: in production it just pushes into the correlator's
/// ring-buffer mutex (~µs).
pub async fn run_pump<F>(
    conn_id: ConnectionId,
    client: TcpStream,
    upstream: TcpStream,
    tap: F,
) -> io::Result<()>
where
    F: Fn(ProxyDirection, ConnectionId, &Bytes) + Send + Sync + 'static,
{
    let mut client_framed = framed_kafka(client);
    let mut upstream_framed = framed_kafka(upstream);
    let tap = Arc::new(tap);

    loop {
        tokio::select! {
            // Client → upstream
            frame = client_framed.next() => {
                let Some(frame) = frame else { break; };
                let frame = frame?;
                let bytes = frame.freeze();
                tap(ProxyDirection::ClientToUpstream, conn_id, &bytes);
                upstream_framed.send(bytes).await?;
            }
            // Upstream → client
            frame = upstream_framed.next() => {
                let Some(frame) = frame else { break; };
                let frame = frame?;
                let bytes = frame.freeze();
                tap(ProxyDirection::UpstreamToClient, conn_id, &bytes);
                client_framed.send(bytes).await?;
            }
        }
    }
    Ok(())
}
```

NOTE: `LengthDelimitedCodec` decodes into `BytesMut`, which has `.freeze()` to convert to immutable `Bytes`. The send side accepts `Bytes` and prepends the length prefix automatically — we do NOT pass the prefix bytes through manually.

NOTE on the `Arc` around `tap`: we accept `F: Fn` so the callback can be invoked from either select arm. We don't actually need the `Arc` since `tap` is owned by the function — the variable shadowing into `Arc::new(tap)` is dead. **Drop the `let tap = Arc::new(tap);` line** — the closure `tap(...)` calls work directly on the moved `F`. Verify clippy is happy with this.

(If clippy complains about `tap` not being `Send + Sync` for the spawn boundary, add `+ 'static` and pass through the original `tap` directly.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cd src-tauri && cargo test --lib proxy::tests::per_connection_pump_taps_both_directions 2>&1 | tail -30`
Expected: `test result: ok. 1 passed`.

- [ ] **Step 5: Run all proxy tests + lints**

Run: `cd src-tauri && cargo test --lib proxy 2>&1 | tail -10 && cargo clippy --all-targets --message-format=short 2>&1 | tail -20`
Expected: all proxy tests pass, no clippy warnings.

- [ ] **Step 6: Commit**

```bash
cd /Users/sderosiaux/code/personal/kapture
git add src-tauri/src/proxy.rs
git commit -m "proxy: per-connection bidirectional pump with frame tap"
```

---

## Task 5: ProtoFrame emission — wire the pump tap to ProtoCorrelator

We have the pump tapping raw bytes. Now build the bridge: convert each tapped frame into a `ProtoEvent` (the same struct the librdkafka FFI emits) and feed it to `ProtoCorrelator::record_event`. Use the per-connection `CorrelationMap` to enrich responses with the matching request's `(api_key, api_version)` and RTT.

The proxy's `broker_id` field carries the `ConnectionId` (truncated to i32 — fine for u64 < 2 billion which is way beyond any session). Renaming the field to `connection_id` is a Phase 5 polish item; for now we keep the type identical so the existing UI keeps working.

**Files:**

- Modify: `src-tauri/src/proxy.rs`
- Test: `src-tauri/src/proxy.rs`

- [ ] **Step 1: Write the failing test**

Append to `src-tauri/src/proxy.rs` `mod tests`:

```rust
    #[test]
    fn build_proto_event_for_request_uses_peeked_header() {
        let map = CorrelationMap::default();
        // 8-byte header prefix: api_key=18 (ApiVersions), api_ver=3, corr_id=99
        let mut frame = Vec::new();
        frame.extend_from_slice(&18i16.to_be_bytes());
        frame.extend_from_slice(&3i16.to_be_bytes());
        frame.extend_from_slice(&99i32.to_be_bytes());
        frame.extend_from_slice(b"....rest....");

        let event = build_proto_event(
            ProxyDirection::ClientToUpstream,
            ConnectionId(7),
            &frame,
            &map,
        );

        assert!(matches!(event.direction, crate::proto_hook::ProtoDirection::Send));
        assert_eq!(event.api_key, 18);
        assert_eq!(event.api_version, 3);
        assert_eq!(event.corr_id, 99);
        assert_eq!(event.broker_id, 7);
        assert_eq!(event.payload_size, frame.len());
        assert!(event.rtt_ms == 0.0);
        // Map now holds an entry for corr_id 99.
        assert!(map.take_response(99).is_some());
    }

    #[test]
    fn build_proto_event_for_response_resolves_from_map() {
        let map = CorrelationMap::default();
        map.record_request(
            42,
            RequestHeaderPeek { api_key: 1, api_version: 13, corr_id: 42 },
        );
        // Response wire prefix: corr_id (i32 BE) at offset 0.
        let mut frame = Vec::new();
        frame.extend_from_slice(&42i32.to_be_bytes());
        frame.extend_from_slice(b"....body....");

        let event = build_proto_event(
            ProxyDirection::UpstreamToClient,
            ConnectionId(7),
            &frame,
            &map,
        );

        assert!(matches!(event.direction, crate::proto_hook::ProtoDirection::Recv));
        assert_eq!(event.api_key, 1);
        assert_eq!(event.api_version, 13);
        assert_eq!(event.corr_id, 42);
        assert_eq!(event.broker_id, 7);
        assert!(event.rtt_ms >= 0.0);
    }

    #[test]
    fn build_proto_event_for_unknown_response_is_marked_unknown() {
        let map = CorrelationMap::default();
        // Response with no matching request in the map.
        let mut frame = Vec::new();
        frame.extend_from_slice(&404i32.to_be_bytes());
        frame.extend_from_slice(b"....body....");

        let event = build_proto_event(
            ProxyDirection::UpstreamToClient,
            ConnectionId(7),
            &frame,
            &map,
        );

        assert_eq!(event.api_key, -1);
        assert_eq!(event.api_version, -1);
        assert_eq!(event.corr_id, 404);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd src-tauri && cargo test --lib proxy::tests::build_proto_event 2>&1 | tail -20`
Expected: FAIL — `build_proto_event` undefined.

- [ ] **Step 3: Implement `build_proto_event`**

Append to `src-tauri/src/proxy.rs`:

```rust
use crate::proto_hook::{ProtoDirection, ProtoEvent};

/// Cap on `payload` we copy into the `ProtoEvent`. Mirrors the C-side
/// `RD_KAFKA_PROTO_HOOK_PAYLOAD_MAX` so the Protocol tab's hex view +
/// decoded body stays bounded across both client and proxy modes.
const PROTO_PAYLOAD_CAP: usize = 64 * 1024;

/// Build the `ProtoEvent` for one tapped frame. On the request path,
/// peek the header and stash it in `corr_map`. On the response path,
/// look up the matching request to recover `(api_key, api_version)`
/// and RTT.
#[must_use]
pub(crate) fn build_proto_event(
    dir: ProxyDirection,
    conn_id: ConnectionId,
    frame: &[u8],
    corr_map: &CorrelationMap,
) -> ProtoEvent {
    let payload_size = frame.len();
    let take = payload_size.min(PROTO_PAYLOAD_CAP);
    let payload = frame[..take].to_vec();
    let broker_id = i32::try_from(conn_id.0 & 0x7FFF_FFFF).unwrap_or(i32::MAX);

    match dir {
        ProxyDirection::ClientToUpstream => {
            let header = peek_request_header(frame);
            if let Some(h) = header {
                corr_map.record_request(h.corr_id, h);
            }
            ProtoEvent {
                direction: ProtoDirection::Send,
                api_key: header.map_or(-1, |h| i32::from(h.api_key)),
                api_version: header.map_or(-1, |h| i32::from(h.api_version)),
                corr_id: header.map_or(0, |h| h.corr_id),
                broker_id,
                payload_size,
                rtt_ms: 0.0,
                payload,
            }
        }
        ProxyDirection::UpstreamToClient => {
            // Response wire prefix is just the 4-byte correlation id.
            let corr_id = if frame.len() >= 4 {
                i32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]])
            } else {
                0
            };
            let pending = corr_map.take_response(corr_id);
            let rtt_ms = pending.map_or(0.0, |p| p.rtt_at(Instant::now()));
            ProtoEvent {
                direction: ProtoDirection::Recv,
                api_key: pending.map_or(-1, |p| i32::from(p.header.api_key)),
                api_version: pending.map_or(-1, |p| i32::from(p.header.api_version)),
                corr_id,
                broker_id,
                payload_size,
                rtt_ms,
                payload,
            }
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd src-tauri && cargo test --lib proxy::tests::build_proto_event 2>&1 | tail -10`
Expected: `test result: ok. 3 passed`.

- [ ] **Step 5: Verify the existing decoder still works for proxy-shaped payloads**

The existing `proto_decode::decode_frame` (`src-tauri/src/proto_decode.rs:31`) expects a 4-byte size prefix at the start of `payload`. Our proxy tap captures the body **without** the size prefix (the `LengthDelimitedCodec` strips it). So `decode_frame` would mis-parse.

Three options:

- (a) Re-prepend the size prefix in `build_proto_event` before stashing into `payload`.
- (b) Add a `decode_body_only` entry point to `proto_decode` and use it when the source is the proxy.
- (c) Document `payload` as "frame body, no size prefix" and have the consumer (`ProtoCorrelator::record_event`) know the source.

(a) is the smallest change — we already keep the original frame length in `payload_size`, and the size prefix is just `(frame.len() as i32).to_be_bytes()`. Do (a).

Modify `build_proto_event` so the `payload` field includes a size-prefix header that matches what `decode_frame` expects:

```rust
// Replace the `let take = ...; let payload = ...;` lines with:
let body_take = payload_size.min(PROTO_PAYLOAD_CAP - 4);
let mut payload = Vec::with_capacity(body_take + 4);
let size_prefix = i32::try_from(payload_size).unwrap_or(i32::MAX);
payload.extend_from_slice(&size_prefix.to_be_bytes());
payload.extend_from_slice(&frame[..body_take]);
```

And update the `payload_size` assertion in tests: it represents the WIRE size including the size prefix. Wait — the wire size is `4 + body.len()` from a kafkacat perspective, but `ProtoEvent.payload_size` in the rdkafka path means the frame body length (excluding the 4-byte size header). Let me check the existing `proto_hook.rs` semantics:

`proto_hook.rs:80` — `pub payload_size: usize` — described as "true wire size". In `correlator.rs:171-184` it's stored as `size: event.payload_size`. The hex view trims to `captured = payload.len()`.

For consistency with the rdkafka path, `payload_size` should be the body length only (matching what librdkafka reports — the frame body, the codec already stripped the prefix on its side too). And the captured `payload` field should align with what `proto_decode::decode_frame` expects, which is `[size_prefix(4) | header | body]`. So:

- `payload_size = frame.len()` — the body length (what the codec gave us).
- `payload = [size_prefix bytes (= frame.len() as i32 BE) | frame[..take]]` — to match `decode_frame`'s expectation.

Update the test assertions accordingly: `event.payload_size` equals `frame.len()` (body length), but `event.payload.len()` is `4 + min(frame.len(), CAP-4)`.

Update the tests:

```rust
        assert_eq!(event.payload_size, frame.len());
        assert_eq!(&event.payload[..4], &(frame.len() as i32).to_be_bytes());
        assert_eq!(&event.payload[4..], &frame[..]);
```

(Apply the same correction to all three tests.)

- [ ] **Step 6: Re-run tests**

Run: `cd src-tauri && cargo test --lib proxy::tests 2>&1 | tail -10`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
cd /Users/sderosiaux/code/personal/kapture
git add src-tauri/src/proxy.rs
git commit -m "proxy: emit ProtoEvent with header peek + corr-id pairing"
```

---

## Task 6: Listener + ProxyHandle — accept loop and stop signal

Now wrap the pump in a real `TcpListener` accept loop, with a `tokio::sync::watch` stop signal mirroring `CaptureHandle::stop_tx`.

**Files:**

- Modify: `src-tauri/src/proxy.rs`
- Test: `src-tauri/src/proxy.rs`

- [ ] **Step 1: Write the failing integration test**

Append to `src-tauri/src/proxy.rs` `mod tests`:

```rust
    #[tokio::test]
    async fn proxy_handle_accepts_one_client_and_forwards_to_upstream() {
        // Fake upstream — accepts ONE connection, echoes one frame.
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (sock, _) = upstream.accept().await.unwrap();
            let mut framed = framed_kafka(sock);
            use futures::{SinkExt, StreamExt};
            let frame = framed.next().await.unwrap().unwrap();
            framed.send(frame.freeze()).await.unwrap();
        });

        let correlator = Arc::new(crate::correlator::ProtoCorrelator::new());
        let cfg = ProxyConfig {
            upstream: upstream_addr.to_string(),
            listen_port: 0, // OS assigns
        };
        let handle = ProxyHandle::start(cfg, Arc::clone(&correlator)).await.unwrap();
        let listen_addr = handle.local_addr();

        // Drive a fake client.
        let mut client = TcpStream::connect(listen_addr).await.unwrap();
        client.write_all(&5u32.to_be_bytes()).await.unwrap();
        // Use a 4-byte header prefix worth of data so peek doesn't reject.
        client.write_all(b"\x00\x12\x00\x03X").await.unwrap();
        let mut framed = framed_kafka(client);
        use futures::StreamExt;
        let echoed = framed.next().await.unwrap().unwrap();
        assert_eq!(echoed.as_ref(), b"\x00\x12\x00\x03X");

        upstream_task.await.unwrap();

        // Correlator should have observed at least 2 frames (send + recv).
        let summaries = correlator.summaries(100);
        assert!(summaries.len() >= 2);

        handle.stop().await;
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd src-tauri && cargo test --lib proxy::tests::proxy_handle_accepts 2>&1 | tail -30`
Expected: FAIL — `ProxyHandle` undefined.

- [ ] **Step 3: Implement `ProxyHandle`**

Append to `src-tauri/src/proxy.rs`:

```rust
use crate::correlator::ProtoCorrelator;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// A running proxy listener. Drop / `stop()` to tear down.
pub struct ProxyHandle {
    stop_tx: watch::Sender<bool>,
    accept_task: Option<JoinHandle<()>>,
    local_addr: SocketAddr,
    upstream: String,
}

impl std::fmt::Debug for ProxyHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyHandle")
            .field("local_addr", &self.local_addr)
            .field("upstream", &self.upstream)
            .field("running", &self.accept_task.is_some())
            .finish()
    }
}

impl ProxyHandle {
    /// Bind the listener and spawn the accept loop.
    ///
    /// # Errors
    /// Returns the underlying `io::Error` if the bind fails (port in
    /// use, permission denied, …).
    pub async fn start(
        config: ProxyConfig,
        correlator: Arc<ProtoCorrelator>,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(config.listen_addr()).await?;
        let local_addr = listener.local_addr()?;
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let upstream = config.upstream.clone();
        let upstream_for_task = upstream.clone();

        let accept_task = tokio::spawn(async move {
            info!(listen = %local_addr, upstream = %upstream_for_task, "proxy listening");
            loop {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_ok() && *stop_rx.borrow() {
                            info!("proxy accept loop stopping");
                            break;
                        }
                    }
                    accepted = listener.accept() => {
                        match accepted {
                            Ok((client_sock, peer)) => {
                                let conn_id = next_connection_id();
                                let upstream_target = upstream_for_task.clone();
                                let correlator = Arc::clone(&correlator);
                                let corr_map = Arc::new(CorrelationMap::default());
                                tokio::spawn(async move {
                                    let upstream_sock = match TcpStream::connect(&upstream_target).await {
                                        Ok(s) => s,
                                        Err(err) => {
                                            warn!(conn = conn_id.0, error = %err, "upstream connect failed");
                                            return;
                                        }
                                    };
                                    info!(conn = conn_id.0, peer = %peer, "proxy connection opened");
                                    let corr_map_for_tap = Arc::clone(&corr_map);
                                    let result = run_pump(
                                        conn_id,
                                        client_sock,
                                        upstream_sock,
                                        move |dir, conn, payload| {
                                            let event = build_proto_event(
                                                dir,
                                                conn,
                                                payload,
                                                &corr_map_for_tap,
                                            );
                                            correlator.record_event(&event);
                                        },
                                    )
                                    .await;
                                    if let Err(err) = result {
                                        warn!(conn = conn_id.0, error = %err, "proxy pump error");
                                    }
                                    info!(conn = conn_id.0, "proxy connection closed");
                                });
                            }
                            Err(err) => {
                                warn!(error = %err, "proxy accept failed");
                            }
                        }
                    }
                }
            }
        });

        Ok(Self {
            stop_tx,
            accept_task: Some(accept_task),
            local_addr,
            upstream,
        })
    }

    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    #[must_use]
    pub fn upstream(&self) -> &str {
        &self.upstream
    }

    pub async fn stop(mut self) {
        let _ = self.stop_tx.send(true);
        if let Some(task) = self.accept_task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for ProxyHandle {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(true);
        if let Some(task) = self.accept_task.take() {
            task.abort();
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd src-tauri && cargo test --lib proxy::tests::proxy_handle_accepts 2>&1 | tail -30`
Expected: `test result: ok. 1 passed`.

- [ ] **Step 5: Run all proxy tests + lints**

Run: `cd src-tauri && cargo test --lib proxy 2>&1 | tail -10 && cargo clippy --all-targets --message-format=short 2>&1 | tail -20`
Expected: all proxy tests pass, no clippy warnings.

- [ ] **Step 6: Commit**

```bash
cd /Users/sderosiaux/code/personal/kapture
git add src-tauri/src/proxy.rs
git commit -m "proxy: ProxyHandle with accept loop + per-conn pump spawn"
```

---

## Task 7: AppState wiring — proxy slot mutually exclusive with capture

**Files:**

- Modify: `src-tauri/src/state.rs`

- [ ] **Step 1: Read the current state to plan the change**

Read `src-tauri/src/state.rs` end-to-end. The `Inner` struct holds `capture: Option<CaptureHandle>` and `correlator: Option<Arc<ProtoCorrelator>>`. We add a sibling `proxy: Option<ProxyHandle>` and let both modes share the `correlator` slot — there's exactly one Protocol tab regardless of mode, so one correlator at a time is correct.

- [ ] **Step 2: Modify `state.rs`**

Edit `src-tauri/src/state.rs:36-42` (the `Inner` struct):

```rust
#[derive(Debug, Default)]
struct Inner {
    capture: Option<CaptureHandle>,
    proxy: Option<crate::proxy::ProxyHandle>,
    sr_client: Option<Arc<SchemaRegistryClient>>,
    correlator: Option<Arc<ProtoCorrelator>>,
    started_at: Option<Instant>,
}
```

After the existing `take_capture` impl (around `state.rs:76-86`), add:

```rust
    pub fn install_proxy(&self, handle: crate::proxy::ProxyHandle, correlator: Arc<ProtoCorrelator>) {
        {
            let mut guard = self.inner.lock();
            guard.proxy = Some(handle);
            guard.correlator = Some(correlator);
            guard.started_at = Some(Instant::now());
        }
        self.capture_pending.store(false, Ordering::Release);
    }

    pub fn take_proxy(&self) -> Option<crate::proxy::ProxyHandle> {
        let taken = {
            let mut guard = self.inner.lock();
            guard.started_at = None;
            guard.correlator = None;
            guard.proxy.take()
        };
        self.capture_pending.store(false, Ordering::Release);
        taken
    }

    pub fn is_proxying(&self) -> bool {
        self.inner.lock().proxy.is_some()
    }
```

Update `is_capturing()` to mean "any capture surface is active" — this preserves existing semantics for the stats emitter loop:

Change `state.rs:88-91`:

```rust
    pub fn is_capturing(&self) -> bool {
        let guard = self.inner.lock();
        let has_handle = guard.capture.is_some() || guard.proxy.is_some();
        drop(guard);
        has_handle || self.capture_pending.load(Ordering::Acquire)
    }
```

Update `try_claim_capture_slot` to also reject when a proxy is running:

Change `state.rs:97-106`:

```rust
    pub fn try_claim_capture_slot(&self) -> bool {
        let guard = self.inner.lock();
        if guard.capture.is_some() || guard.proxy.is_some() {
            return false;
        }
        drop(guard);
        self.capture_pending
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
```

- [ ] **Step 3: Build to verify the type plumbing compiles**

Run: `cd src-tauri && cargo build --message-format=short 2>&1 | tail -20`
Expected: clean build (warnings about unused new methods are OK at this stage).

- [ ] **Step 4: Run lints**

Run: `cd src-tauri && cargo clippy --all-targets --message-format=short 2>&1 | tail -20`
Expected: no warnings.

- [ ] **Step 5: Commit**

```bash
cd /Users/sderosiaux/code/personal/kapture
git add src-tauri/src/state.rs
git commit -m "state: mutually-exclusive proxy slot alongside capture"
```

---

## Task 8: Tauri commands — start_proxy / stop_proxy

**Files:**

- Modify: `src-tauri/src/commands.rs`
- Modify: `src-tauri/src/lib.rs`
- Modify: `src-tauri/src/error.rs` (add `AlreadyProxying`, `NotProxying` if not covered by existing variants)

- [ ] **Step 1: Read existing command shape and error variants**

Run: `cd /Users/sderosiaux/code/personal/kapture && rg -n 'pub enum KaptureError' src-tauri/src/error.rs`
Read the `KaptureError` enum end-to-end so the new variants match the existing pattern (`thiserror::Error`, JSON-serialisable, etc.).

- [ ] **Step 2: Add error variants if missing**

In `src-tauri/src/error.rs` (file expected to follow the `#[derive(Debug, thiserror::Error, serde::Serialize)]` pattern), append two variants if not present:

```rust
    #[error("a proxy listener is already running")]
    AlreadyProxying,
    #[error("no proxy listener is running")]
    NotProxying,
    #[error("proxy: {0}")]
    Proxy(String),
```

(If the existing `AlreadyCapturing` / `NotCapturing` variants cover the proxy cases too because `is_capturing` returned `true`, we still want a clearer error message in proxy mode. Add the variants.)

- [ ] **Step 3: Add the two commands**

Append to `src-tauri/src/commands.rs`:

```rust
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyStatus {
    pub listen_addr: String,
    pub upstream: String,
}

/// Start the proxy listener. Bound to `127.0.0.1:listen_port`. The
/// previous capture (if any) is stopped first so client mode and
/// proxy mode are mutually exclusive — exactly one Protocol tab at
/// a time.
#[tauri::command]
pub async fn start_proxy(
    state: State<'_, AppState>,
    app: AppHandle,
    upstream: String,
    listen_port: u16,
) -> Result<ProxyStatus> {
    if let Some(handle) = state.take_capture() {
        handle.stop().await;
    }
    if let Some(handle) = state.take_proxy() {
        handle.stop().await;
    }
    if !state.try_claim_capture_slot() {
        return Err(KaptureError::AlreadyProxying);
    }
    state.buffer.clear();

    let trimmed_upstream = upstream.trim().to_owned();
    if trimmed_upstream.is_empty() {
        state.release_capture_slot();
        return Err(KaptureError::Config("upstream must be non-empty".to_owned()));
    }

    let correlator = Arc::new(ProtoCorrelator::new());
    let cfg = crate::proxy::ProxyConfig {
        upstream: trimmed_upstream.clone(),
        listen_port,
    };
    let handle = crate::proxy::ProxyHandle::start(cfg, Arc::clone(&correlator))
        .await
        .map_err(|err| {
            state.release_capture_slot();
            KaptureError::Proxy(err.to_string())
        })?;
    let listen_addr = handle.local_addr().to_string();
    state.install_proxy(handle, correlator);
    spawn_stats_emitter(&app);
    info!(listen = %listen_addr, upstream = %trimmed_upstream, "proxy started");

    Ok(ProxyStatus {
        listen_addr,
        upstream: trimmed_upstream,
    })
}

#[tauri::command]
pub async fn stop_proxy(state: State<'_, AppState>) -> Result<()> {
    let Some(handle) = state.take_proxy() else {
        return Err(KaptureError::NotProxying);
    };
    handle.stop().await;
    info!("proxy stopped");
    Ok(())
}
```

- [ ] **Step 4: Register the commands in `lib.rs`**

In `src-tauri/src/lib.rs`, add to the `invoke_handler` list (around lib.rs:70-88) — after `commands::disconnect`:

```rust
            commands::start_proxy,
            commands::stop_proxy,
```

- [ ] **Step 5: Build + lint**

Run: `cd src-tauri && cargo build --message-format=short 2>&1 | tail -10 && cargo clippy --all-targets --message-format=short 2>&1 | tail -20`
Expected: no errors, no warnings.

- [ ] **Step 6: Smoke test the listener manually**

In one terminal:

```bash
cd /Users/sderosiaux/code/personal/kapture
docker compose up -d redpanda  # if not already up; SR not needed for proxy smoke
pnpm tauri dev
```

In the dev tools console (or via a temp CLI fixture — see Task 9), drive the start_proxy command. We'll do the proper end-to-end with kafkacat in Task 11.

- [ ] **Step 7: Commit**

```bash
cd /Users/sderosiaux/code/personal/kapture
git add src-tauri/src/commands.rs src-tauri/src/lib.rs src-tauri/src/error.rs
git commit -m "commands: start_proxy / stop_proxy Tauri handlers"
```

---

## Task 9: Frontend — Mode toggle in ConnectionDialog

**Files:**

- Modify: `src/types.ts`
- Modify: `src/components/ConnectionDialog.tsx`
- Modify: `src/App.tsx` (dispatch on mode)
- Modify: `src/components/TopBar.tsx` (status pill in proxy mode)

- [ ] **Step 1: Add proxy types to `types.ts`**

Append to `src/types.ts`:

```ts
export type ConnectionMode = "client" | "proxy";

export interface ProxyConfig {
  upstream: string;
  listenPort: number;
}

export interface ProxyStatus {
  listenAddr: string;
  upstream: string;
}
```

- [ ] **Step 2: Read ConnectionDialog to plan the edit**

Run: `head -100 src/components/ConnectionDialog.tsx` and locate the form's outer state hooks. Most existing fields (bootstrap, topics, auth, registry) are client-mode only. We add a `mode` state at the top and gate the existing fields behind `mode === "client"`.

- [ ] **Step 3: Add the Mode toggle**

In `src/components/ConnectionDialog.tsx`, near the top of the component (after the existing `useState` calls):

```tsx
const [mode, setMode] = useState<ConnectionMode>("client");
const [proxyUpstream, setProxyUpstream] = useState("localhost:9092");
const [proxyListenPort, setProxyListenPort] = useState(9092);
```

Render at the top of the form (above the existing fields):

```tsx
<fieldset className="mode-toggle">
  <legend>Mode</legend>
  <label>
    <input
      type="radio"
      name="mode"
      value="client"
      checked={mode === "client"}
      onChange={() => setMode("client")}
    />
    Client (Kapture connects as a consumer)
  </label>
  <label>
    <input
      type="radio"
      name="mode"
      value="proxy"
      checked={mode === "proxy"}
      onChange={() => setMode("proxy")}
    />
    Proxy (point your apps at Kapture)
  </label>
</fieldset>

{mode === "proxy" ? (
  <>
    <label>
      Upstream broker
      <input
        type="text"
        value={proxyUpstream}
        onChange={(e) => setProxyUpstream(e.target.value)}
        placeholder="kafka.example.com:9092"
      />
    </label>
    <label>
      Listen port (127.0.0.1)
      <input
        type="number"
        value={proxyListenPort}
        onChange={(e) => setProxyListenPort(Number(e.target.value))}
        min={1}
        max={65535}
      />
    </label>
  </>
) : (
  /* existing client-mode fields here, unchanged */
)}
```

Wrap the existing client-mode form (bootstrap servers, topics, schema registry, auth, TLS) in the `else` branch so it's not rendered in proxy mode.

- [ ] **Step 4: Branch the submit handler**

In the existing submit handler, branch on `mode`:

```tsx
if (mode === "proxy") {
  const status: ProxyStatus = await invoke("start_proxy", {
    upstream: proxyUpstream,
    listenPort: proxyListenPort,
  });
  // Update connection state to reflect proxy mode
  onConnected({ kind: "proxy", status });
  return;
}
// existing client-mode connect call
```

(The exact signature of `onConnected` depends on the file — adapt to whatever the parent passes.)

- [ ] **Step 5: Update `App.tsx` connection state**

The connection state currently tracks one client mode. Add a discriminated union or an extra field. Smallest change: add `mode: ConnectionMode` to `ConnectionState`, and a `proxyStatus: ProxyStatus | null` field. Disconnect dispatches to `stop_proxy` when `mode === "proxy"`, `disconnect` otherwise.

Edit `src/types.ts` `ConnectionState`:

```ts
export interface ConnectionState {
  status: ConnectionStatus;
  mode: ConnectionMode; // "client" | "proxy"
  cluster: string | null;
  topicPattern: string | null;
  error: string | null;
  schemaRegistryUrl: string | null;
  fromBeginning: boolean;
  authPrefill: ConnectionAuthPrefill | null;
  proxyStatus: ProxyStatus | null;
}
```

In `src/App.tsx`, the disconnect handler:

```tsx
const disconnect = async () => {
  if (connection.mode === "proxy") {
    await invoke("stop_proxy");
  } else {
    await invoke("disconnect");
  }
};
```

- [ ] **Step 6: TopBar status pill**

In `src/components/TopBar.tsx`, when `mode === "proxy"` and `proxyStatus` is non-null, render:

```tsx
<span className="cluster-pill">
  proxy {proxyStatus.listenAddr} → {proxyStatus.upstream}
</span>
```

Otherwise render the existing client-mode pill unchanged.

- [ ] **Step 7: Build + typecheck**

Run: `cd /Users/sderosiaux/code/personal/kapture && pnpm typecheck 2>&1 | tail -20`
Expected: no errors. (If `pnpm typecheck` doesn't exist, substitute `pnpm exec tsc --noEmit`.)

- [ ] **Step 8: Lint**

Run: `pnpm lint 2>&1 | tail -20`
Expected: no errors.

- [ ] **Step 9: Commit**

```bash
cd /Users/sderosiaux/code/personal/kapture
git add src/types.ts src/components/ConnectionDialog.tsx src/components/TopBar.tsx src/App.tsx
git commit -m "ui: ConnectionDialog mode toggle (client / proxy)"
```

---

## Task 10: End-to-end smoke test against Apache Kafka 4.2 + kafkacat

The final integration check: a real Kafka broker, a real kafkacat, and Kapture in proxy mode in between. This is a manual test the agent runs and reports on; not in CI yet.

**Files:**

- None (this task is a smoke test, no code).

- [ ] **Step 1: Bring up the dev stack**

```bash
cd /Users/sderosiaux/code/personal/kapture
docker compose up -d
docker compose ps   # verify both Redpanda + Apache Kafka are up
```

- [ ] **Step 2: Start Kapture in dev mode**

```bash
cd /Users/sderosiaux/code/personal/kapture
pnpm tauri dev
```

- [ ] **Step 3: Configure the proxy via the dialog**

In Kapture's Connection dialog: Mode = Proxy. Upstream = `localhost:29092` (Apache Kafka). Listen port = `9092`. Click Start.

The TopBar pill should show `proxy 127.0.0.1:9092 → localhost:29092`.

- [ ] **Step 4: Drive kafkacat**

In a terminal:

```bash
kafkacat -b localhost:9092 -L
```

`-L` issues a Metadata request and prints the broker / topic list.

Expected: kafkacat prints the cluster metadata exactly as if it had talked to the broker directly. (For multi-broker clusters this will fail because the advertised broker address from the broker's response will redirect kafkacat away from our proxy — that's Phase 2's job.)

- [ ] **Step 5: Verify the Protocol tab**

Switch to the Protocol tab in Kapture. You should see (at least) these frames in order:

1. `ApiVersions` (request, send)
2. `ApiVersions` (response, recv)
3. `Metadata` (request, send)
4. `Metadata` (response, recv)

Click any row → the right-hand pane shows the decoded `kafka-protocol` debug output. The `connection_id`-as-`brokerId` field should match across the request and response of the same exchange.

- [ ] **Step 6: Drive a Produce + Fetch**

```bash
echo "hello-from-kafkacat" | kafkacat -b localhost:9092 -P -t kapture-proxy-smoke
kafkacat -b localhost:9092 -C -t kapture-proxy-smoke -e
```

Expected in the Protocol tab: `Produce` request + response, then `FindCoordinator` / `OffsetFetch` / `Fetch` request + response chains. RTT (Recv frames) should be in low single-digit ms for a local broker.

- [ ] **Step 7: Stop the proxy**

In the dialog, click Disconnect. The TopBar should return to disconnected. New kafkacat invocations should fail to connect (`Connection refused`).

- [ ] **Step 8: Commit a manual test note**

Create `docs/specs/proxy-mode-phase-1-smoke.md` with the actual session output (one frame screenshot or text dump showing the API key list captured). Then:

```bash
cd /Users/sderosiaux/code/personal/kapture
git add docs/specs/proxy-mode-phase-1-smoke.md
git commit -m "docs: proxy phase-1 smoke test results vs kafkacat + Apache Kafka 4.2"
```

---

## Task 11: Codex security review (mandatory per project conventions)

Per `kapture-conventions.md`: every major surface addition gets a Codex security review before merging.

**Files:**

- None directly — review is a subagent dispatch + follow-up commits applying the findings.

- [ ] **Step 1: Dispatch Codex review**

Use the `Agent` tool with `subagent_type: "codex:codex-rescue"`. The prompt should include:

- Files to review: `src-tauri/src/proxy.rs`, the `state.rs` diff, the `commands.rs` diff (`start_proxy`, `stop_proxy`), the `error.rs` additions.
- Threat model: an arbitrary process can connect to `127.0.0.1:9092` and have its bytes forwarded to a remote Kafka cluster + observed by Kapture. The user trusts what shows in their Protocol tab — we MUST NOT corrupt or drop bytes silently. We MUST NOT leak SASL credentials in logs (none captured at this phase, but worth verifying defensively).
- Specific concerns: framer max-frame-size DOS, upstream-connect retry behaviour, panics in the pump task that could leak FDs, `unwrap`/`expect` in production paths.
- Output expected: list of findings with severity and concrete fix per file:line.

- [ ] **Step 2: Apply ALL findings**

Per convention, fix all findings — no deferred unless explicit reason. Each fix is its own commit:

```bash
git commit -m "proxy: fix codex finding N: <one-line summary>"
```

- [ ] **Step 3: Re-run the full check**

```bash
cd /Users/sderosiaux/code/personal/kapture
pnpm check
```

Expected: typecheck + eslint + prettier + cargo fmt + clippy (deny pedantic+nursery) + cargo test all pass.

- [ ] **Step 4: Final commit summary**

If any deferred items remain (should be none), document them in `docs/specs/proxy-mode-phase-1-smoke.md` with rationale.

---

## Self-review checklist

**Spec coverage:**

- Phase 1 listener (spec § "Phase 1" item 1) → Task 6
- Per-connection pump (item 2) → Tasks 4 + 6
- LengthDelimitedCodec framer (item 3) → Task 2
- Tap → ProtoFrame (item 4) → Task 5, glue in Task 6
- GUI Mode toggle (item 5) → Task 9
- Disable rdkafka client path when in proxy mode (item 6) → Task 7 (mutual exclusion in AppState) + Task 8 (start_proxy stops capture first)
- Drop / reshape Messages tab (item 7) → DEFERRED to Phase 1.5. Documented above. Phase 1 ships frames in the Protocol tab only, per the spec's "Decision" exit criterion ("kafkacat should show its frames in the Protocol tab").

**Placeholder scan:** No "TBD", "implement later", or vague "add error handling" steps. Every code step has the actual code.

**Type consistency:** `ConnectionId(u64)` defined Task 4, used Task 4-6. `ProxyDirection` defined Task 4, used Tasks 4-5. `RequestHeaderPeek` defined Task 3, used Tasks 3 + 5. `ProtoEvent` is the existing type from `proto_hook.rs` — verified used identically.

**Open question for the implementer:** Step 5 of Task 5 introduces the size-prefix re-prepend. The `ProtoEvent.payload_size` semantics in the existing rdkafka path (`proto_hook.rs:75`) is "true wire size" — measured by librdkafka before the size prefix is consumed. That measurement equals `4 + body.len()`. In our proxy path the `LengthDelimitedCodec` strips the prefix on read, so `frame.len() == body.len()`. To match the rdkafka semantics, set `payload_size = frame.len() + 4`. Update the test assertions to expect `frame.len() + 4`. (Verify against a concrete rdkafka frame in `cargo run --example proto_smoke` if any doubt remains; the existing Protocol tab `size` column shows what you'll match.)

---

---

# Phase 2 — Multi-broker rewrite + verb mapping

**Goal:** A single proxy listener entry point handles a whole multi-broker cluster. The user points `kafkacat -b localhost:9092` at us; we rewrite every response that carries broker / coordinator addresses so the client routes everything back through Kapture, not directly to upstream brokers. Lazy-bind one local listener per upstream broker observed.

**Verbs that carry routable host:port (and must be rewritten):**

| API key | Verb                      | Path                                                       | Notes                                                                                  |
| ------- | ------------------------- | ---------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| 3       | `MetadataResponse`        | `brokers[].host`, `brokers[].port`                         | Drives all client routing — without this rewrite, the client immediately bypasses us.  |
| 10      | `FindCoordinatorResponse` | v0-v3: `host`, `port`. v4+: `coordinators[].host`, `.port` | Group + transaction coordinators. Without rewrite, OffsetCommit / Heartbeat go direct. |
| 60      | `DescribeClusterResponse` | `brokers[].host`, `brokers[].port`                         | Used by AdminClient (`kafka-cluster.sh --describe`, librdkafka admin metadata).        |

All other broker references (Produce/Fetch responses, OffsetCommit, OffsetForLeaderEpoch, LeaderAndIsr, AlterPartitionReassignments, ElectLeaders, DescribeQuorum) carry `node_id` integers only — the client resolves `node_id → host:port` via the cached Metadata response. So once those three verbs are rewritten consistently the cluster works end-to-end through the proxy.

---

## Task 12: Multi-broker docker-compose stack

**Files:**

- Modify: `docker-compose.yml`

- [ ] **Step 1: Append a 3-broker Apache Kafka KRaft profile**

Add to `docker-compose.yml` services (port range 39092-39094 to avoid colliding with the existing single-broker `kafka` on 29092):

```yaml
# ──────────── Apache Kafka KRaft 3-broker (multi-broker proxy tests) ────────────
kafka-mb-1:
  image: apache/kafka:latest
  container_name: kapture-kafka-mb-1
  profiles: ["mb"]
  environment:
    KAFKA_NODE_ID: "1"
    KAFKA_PROCESS_ROLES: "broker,controller"
    KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092,EXTERNAL://0.0.0.0:39092,CONTROLLER://0.0.0.0:9093"
    KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-mb-1:9092,EXTERNAL://localhost:39092"
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
    KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
    KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
    KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-mb-1:9093,2@kafka-mb-2:9093,3@kafka-mb-3:9093"
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "3"
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "3"
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "2"
    KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0"
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    CLUSTER_ID: "kapture-mb-cluster"
  ports:
    - "39092:39092"
  networks: [kafka-mb]

kafka-mb-2:
  image: apache/kafka:latest
  container_name: kapture-kafka-mb-2
  profiles: ["mb"]
  environment:
    KAFKA_NODE_ID: "2"
    KAFKA_PROCESS_ROLES: "broker,controller"
    KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092,EXTERNAL://0.0.0.0:39093,CONTROLLER://0.0.0.0:9093"
    KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-mb-2:9092,EXTERNAL://localhost:39093"
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
    KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
    KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
    KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-mb-1:9093,2@kafka-mb-2:9093,3@kafka-mb-3:9093"
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "3"
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "3"
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "2"
    KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0"
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    CLUSTER_ID: "kapture-mb-cluster"
  ports:
    - "39093:39093"
  networks: [kafka-mb]

kafka-mb-3:
  image: apache/kafka:latest
  container_name: kapture-kafka-mb-3
  profiles: ["mb"]
  environment:
    KAFKA_NODE_ID: "3"
    KAFKA_PROCESS_ROLES: "broker,controller"
    KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092,EXTERNAL://0.0.0.0:39094,CONTROLLER://0.0.0.0:9093"
    KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-mb-3:9092,EXTERNAL://localhost:39094"
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
    KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
    KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
    KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-mb-1:9093,2@kafka-mb-2:9093,3@kafka-mb-3:9093"
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "3"
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "3"
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "2"
    KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0"
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    CLUSTER_ID: "kapture-mb-cluster"
  ports:
    - "39094:39094"
  networks: [kafka-mb]
```

And add to `networks:` block:

```yaml
kafka-mb:
  name: kapture-kafka-mb
```

Add to `package.json` scripts:

```json
"stack:up:mb": "docker compose --profile mb up -d",
"stack:down:mb": "docker compose --profile mb down"
```

- [ ] **Step 2: Bring up the stack and verify**

```bash
cd /Users/sderosiaux/code/personal/kapture
pnpm stack:up:mb
# Wait for healthy startup
sleep 15
kafkacat -b localhost:39092 -L | head -30
```

Expected: list of 3 brokers, IDs 1/2/3, advertised as `localhost:39092` / `:39093` / `:39094`.

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml package.json
git commit -m "compose: 3-broker Apache Kafka KRaft profile (mb) for proxy tests"
```

---

## Task 13: BrokerMap — port pool for upstream brokers

**Files:**

- Modify: `src-tauri/src/proxy.rs`

- [ ] **Step 1: Write the failing test**

Append to `proxy.rs` `mod tests`:

```rust
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
        let local = map.ensure_listener("upstream.example.com", 9092).await.unwrap();
        let upstream = map.upstream_for_local(local).unwrap();
        assert_eq!(upstream, ("upstream.example.com".to_owned(), 9092));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd src-tauri && cargo test --lib proxy::tests::broker_map 2>&1 | tail -10`
Expected: FAIL — `BrokerMap` undefined.

- [ ] **Step 3: Implement BrokerMap**

Append to `proxy.rs`:

```rust
/// Map between upstream Kafka brokers `(host, port)` and the local
/// loopback ports we've bound for them. The first entry is the
/// bootstrap broker the user configured; subsequent entries are
/// lazily added as Metadata / FindCoordinator / DescribeCluster
/// responses reveal new brokers.
///
/// Bidirectional: `ensure_listener(host, port)` allocates (or returns
/// the cached) local port. `upstream_for_local(local)` is used by
/// the per-listener pump to know where to forward bytes to.
#[derive(Debug, Default)]
pub struct BrokerMap {
    inner: parking_lot::RwLock<BrokerMapInner>,
}

#[derive(Debug, Default)]
struct BrokerMapInner {
    by_upstream: HashMap<(String, u16), u16>,
    by_local: HashMap<u16, (String, u16)>,
}

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
        let mut inner = self.inner.write();
        inner.by_upstream.insert((host.to_owned(), port), local_port);
        inner.by_local.insert(local_port, (host.to_owned(), port));
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd src-tauri && cargo test --lib proxy::tests::broker_map 2>&1 | tail -10`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src-tauri/src/proxy.rs
git commit -m "proxy: BrokerMap — bidirectional upstream↔local port mapping"
```

---

## Task 14: Response rewriter — Metadata, FindCoordinator, DescribeCluster

We decode the response with the right `(api_key, api_version)` from the `CorrelationMap`, mutate broker / coordinator host:port to point at our local listeners, re-encode, return new bytes. For verbs we don't rewrite, return `None` (the pump forwards the original bytes verbatim).

**Files:**

- Create: `src-tauri/src/proxy_rewrite.rs`
- Modify: `src-tauri/src/proxy.rs` (declare and use)

- [ ] **Step 1: Write the failing test**

Create `src-tauri/src/proxy_rewrite.rs`:

```rust
//! Rewrite Kafka responses that carry broker / coordinator host:port
//! so a client routing on those addresses comes back through Kapture
//! instead of bypassing us.
//!
//! Three verbs need rewriting (see `docs/specs/proxy-mode.md` and
//! the plan for Phase 2):
//!   - MetadataResponse        (api key 3)
//!   - FindCoordinatorResponse (api key 10)
//!   - DescribeClusterResponse (api key 60)
//!
//! All other responses are forwarded verbatim — they reference
//! brokers by `node_id` only and the client resolves the address
//! via the (already-rewritten) Metadata cache.

#![allow(clippy::wildcard_imports)]

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

use crate::proxy::BrokerMap;

/// Try to rewrite a response frame body so its broker / coordinator
/// addresses point at our local proxy listeners.
///
/// `frame` is the raw response *body* as it came off the wire,
/// **without** the 4-byte length prefix (the codec already stripped
/// it). The first 4 bytes are the correlation_id, followed by the
/// optional response-header tagged fields, followed by the response
/// payload.
///
/// Returns `Ok(Some(rewritten_bytes))` on a successful rewrite,
/// `Ok(None)` if the API doesn't need rewriting or the buffer was
/// untouched, `Err(_)` on decode/encode failure (caller logs and
/// forwards verbatim — never silently drop frames).
pub async fn rewrite_response(
    api_key: i16,
    api_version: i16,
    frame: &[u8],
    broker_map: &BrokerMap,
) -> Result<Option<Bytes>, RewriteError> {
    let api = ApiKey::try_from(api_key).map_err(|_| RewriteError::UnknownApiKey(api_key))?;
    match api {
        ApiKey::Metadata => rewrite_metadata(api_version, frame, broker_map).await,
        ApiKey::FindCoordinator => rewrite_find_coordinator(api_version, frame, broker_map).await,
        ApiKey::DescribeCluster => rewrite_describe_cluster(api_version, frame, broker_map).await,
        _ => Ok(None),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RewriteError {
    #[error("decode failed: {0}")]
    Decode(String),
    #[error("encode failed: {0}")]
    Encode(String),
    #[error("listener bind failed for {host}:{port}: {err}")]
    Bind { host: String, port: u16, err: std::io::Error },
    #[error("unknown api key: {0}")]
    UnknownApiKey(i16),
}

async fn rewrite_metadata(
    version: i16,
    frame: &[u8],
    broker_map: &BrokerMap,
) -> Result<Option<Bytes>, RewriteError> {
    let mut buf = Bytes::copy_from_slice(frame);
    let header_version = ApiKey::Metadata.response_header_version(version);
    let _hdr = ResponseHeader::decode(&mut buf, header_version)
        .map_err(|e| RewriteError::Decode(format!("metadata header: {e}")))?;
    let mut resp = MetadataResponse::decode(&mut buf, version)
        .map_err(|e| RewriteError::Decode(format!("metadata body: {e}")))?;

    for broker in &mut resp.brokers {
        let host = broker.host.to_string();
        let port = u16::try_from(broker.port).unwrap_or(0);
        if port == 0 {
            continue;
        }
        let local = broker_map.ensure_listener(&host, port).await
            .map_err(|err| RewriteError::Bind { host: host.clone(), port, err })?;
        broker.host = StrBytes::from_string("127.0.0.1".to_owned());
        broker.port = i32::from(local);
    }

    encode_response(version, &resp, ApiKey::Metadata)
}

async fn rewrite_find_coordinator(
    version: i16,
    frame: &[u8],
    broker_map: &BrokerMap,
) -> Result<Option<Bytes>, RewriteError> {
    let mut buf = Bytes::copy_from_slice(frame);
    let header_version = ApiKey::FindCoordinator.response_header_version(version);
    let _hdr = ResponseHeader::decode(&mut buf, header_version)
        .map_err(|e| RewriteError::Decode(format!("find_coord header: {e}")))?;
    let mut resp = FindCoordinatorResponse::decode(&mut buf, version)
        .map_err(|e| RewriteError::Decode(format!("find_coord body: {e}")))?;

    if version <= 3 {
        // Single coordinator at top level.
        let host = resp.host.to_string();
        let port = u16::try_from(resp.port).unwrap_or(0);
        if port != 0 {
            let local = broker_map.ensure_listener(&host, port).await
                .map_err(|err| RewriteError::Bind { host: host.clone(), port, err })?;
            resp.host = StrBytes::from_string("127.0.0.1".to_owned());
            resp.port = i32::from(local);
        }
    } else {
        for c in &mut resp.coordinators {
            let host = c.host.to_string();
            let port = u16::try_from(c.port).unwrap_or(0);
            if port == 0 {
                continue;
            }
            let local = broker_map.ensure_listener(&host, port).await
                .map_err(|err| RewriteError::Bind { host: host.clone(), port, err })?;
            c.host = StrBytes::from_string("127.0.0.1".to_owned());
            c.port = i32::from(local);
        }
    }

    encode_response(version, &resp, ApiKey::FindCoordinator)
}

async fn rewrite_describe_cluster(
    version: i16,
    frame: &[u8],
    broker_map: &BrokerMap,
) -> Result<Option<Bytes>, RewriteError> {
    let mut buf = Bytes::copy_from_slice(frame);
    let header_version = ApiKey::DescribeCluster.response_header_version(version);
    let _hdr = ResponseHeader::decode(&mut buf, header_version)
        .map_err(|e| RewriteError::Decode(format!("describe_cluster header: {e}")))?;
    let mut resp = DescribeClusterResponse::decode(&mut buf, version)
        .map_err(|e| RewriteError::Decode(format!("describe_cluster body: {e}")))?;

    for b in &mut resp.brokers {
        let host = b.host.to_string();
        let port = u16::try_from(b.port).unwrap_or(0);
        if port == 0 {
            continue;
        }
        let local = broker_map.ensure_listener(&host, port).await
            .map_err(|err| RewriteError::Bind { host: host.clone(), port, err })?;
        b.host = StrBytes::from_string("127.0.0.1".to_owned());
        b.port = i32::from(local);
    }

    encode_response(version, &resp, ApiKey::DescribeCluster)
}

fn encode_response<T: Encodable>(
    version: i16,
    msg: &T,
    api: ApiKey,
) -> Result<Option<Bytes>, RewriteError> {
    let header_version = api.response_header_version(version);
    let mut out = BytesMut::with_capacity(256);
    // ResponseHeader: just the correlation_id and (in flexible
    // versions) tagged fields. We zero corr_id here because the
    // caller will overwrite the first 4 bytes with the real
    // correlation_id before forwarding — that way we don't have to
    // round-trip the header through the body decode.
    let header = ResponseHeader::default();
    header.encode(&mut out, header_version)
        .map_err(|e| RewriteError::Encode(format!("header: {e}")))?;
    msg.encode(&mut out, version)
        .map_err(|e| RewriteError::Encode(format!("body: {e}")))?;
    Ok(Some(out.freeze()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use kafka_protocol::messages::metadata_response::MetadataResponseBroker;

    fn build_metadata_response_bytes(version: i16, brokers: Vec<(i32, &str, i32)>) -> Vec<u8> {
        let mut resp = MetadataResponse::default();
        resp.brokers = brokers
            .into_iter()
            .map(|(node_id, host, port)| {
                let mut b = MetadataResponseBroker::default();
                b.node_id = kafka_protocol::messages::BrokerId(node_id);
                b.host = StrBytes::from_string(host.to_owned());
                b.port = port;
                b
            })
            .collect();

        let header_version = ApiKey::Metadata.response_header_version(version);
        let mut out = BytesMut::new();
        ResponseHeader::default().encode(&mut out, header_version).unwrap();
        resp.encode(&mut out, version).unwrap();
        out.to_vec()
    }

    #[tokio::test]
    async fn rewrites_metadata_brokers_to_local_listeners() {
        let map = BrokerMap::new();
        // v12 is a flexible version, exercises tagged fields.
        let bytes = build_metadata_response_bytes(
            12,
            vec![
                (1, "kafka-mb-1.local", 39092),
                (2, "kafka-mb-2.local", 39093),
                (3, "kafka-mb-3.local", 39094),
            ],
        );
        let rewritten = rewrite_response(3, 12, &bytes, &map).await.unwrap().unwrap();

        // Decode the rewritten bytes and verify each broker host is now 127.0.0.1.
        let mut buf = rewritten;
        let header_version = ApiKey::Metadata.response_header_version(12);
        let _hdr = ResponseHeader::decode(&mut buf, header_version).unwrap();
        let resp = MetadataResponse::decode(&mut buf, 12).unwrap();
        for b in &resp.brokers {
            assert_eq!(b.host.to_string(), "127.0.0.1");
            // Port is the local listener port — must be non-zero
            // and present in the broker map under the original.
            assert!(b.port > 0);
        }
        // Map must contain 3 distinct entries.
        let snapshot = map.snapshot();
        assert_eq!(snapshot.len(), 3);
    }

    #[tokio::test]
    async fn passes_unknown_api_through() {
        let map = BrokerMap::new();
        // Produce response (api key 0) — not in our rewrite set.
        let result = rewrite_response(0, 9, &[0u8; 16], &map).await.unwrap();
        assert!(result.is_none());
    }
}
```

- [ ] **Step 2: Wire the module**

In `src-tauri/src/lib.rs`, add `mod proxy_rewrite;` next to `mod proxy;`.

- [ ] **Step 3: Run the tests**

Run: `cd src-tauri && cargo test --lib proxy_rewrite 2>&1 | tail -20`
Expected: 2 passed.

If the tests fail because `kafka-protocol` 0.16 doesn't compile encode for these types, check the crate API: the message types implement `Encodable` and `Decodable`. The exact `StrBytes::from_string` signature may differ — adjust to `StrBytes::from_str` or `StrBytes::from_static` based on the actual signature in the version we depend on. Adjust types accordingly. The principle stands; if the encode round-trips lose tagged fields, we may need to copy them across.

- [ ] **Step 4: Commit**

```bash
git add src-tauri/src/proxy_rewrite.rs src-tauri/src/lib.rs
git commit -m "proxy: response rewriter for Metadata / FindCoordinator / DescribeCluster"
```

---

## Task 15: Wire response rewriting into the pump

Update the per-connection pump so on every UpstreamToClient frame, it consults the corresponding request in `CorrelationMap` for `(api_key, api_version)`, calls `proxy_rewrite::rewrite_response`, and forwards either the rewritten bytes (with the original 4-byte correlation_id preserved) or the original verbatim.

**Files:**

- Modify: `src-tauri/src/proxy.rs`
- Test: `src-tauri/src/proxy.rs`

- [ ] **Step 1: Replace the simple `run_pump` callback with a richer one**

Replace the `tap` callback signature with `FrameHandler` that returns the bytes to forward. Pseudocode:

```rust
pub async fn run_pump_with_rewrite(
    conn_id: ConnectionId,
    client: TcpStream,
    upstream: TcpStream,
    correlator: Arc<ProtoCorrelator>,
    corr_map: Arc<CorrelationMap>,
    broker_map: Arc<BrokerMap>,
) -> io::Result<()> {
    let mut client_framed = framed_kafka(client);
    let mut upstream_framed = framed_kafka(upstream);

    loop {
        tokio::select! {
            frame = client_framed.next() => {
                let Some(frame) = frame else { break; };
                let frame = frame?;
                let bytes = frame.freeze();
                let event = build_proto_event(
                    ProxyDirection::ClientToUpstream,
                    conn_id,
                    &bytes,
                    &corr_map,
                );
                correlator.record_event(&event);
                upstream_framed.send(bytes).await?;
            }
            frame = upstream_framed.next() => {
                let Some(frame) = frame else { break; };
                let frame = frame?;
                let bytes = frame.freeze();
                let event = build_proto_event(
                    ProxyDirection::UpstreamToClient,
                    conn_id,
                    &bytes,
                    &corr_map,
                );
                let api_key = i16::try_from(event.api_key).unwrap_or(-1);
                let api_version = i16::try_from(event.api_version).unwrap_or(-1);
                correlator.record_event(&event);

                let forward = if api_key >= 0 {
                    match crate::proxy_rewrite::rewrite_response(
                        api_key, api_version, &bytes, &broker_map,
                    ).await {
                        Ok(Some(mut rewritten)) => {
                            // Splice the original correlation_id back in.
                            // The rewriter encoded a fresh ResponseHeader
                            // with corr_id=0; replace the first 4 bytes.
                            if rewritten.len() >= 4 && bytes.len() >= 4 {
                                let mut buf = BytesMut::from(rewritten.as_ref());
                                buf[0..4].copy_from_slice(&bytes[0..4]);
                                buf.freeze()
                            } else {
                                bytes.clone()
                            }
                        }
                        Ok(None) => bytes.clone(),
                        Err(err) => {
                            warn!(error = %err, "rewrite failed; forwarding verbatim");
                            bytes.clone()
                        }
                    }
                } else {
                    bytes.clone()
                };
                client_framed.send(forward).await?;
            }
        }
    }
    Ok(())
}
```

- [ ] **Step 2: Test against a stub upstream that emits a Metadata response**

```rust
    #[tokio::test]
    async fn pump_rewrites_metadata_response_brokers_to_local() {
        // Fake upstream: when a client sends ANY frame, reply with a
        // pre-built Metadata response that advertises 3 distant brokers.
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();

        let upstream_task = tokio::spawn(async move {
            let (sock, _) = upstream.accept().await.unwrap();
            let mut framed = framed_kafka(sock);
            // Read one request frame from the client.
            let _req = framed.next().await.unwrap().unwrap();
            // Send a Metadata v12 response.
            let body = build_metadata_response_bytes(
                12,
                vec![(1, "kafka-mb-1", 39092), (2, "kafka-mb-2", 39093)],
            );
            // Splice the corr_id=42 from the (fake) request.
            let mut buf = BytesMut::from(&body[..]);
            buf[0..4].copy_from_slice(&42i32.to_be_bytes());
            framed.send(buf.freeze()).await.unwrap();
        });

        // Client side: connect through our pump.
        let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client_addr = client_listener.local_addr().unwrap();
        let upstream_target = upstream_addr.to_string();
        let correlator = Arc::new(crate::correlator::ProtoCorrelator::new());
        let corr_map = Arc::new(CorrelationMap::default());
        let broker_map = Arc::new(BrokerMap::new());
        let correlator_for_test = Arc::clone(&correlator);
        let broker_map_for_test = Arc::clone(&broker_map);

        let pump_task = tokio::spawn(async move {
            let (client_sock, _) = client_listener.accept().await.unwrap();
            let upstream_sock = TcpStream::connect(upstream_target).await.unwrap();
            run_pump_with_rewrite(
                ConnectionId(1),
                client_sock,
                upstream_sock,
                correlator,
                corr_map,
                broker_map,
            ).await.unwrap();
        });

        // Drive the client. Send a Metadata v12 request (api_key=3,
        // api_ver=12, corr_id=42, then dummy header tail).
        let mut client = TcpStream::connect(client_addr).await.unwrap();
        let mut req = Vec::new();
        req.extend_from_slice(&3i16.to_be_bytes());
        req.extend_from_slice(&12i16.to_be_bytes());
        req.extend_from_slice(&42i32.to_be_bytes());
        // client_id (nullable string, length=-1) + tagged fields=0
        req.extend_from_slice(&(-1i16).to_be_bytes());
        req.push(0); // tagged fields count = 0
        // Empty MetadataRequest body (topics array null + tagged fields).
        req.push(0xFF); // null array marker for v12 flexible
        req.push(0); // tagged fields
        let len = u32::try_from(req.len()).unwrap();
        client.write_all(&len.to_be_bytes()).await.unwrap();
        client.write_all(&req).await.unwrap();

        // Read the rewritten response.
        let mut framed_client = framed_kafka(client);
        use futures::StreamExt;
        let resp = framed_client.next().await.unwrap().unwrap();
        let mut buf = resp.freeze();
        // First 4 bytes should be corr_id=42.
        let corr_id = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(corr_id, 42);
        // Decode and verify brokers were rewritten.
        let header_version = ApiKey::Metadata.response_header_version(12);
        let _hdr = ResponseHeader::decode(&mut buf, header_version).unwrap();
        let decoded = MetadataResponse::decode(&mut buf, 12).unwrap();
        for b in &decoded.brokers {
            assert_eq!(b.host.to_string(), "127.0.0.1");
            assert!(b.port > 0 && b.port < 65536);
        }
        // BrokerMap should now hold both upstream entries.
        assert_eq!(broker_map_for_test.snapshot().len(), 2);
        // Correlator should have recorded request + response.
        assert!(correlator_for_test.summaries(10).len() >= 2);

        upstream_task.await.unwrap();
        pump_task.abort();
    }
```

- [ ] **Step 3: Run tests + clippy**

Run: `cd src-tauri && cargo test --lib proxy 2>&1 | tail -20 && cargo clippy --all-targets --message-format=short 2>&1 | tail -20`
Expected: all proxy tests pass, no clippy warnings.

- [ ] **Step 4: Commit**

```bash
git add src-tauri/src/proxy.rs
git commit -m "proxy: pump rewrites Metadata/FindCoord/DescribeCluster responses"
```

---

## Task 16: Lazy-bind listeners — one accept loop per upstream broker

When a Metadata response reveals a new broker, the rewriter has already allocated a local port via `BrokerMap::ensure_listener`. Now we need an actual accept loop bound to that port that forwards to the right upstream.

**Files:**

- Modify: `src-tauri/src/proxy.rs`

- [ ] **Step 1: Refactor `ProxyHandle` to support multiple listeners**

Change `ProxyHandle::accept_task: Option<JoinHandle<()>>` to `accept_tasks: Vec<JoinHandle<()>>` (or a `JoinSet`). On `start`, bind the bootstrap listener AND seed the broker map with `(upstream, listen_port)` so subsequent rewrites map the same upstream back to the same local port.

Add a method `ensure_listener_running(&self, local_port: u16)` that, given a `local_port` already reserved in `BrokerMap`, spawns an accept loop for it (idempotent — bail if already running).

Concretely:

```rust
struct ListenerSlot {
    task: JoinHandle<()>,
    local_addr: SocketAddr,
}

pub struct ProxyHandle {
    stop_tx: watch::Sender<bool>,
    listeners: Mutex<HashMap<u16, ListenerSlot>>,
    bootstrap_addr: SocketAddr,
    bootstrap_upstream: String,
    correlator: Arc<ProtoCorrelator>,
    broker_map: Arc<BrokerMap>,
}
```

Provide `ProxyHandle::start(config, correlator) -> io::Result<Self>` that:

1. Reserves the bootstrap entry in the broker_map: parse `config.upstream` into `(host, port)`, call `broker_map.reserve(host.clone(), port, config.listen_port)`.
2. Binds the bootstrap listener.
3. Spawns the bootstrap accept loop (which uses `run_pump_with_rewrite`).
4. Hooks the rewriter to call `ensure_listener_running` after every `BrokerMap::ensure_listener` call. **The cleanest way**: instead of having the rewriter call broker_map directly, have it call a `BrokerProvisioner` trait that the proxy owns; the proxy's impl binds + spawns the accept loop + records in broker_map atomically.

Sketch:

```rust
#[async_trait::async_trait]
pub trait BrokerProvisioner: Send + Sync {
    async fn ensure(&self, host: &str, port: u16) -> io::Result<u16>;
}

// proxy_rewrite::rewrite_response now takes &dyn BrokerProvisioner instead of &BrokerMap.

impl BrokerProvisioner for ProxyHandle {
    async fn ensure(&self, host: &str, port: u16) -> io::Result<u16> {
        // Idempotent under the inner Mutex: check broker_map, if already
        // bound just return; else bind, spawn the accept loop, record.
        // ...
    }
}
```

Add `async-trait = "0.1"` to `Cargo.toml` if not already there.

This is the largest single piece of code in Phase 2 — read the existing `ProxyHandle::start` carefully and write the rebinding step by step.

- [ ] **Step 2: Test the multi-listener flow**

Test name: `proxy_handle_provisions_a_listener_per_upstream_broker_observed`. Setup: a stub upstream (the bootstrap broker) that on first request emits a Metadata response advertising 2 brokers (itself + a fake second broker on 127.0.0.1:0 — pick any free port). After the response is rewritten, assert:

- `broker_map.snapshot().len() == 2`
- A new TCP listener is reachable at the local port the rewriter assigned for the second broker.

- [ ] **Step 3: Run all proxy tests + lints**

Run: `cd src-tauri && cargo test --lib proxy 2>&1 | tail -20 && cargo clippy --all-targets --message-format=short 2>&1 | tail -20`
Expected: all pass, no warnings.

- [ ] **Step 4: Commit**

```bash
git add src-tauri/src/proxy.rs src-tauri/src/proxy_rewrite.rs src-tauri/Cargo.toml
git commit -m "proxy: lazy-bind one listener per upstream broker observed"
```

---

## Task 17: End-to-end multi-broker smoke test

**Files:**

- Modify: `docs/specs/proxy-mode-phase-1-smoke.md` (extend with Phase 2 results)

- [ ] **Step 1: Bring up the 3-broker stack**

```bash
cd /Users/sderosiaux/code/personal/kapture
pnpm stack:up:mb
sleep 15
kafkacat -b localhost:39092 -L | head -30  # Verify 3 brokers visible
```

- [ ] **Step 2: Configure the proxy**

In Kapture: Mode = Proxy, Upstream = `localhost:39092`, Listen port = `9092`, Start.

- [ ] **Step 3: Drive a multi-broker workload**

```bash
kafkacat -b localhost:9092 -L                # Metadata via proxy
echo "k1:hello-1" | kafkacat -b localhost:9092 -P -t mb-test -K:
echo "k2:hello-2" | kafkacat -b localhost:9092 -P -t mb-test -K:
echo "k3:hello-3" | kafkacat -b localhost:9092 -P -t mb-test -K:
kafkacat -b localhost:9092 -C -t mb-test -e
```

- [ ] **Step 4: Verify the Protocol tab**

Expected sequence of frames in Kapture:

1. Bootstrap connection (conn_id=1): ApiVersions, Metadata
2. After Metadata rewrite, kafkacat opens connections to the rewritten broker addresses (still `127.0.0.1` but on the lazy-allocated ports). Each becomes a new conn_id (2, 3, 4).
3. Per-broker traffic: Produce on the partition leader's broker, Fetch on the same.
4. Consumer group flow: FindCoordinator → JoinGroup → SyncGroup → OffsetFetch → Fetch → OffsetCommit.

The Protocol tab `brokerId` column (which carries `connection_id` in proxy mode) lets us distinguish per-broker conversations.

- [ ] **Step 5: Document results**

Append to `docs/specs/proxy-mode-phase-1-smoke.md` the actual frame sequence (text dump of the Protocol tab over 10 s) and the broker-map snapshot. Commit:

```bash
git add docs/specs/proxy-mode-phase-1-smoke.md
git commit -m "docs: phase-2 multi-broker smoke results (3-broker KRaft)"
```

---

## Task 18: Codex security review (Phase 2)

Same protocol as Task 11 — dispatch the codex security review against the Phase 2 code. Pay specific attention to:

- Decode-error → forward-verbatim path. Ensure a malicious upstream can't make us spin / panic by crafting a malformed response we partially parse.
- BrokerMap unbounded growth. If a broker advertises thousands of brokers, do we cap?
- The `127.0.0.1:0` rebind dance has a TOCTOU window where another process can grab the port. Document the residual risk; mitigation is "user runs Kapture in a controlled localhost environment".
- The `correlator` records the **original** response bytes, not the rewritten ones. That's deliberate — Wireshark-style "show me what was on the wire" — but document it explicitly.

Apply all findings as separate commits. Final `pnpm check` must pass.

---

## Plan complete — execution choice

Two execution options:

**1. Subagent-Driven (recommended):** I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution:** Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
