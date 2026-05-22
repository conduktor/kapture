//! Unit tests for `jvm_tap.rs`. Lifted out to keep the production
//! module under the 1000-line file budget. Re-includes the parent
//! module via `use super::*` so test helpers and internal items
//! stay reachable without exporting them publicly.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

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

/// `process_payload` must reject NEW `agent_conn_id`s once the
/// per-session cap is reached. Existing entries are unaffected —
/// LRU-evicting an in-progress reassembly buffer would corrupt
/// the stream, so the policy is "drop new conns, keep tracked
/// ones working".
///
/// We exercise `process_payload` directly here rather than going
/// through the UDS round-trip: the cap is 4096, simulating that
/// many concurrent UDS frames would be slow and fragile.
#[tokio::test]
async fn per_session_conn_cap_drops_new_agent_conn_ids_past_the_limit() {
    let correlator = Arc::new(ProtoCorrelator::new());
    let mut session = AgentSession::new();
    // Seed the session up to the cap with empty entries.
    let frame = make_api_versions_request_frame(7);
    for i in 0..u32::try_from(MAX_AGENT_CONN_IDS_PER_SESSION).unwrap() {
        process_payload(&mut session, &correlator, i, 0, &frame).unwrap();
    }
    assert_eq!(session.conn_ids.len(), MAX_AGENT_CONN_IDS_PER_SESSION);
    let frames_at_cap = correlator.frame_count();
    // One MORE agent_conn_id past the cap — must be dropped.
    let beyond = u32::try_from(MAX_AGENT_CONN_IDS_PER_SESSION).unwrap();
    process_payload(&mut session, &correlator, beyond, 0, &frame).unwrap();
    assert_eq!(
        session.conn_ids.len(),
        MAX_AGENT_CONN_IDS_PER_SESSION,
        "conn cap must not grow past the limit"
    );
    assert_eq!(
        correlator.frame_count(),
        frames_at_cap,
        "frame from beyond-cap agent_conn_id must be dropped"
    );
    // An EXISTING agent_conn_id must still work after we hit the cap.
    let existing = 42u32;
    let before = correlator.frame_count();
    process_payload(&mut session, &correlator, existing, 0, &frame).unwrap();
    assert_eq!(
        correlator.frame_count(),
        before + 1,
        "existing agent_conn_id must keep flowing after the cap is hit"
    );
}

/// After a single large Kafka frame drains the reassembly buffer
/// to empty, the underlying `BytesMut` allocation must be
/// reclaimed — `split_to(at).freeze()` advances the buffer
/// pointer but keeps the same backing alloc, so without the
/// shrink step we'd pin peak-sized allocations for the life of
/// the entry. We send a ~512 KiB Kafka frame, then check that
/// `process_payload` left the buffer with a tiny capacity.
#[tokio::test]
async fn reassembly_buffer_capacity_shrinks_after_large_frame_drains() {
    let correlator = Arc::new(ProtoCorrelator::new());
    let mut session = AgentSession::new();

    // 512 KiB body — well above the 64 KiB shrink threshold.
    let body_size = 512 * 1024;
    let mut frame = Vec::with_capacity(4 + body_size);
    frame.extend_from_slice(&u32::try_from(body_size).unwrap().to_be_bytes());
    // Body content is a valid-looking ApiVersions header followed
    // by zero padding so build_proto_event doesn't reject it.
    frame.extend_from_slice(&18i16.to_be_bytes()); // ApiVersions
    frame.extend_from_slice(&3i16.to_be_bytes()); // v3
    frame.extend_from_slice(&99i32.to_be_bytes()); // corr_id
    frame.extend_from_slice(&(-1i16).to_be_bytes()); // null client_id
    frame.extend_from_slice(&vec![0u8; body_size - 10]);

    process_payload(&mut session, &correlator, 9, 0, &frame).unwrap();
    assert_eq!(correlator.frame_count(), 1, "frame should have decoded");

    let buf = session
        .buffers
        .get(&(9, 0))
        .expect("buffer entry should exist");
    assert!(
        buf.is_empty(),
        "after extracting the only frame, buffer must be empty"
    );
    assert!(
        buf.capacity() <= REASSEMBLY_BUFFER_SHRINK_THRESHOLD,
        "expected buffer capacity to be reclaimed to <= {} after large frame drained, got {}",
        REASSEMBLY_BUFFER_SHRINK_THRESHOLD,
        buf.capacity()
    );
}
