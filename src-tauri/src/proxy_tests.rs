use super::*;
use bytes::BytesMut;
use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
use kafka_protocol::messages::{ApiKey, BrokerId, MetadataResponse, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use parking_lot::Mutex as PMutex;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

/// Local copy of the `proxy_rewrite::tests` helper. Duplicated
/// rather than re-exported so production code stays free of test
/// fixtures.
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

#[test]
fn proxy_config_normalises_listen_addr() {
    let cfg = ProxyConfig::new("upstream:9092".to_owned(), 9092);
    assert_eq!(cfg.upstream, "upstream:9092");
    assert_eq!(cfg.listen_addr().to_string(), "127.0.0.1:9092");
}

#[tokio::test]
async fn frame_codec_decodes_length_prefixed_payloads() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let mut framed = framed_kafka(sock);
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

#[test]
fn correlation_map_pairs_request_and_response() {
    let map = CorrelationMap::default();
    map.record_request(
        42,
        RequestHeaderPeek {
            api_key: 1,
            api_version: 13,
            corr_id: 42,
        },
    )
    .unwrap();
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

#[test]
fn correlation_map_rejects_unbounded_in_flight_requests() {
    let map = CorrelationMap::default();
    for corr_id in 0..MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION {
        map.record_request(
            i32::try_from(corr_id).unwrap(),
            RequestHeaderPeek {
                api_key: 3,
                api_version: 12,
                corr_id: i32::try_from(corr_id).unwrap(),
            },
        )
        .unwrap();
    }

    let result = map.record_request(
        99_999,
        RequestHeaderPeek {
            api_key: 3,
            api_version: 12,
            corr_id: 99_999,
        },
    );
    assert!(result.is_err());
}

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
        9092,
        &frame,
        &map,
    )
    .unwrap();

    assert!(matches!(
        event.direction,
        crate::proto_event::ProtoDirection::Send
    ));
    assert_eq!(event.api_key, 18);
    assert_eq!(event.api_version, 3);
    assert_eq!(event.corr_id, 99);
    assert_eq!(event.connection_id, 7);
    assert_eq!(event.local_port, 9092);
    assert_eq!(event.payload_size, frame.len() + 4);
    let body_len = i32::try_from(frame.len()).unwrap();
    assert_eq!(&event.payload[..4], &body_len.to_be_bytes());
    assert_eq!(&event.payload[4..], &frame[..]);
    assert!(event.rtt_ms == 0.0);
    // Map now holds an entry for corr_id 99.
    assert!(map.take_response(99).is_some());
}

#[test]
fn build_proto_event_for_response_resolves_from_map() {
    let map = CorrelationMap::default();
    map.record_request(
        42,
        RequestHeaderPeek {
            api_key: 1,
            api_version: 13,
            corr_id: 42,
        },
    )
    .unwrap();
    // Response wire prefix: corr_id (i32 BE) at offset 0.
    let mut frame = Vec::new();
    frame.extend_from_slice(&42i32.to_be_bytes());
    frame.extend_from_slice(b"....body....");

    let event = build_proto_event(
        ProxyDirection::UpstreamToClient,
        ConnectionId(7),
        9093,
        &frame,
        &map,
    )
    .unwrap();

    assert!(matches!(
        event.direction,
        crate::proto_event::ProtoDirection::Recv
    ));
    assert_eq!(event.api_key, 1);
    assert_eq!(event.api_version, 13);
    assert_eq!(event.corr_id, 42);
    assert_eq!(event.connection_id, 7);
    assert_eq!(event.local_port, 9093);
    assert_eq!(event.payload_size, frame.len() + 4);
    let body_len = i32::try_from(frame.len()).unwrap();
    assert_eq!(&event.payload[..4], &body_len.to_be_bytes());
    assert_eq!(&event.payload[4..], &frame[..]);
    assert!(event.rtt_ms >= 0.0);
}

#[test]
fn build_proto_event_redacts_sasl_authenticate_request_payload() {
    // Build a SaslAuthenticate (api_key=36) v2 request frame whose
    // body carries a PLAIN credential. Neutral fixture string —
    // never reuse Phase 3 docker-compose credentials in test
    // sources (codex v2 LOW finding 5).
    let map = CorrelationMap::default();
    let secret = b"\0fixture-user\0fixture-secret";
    let mut frame = Vec::new();
    frame.extend_from_slice(&36i16.to_be_bytes()); // api_key
    frame.extend_from_slice(&2i16.to_be_bytes()); // api_version
    frame.extend_from_slice(&77i32.to_be_bytes()); // corr_id
    frame.extend_from_slice(&(-1i16).to_be_bytes()); // client_id null
    frame.push(0); // tagged fields
    frame.extend_from_slice(secret);

    // Sanity: the raw frame contains the credential.
    assert!(frame.windows(secret.len()).any(|w| w == secret));

    let event = build_proto_event(
        ProxyDirection::ClientToUpstream,
        ConnectionId(11),
        0,
        &frame,
        &map,
    )
    .unwrap();
    assert_eq!(event.api_key, API_KEY_SASL_AUTHENTICATE);

    // The inspector copy must not contain the credential anywhere.
    assert!(
        !event.payload.windows(secret.len()).any(|w| w == secret),
        "SaslAuthenticate inspector payload must not contain raw credential bytes",
    );
    assert!(!event
        .payload
        .windows(b"fixture-secret".len())
        .any(|w| w == b"fixture-secret"));
}

#[test]
fn build_proto_event_does_not_redact_other_request_payloads() {
    // Non-SASL requests must still carry their full payload — only
    // SaslAuthenticate (api_key 36) is redacted.
    let map = CorrelationMap::default();
    let mut frame = Vec::new();
    frame.extend_from_slice(&3i16.to_be_bytes()); // api_key=Metadata
    frame.extend_from_slice(&12i16.to_be_bytes()); // version
    frame.extend_from_slice(&5i32.to_be_bytes()); // corr_id
    frame.extend_from_slice(b"sentinel-payload");

    let event = build_proto_event(
        ProxyDirection::ClientToUpstream,
        ConnectionId(12),
        0,
        &frame,
        &map,
    )
    .unwrap();
    // Payload still contains the sentinel — no redaction applied.
    assert!(event
        .payload
        .windows(b"sentinel-payload".len())
        .any(|w| w == b"sentinel-payload"));
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
        0,
        &frame,
        &map,
    )
    .unwrap();

    assert_eq!(event.api_key, -1);
    assert_eq!(event.api_version, -1);
    assert_eq!(event.corr_id, 404);
    assert_eq!(event.payload_size, frame.len() + 4);
}

/// End-to-end: spin up a fake upstream broker that echoes each
/// frame with its bytes reversed, run the per-connection pump
/// against it, send a frame from the "client" side, and assert
/// (a) the client gets the reversed echo and (b) the inspector
/// tap saw both frames with the right direction.
#[tokio::test]
async fn per_connection_pump_taps_both_directions() {
    type Tap = Arc<PMutex<Vec<(ProxyDirection, Vec<u8>)>>>;

    // Fake upstream — accepts one connection, reads one frame,
    // writes back the reversed bytes (still as a length-prefixed
    // frame), then closes.
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream.local_addr().unwrap();
    let upstream_task = tokio::spawn(async move {
        let (sock, _) = upstream.accept().await.unwrap();
        let mut framed = framed_kafka(sock);
        let frame = framed.next().await.unwrap().unwrap();
        let mut reply = frame.to_vec();
        reply.reverse();
        framed.send(reply.into()).await.unwrap();
    });

    // Tap collector.
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

#[tokio::test]
async fn proxy_handle_accepts_one_client_and_forwards_to_upstream() {
    // Fake upstream — accepts ONE connection, echoes one frame.
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream.local_addr().unwrap();
    let upstream_task = tokio::spawn(async move {
        let (sock, _) = upstream.accept().await.unwrap();
        let mut framed = framed_kafka(sock);
        let frame = framed.next().await.unwrap().unwrap();
        framed.send(frame.freeze()).await.unwrap();
    });

    let correlator = Arc::new(crate::correlator::ProtoCorrelator::new());
    let cfg = ProxyConfig::new(upstream_addr.to_string(), 0);
    let no_op_sink: crate::proxy_handle::RecordSink = Arc::new(|_msg| {});
    let handle = ProxyHandle::start(cfg, Arc::clone(&correlator), no_op_sink)
        .await
        .unwrap();
    let listen_addr = handle.local_addr();

    // Drive a fake client.
    let mut client = TcpStream::connect(listen_addr).await.unwrap();
    client.write_all(&5u32.to_be_bytes()).await.unwrap();
    // Use a 4-byte header prefix worth of data so peek doesn't reject.
    client.write_all(b"\x00\x12\x00\x03X").await.unwrap();
    let mut framed = framed_kafka(client);
    let echoed = framed.next().await.unwrap().unwrap();
    assert_eq!(echoed.as_ref(), b"\x00\x12\x00\x03X");

    upstream_task.await.unwrap();

    // Correlator should have observed at least 2 frames (send + recv).
    let summaries = correlator.summaries(100);
    assert!(summaries.len() >= 2);

    handle.stop().await;
}

#[tokio::test]
async fn pump_rewrites_metadata_response_brokers_to_local() {
    // Fake upstream: when a client sends ANY frame, reply with a
    // pre-built Metadata response that advertises 2 distant brokers.
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

    let provisioner: Arc<dyn BrokerProvisioner> = broker_map;
    let no_op_sink: crate::proxy_handle::RecordSink = Arc::new(|_msg| {});
    let topic_id_map = Arc::new(crate::proxy_topic_ids::TopicIdMap::new());
    let pump_task = tokio::spawn(async move {
        let (client_sock, _) = client_listener.accept().await.unwrap();
        let upstream_sock = TcpStream::connect(upstream_target).await.unwrap();
        run_pump_with_rewrite(
            ConnectionId(1),
            0,
            client_sock,
            upstream_sock,
            correlator,
            corr_map,
            provisioner,
            no_op_sink,
            topic_id_map,
        )
        .await
        .unwrap();
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
