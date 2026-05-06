# Kapture proxy mode — design

The single pivot that makes Kapture genuinely a "Wireshark for Kafka" rather than yet another topic browser. Date: 2026-05-05.

## Why

Today's Kapture has two weak surfaces:

1. **Messages tab** is a topic browser. AKHQ, Conduktor Console, Redpanda Console, Kowl all do this. The Wireshark-style filter DSL + MCP differentiate a little, but the core experience is the same. Users will reach for the tool they already have.
2. **Protocol tab** captures only Kapture's own client traffic (the proto-hook is per-`rd_kafka_t`). What it shows is repetitive housekeeping — Fetch, Heartbeat, Metadata of our own consumer. A user can't point Kapture at _another_ application and see what _that_ application is sending to the broker, which is the core Wireshark interaction.

Proxy mode fixes both at once. Kapture stops being a Kafka _client_ and becomes a Kafka _intermediary_: arbitrary clients (producers, consumers, kafka-cli, Java apps, any KafkaJS / Confluent / Sarama / librdkafka workload) point their `bootstrap.servers` at Kapture, and Kapture forwards every byte to a real broker while parsing the protocol bidirectionally. The user sees the actual wire-level traffic of _their_ application, not of an inspector.

## What changes

| Component                    | Before                                   | After                                                                                               |
| ---------------------------- | ---------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Connection model             | Kapture connects to broker as a consumer | Kapture listens on a TCP port; clients connect to Kapture; Kapture connects to upstream brokers     |
| Per-message capture          | librdkafka high-level Stream consumer    | Parse RecordBatch out of Produce / Fetch frames on the wire                                         |
| Per-frame capture            | proto-hook callback                      | Buffered TCP read + length-prefix framing                                                           |
| Decoder                      | already done (`kafka-protocol` crate)    | reused as-is, now on both Send and Recv paths                                                       |
| Topic regex / consumer group | `subscribe` regex on our consumer        | none — we observe whatever the real client subscribed to                                            |
| Auto-detect                  | localhost broker probe                   | localhost broker probe + suggested proxy port                                                       |
| Multi-broker                 | librdkafka handled it                    | we handle it: rewrite `advertised.listeners` in Metadata responses to route every broker through us |

The proto-hook fork stays in the tree — it remains the cleanest way to inspect a single rdkafka-based application "from inside" — but proxy mode becomes the default and headline feature.

## Architecture

```
┌─────────────┐                    ┌────────────────────┐                    ┌──────────────────┐
│             │                    │                    │                    │                  │
│  Producer   │  bootstrap=        │     Kapture        │  bootstrap=        │     Broker(s)    │
│  Consumer   │  localhost:9092 ─▶ │     proxy          │ ───────────────▶   │                  │
│  CLI        │                    │   (per-conn pump)  │                    │  upstream:9092   │
│             │                    │                    │                    │                  │
└─────────────┘                    └─────────┬──────────┘                    └──────────────────┘
                                             │
                                             │ tap → frame parser → ring buffer
                                             ▼
                                    ┌────────────────────┐
                                    │    GUI / MCP       │
                                    │  Protocol tab      │
                                    │  Messages tab      │
                                    └────────────────────┘
```

### Per-connection state

Each accepted client TCP connection maps 1:1 to one upstream broker connection (chosen via the broker the client wants to reach — see "Multi-broker"). The proxy pump is two `tokio::io::copy_bidirectional` halves but each direction goes through a _frame splitter_ that reads one Kafka frame, hands a copy to the inspector, then forwards the bytes verbatim.

State per connection:

- client TCP socket
- upstream TCP socket
- corr-id → request ApiKey / ApiVersion map (so we can decode the response with the right schema, since responses don't carry ApiKey)
- direction-aware framer (length-prefix int32 + body)
- optional TLS (terminated and re-initiated, or pass-through depending on mode)
- optional SASL (pass-through; we don't intercept SASL state)

### Frame splitter

```
loop {
    // Length prefix: 4-byte big-endian int32
    let size = read_i32(&socket).await?;
    let mut buf = vec![0u8; size as usize];
    read_exact(&mut socket, &mut buf).await?;

    // Tap (copy to inspector queue)
    inspector.send(Frame { dir, conn_id, payload: buf.clone() });

    // Forward verbatim
    other_socket.write_all(&size.to_be_bytes()).await?;
    other_socket.write_all(&buf).await?;
}
```

Tap copies the bytes once. We can cap at the same 64 KiB prefix as today and pass the truncation flag forward to the existing decoder.

### Multi-broker rewrite

When the proxy receives a Metadata response from upstream, the response carries `brokers[].host:port`. The client will then talk to those addresses. To keep traffic flowing through Kapture we rewrite each `host:port` to `localhost:<our-allocated-port-for-that-broker>`. We open a new listener for each new broker we observe, lazy-binding ports, and remember the mapping `{(host, port) → local_port}`.

For TLS: same trick except we present a self-signed cert with the SAN list we generate, and pass through SASL bytes unmodified.

This is the same trick MITM tools use (mitmproxy, kgateway). For dev clusters it's seamless; for prod it requires the user to trust our generated CA or to run in pass-through mode (no TLS termination, just observe SNI for routing).

### Correlation per connection

Per Kafka spec: corrId is monotonic per TCP connection, not per broker_id. Our existing `(brokerId, corrId)` pairing was already wrong-shaped. In proxy mode we have `connection_id` instead — a true unique key. Pairing becomes: same (connection_id, corrId), opposite direction.

## Implementation plan (ordered)

### Phase 1 — single broker, plain TCP, no auth (≈ 2 days)

1. **Listener**: `tokio::net::TcpListener::bind("127.0.0.1:9092").await?`. New AppState entry: `proxy_port: u16`, settable from GUI.
2. **Per-connection pump**: spawn `tokio::task` per accepted socket. Connect upstream to `bootstrap_servers` (single broker for now). Two `frame_splitter` halves driving an mpsc that feeds the existing correlator/decoder pipeline.
3. **Frame parser**: implement the length-prefixed framer as a `tokio_util::codec::LengthDelimitedCodec` with `length_field_offset: 0, length_field_length: 4`.
4. **Tap → ProtoFrame**: emit one `ProtoFrame` per direction. Dedupe with the existing per-rdkafka-client capture path — we want one or the other, not both.
5. **GUI**: ConnectionDialog gets a "Mode" toggle: Client (existing) / Proxy (new). Proxy mode asks for upstream broker host:port + advertised proxy port. Default proxy port: 9092, advertised: same.
6. **Disable** the rdkafka client path when in proxy mode. The `librdkafka` fork stays vendored — handy for the client mode, not used in proxy.
7. **Drop or reshape the Messages tab**. In proxy mode there's no "subscribe" — every Produce goes through us, every Fetch response carries records. Build a separate path that parses RecordBatches out of Produce requests and Fetch responses. This is the same `kafka-protocol` decoder we already use; the structures `ProduceRequest::topic_data[].partition_data[].records` and `FetchResponse::responses[].partitions[].records` give us the Bytes we feed to RecordBatch.

### Phase 2 — multi-broker (≈ 1 day)

1. Inspect every Metadata response on the wire. For each `(host, port)` we haven't seen, lazy-bind a new local listener and remember the mapping.
2. Rewrite `host` to `127.0.0.1` and `port` to the local one before forwarding the Metadata response to the client.
3. The advertised.listeners trick: most brokers advertise the host they bind. Our rewrite has to match the actual upstream address the broker advertises, not the one the client originally configured. Test against a 3-broker Apache Kafka KRaft and against Redpanda.

### Phase 3 — SASL pass-through (≈ 0.5 day)

1. Don't intercept SASL state. Just observe `SaslHandshake` and forward the bytes; the broker validates the credentials directly with the client.
2. Redact SaslAuthenticate request bytes from the inspector tap (same redact-at-MCP rule we already enforce, now extended to the GUI by default since we're in proxy mode where the user might capture credentials of OTHER apps, not their own).

### Phase 4 — TLS (≈ 1-2 days)

Two modes:

- **Pass-through**: terminate TLS at the broker (we just route the encrypted bytes by SNI). We see nothing of the protocol. Low value but safe.
- **MITM**: terminate TLS at Kapture, re-initiate a separate TLS to upstream, present a self-signed cert. User has to add our CA to their trust store. This is what gives full visibility on TLS clusters.

Default: pass-through. MITM behind a settings checkbox with a clear "trust this CA" flow.

### Phase 5 — polish (≈ 0.5 day)

1. New connection summary in the SidePanel: "proxy listening on :9092 → upstream bootstrap.kafka:9092, 4 active connections, 2 broker mappings".
2. Update the docs/spec.md to reflect the proxy as the headline feature.
3. Update MCP: `kafka_connect_profile` becomes `kapture_set_proxy_target(host, port)` semantically. The agent doesn't connect a consumer anymore; it tells Kapture which broker to forward to.

## What stays as-is

- `proto_decode.rs` (kafka-protocol decoder, supports 20+ APIs)
- `correlator.rs` ring buffer + `ProtoFrame` shape (new field: `connection_id`, not `broker_id`)
- DebugTree parser on the frontend
- ProtoList / ProtoDetail components (one prop change: pair by connection_id)
- MCP tools and resources (point at the same correlator)
- Filter DSL on messages

## What gets removed (or moves to a "client mode" submode)

- librdkafka fork dependency at runtime (kept in tree for client mode)
- proto-hook FFI (kept; client mode only)
- `consumer.subscribe(regex)` plumbing (kept for client mode, not used in proxy)
- Auto-commit constraint discussion (proxy is intrinsically passive, no group joined by Kapture)

## Risks

- **Multi-broker rewrite is brittle**. Different brokers handle `advertised.listeners` differently. Apache Kafka with `KAFKA_ADVERTISED_LISTENERS` set to a public hostname won't be reachable from Kapture's localhost rewrite without a proper LAN setup. Mitigation: well-tested against the dev stack, document that the user's advertised address must be reachable from the machine running Kapture.
- **SASL OAUTHBEARER / GSSAPI** are more involved than PLAIN/SCRAM. Phase 3 covers PLAIN/SCRAM only.
- **TLS MITM** requires the user to install our CA. If they do, they implicitly trust the local Kapture install with all their cluster credentials. Non-trivial security communication.
- **Throughput**: at MB/s of Fetch traffic, copying every byte through a tokio task could be the bottleneck. Mitigation: measure first; consider zero-copy `splice()` on Linux as a follow-up.

## Out of scope (future)

- Wire-level capture of `__consumer_offsets` topic via internal-topic ALS replication (would let us reconstruct any group's commit history)
- Replay mode: record everything to disk, replay against a different broker for testing
- Per-record decoding without the proxy (requires full Kafka protocol stack — already done via kafka-protocol crate, reusable)
- Distributed proxy across multiple Kapture instances (clusterable inspector)

## Decision

Start Phase 1 next session. Ship a single-broker plain-TCP proxy that can be pointed at by `kafkacat -b localhost:9092` and shows every Metadata / ApiVersions / Produce / Fetch frame in the Protocol tab. From there iterate.
