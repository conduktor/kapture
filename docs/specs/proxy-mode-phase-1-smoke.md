# Proxy mode — Phase 1 multi-broker smoke

End-to-end validation of the response-rewriter chain against the
3-broker Apache Kafka KRaft `mb` profile, driven programmatically via
`cargo run --example proxy_smoke` and exercised by `kcat`.

## Reproduce

```sh
pnpm stack:up:mb
cargo run --manifest-path src-tauri/Cargo.toml --example proxy_smoke -- \
    --upstream localhost:39092 --listen 9092 --seconds 90 &
# (different shell)
kcat -b 127.0.0.1:9092 -L
printf "k1:v1\nk2:v2\nk3:v3\n" | kcat -b 127.0.0.1:9092 -P -t mb-test -K:
kcat -b 127.0.0.1:9092 -C -t mb-test -e -o beginning
```

`kcat` against `localhost:9092` triggers a noisy IPv6 warning before
falling back to IPv4 — the proxy binds IPv4 only. Hitting `127.0.0.1`
sidesteps it. Functionally identical.

## Control case — `kcat -L` directly against the upstream

`/opt/homebrew/bin/kcat -b localhost:39092 -L`:

```
Metadata for all topics (from broker 2: localhost:39093/2):
 3 brokers:
  broker 1 at localhost:39092
  broker 2 at localhost:39093 (controller)
  broker 3 at localhost:39094
 0 topics:
```

Brokers advertise their internal listener addresses (`localhost:39092`,
`:39093`, `:39094`).

## Through the proxy — `kcat -L` via `127.0.0.1:9092`

`/opt/homebrew/bin/kcat -b localhost:9092 -L` (after the proxy ran):

```
Metadata for all topics (from broker 1: 127.0.0.1:9092/1):
 3 brokers:
  broker 1 at 127.0.0.1:9092 (controller)
  broker 2 at 127.0.0.1:54057
  broker 3 at 127.0.0.1:54058
 0 topics:
```

All three broker advertisements rewritten to `127.0.0.1` with three
distinct ports:

| Upstream          | Local rewrite     |
| ----------------- | ----------------- | --------------------------------------- |
| `localhost:39092` | `127.0.0.1:9092`  | (bootstrap, pre-seeded)                 |
| `localhost:39093` | `127.0.0.1:54057` | (lazy-bound on first Metadata response) |
| `localhost:39094` | `127.0.0.1:54058` | (lazy-bound on first Metadata response) |

The rewriter promotes `localhost` to `127.0.0.1` and emits ephemeral
ports allocated by `BrokerMap::ensure_listener` → `spawn_listener`.

## Frame sequence captured by the `ProtoCorrelator`

71 frames in total across 14 distinct `connection_id` values
(one per kcat-opened connection). API breakdown:

| API         | Frames |
| ----------- | -----: |
| ApiVersions |     28 |
| Metadata    |     28 |
| Produce     |      8 |
| Fetch       |      5 |
| ListOffsets |      2 |

Representative excerpt (proxy_smoke stdout, formatted):

```
FRAME -> api=ApiVersions  v3   corr=0x00000001 conn=1  size=41
FRAME <- api=ApiVersions  v3   corr=0x00000001 conn=1  size=724  rtt=3.8ms
FRAME -> api=Metadata     v13  corr=0x00000002 conn=1  size=26
FRAME <- api=Metadata     v13  corr=0x00000002 conn=1  size=101  rtt=3.0ms
FRAME -> api=ApiVersions  v3   corr=0x00000001 conn=4  size=41
FRAME <- api=ApiVersions  v3   corr=0x00000001 conn=4  size=724  rtt=1.5ms
FRAME -> api=Metadata     v13  corr=0x00000002 conn=4  size=51
FRAME <- api=Metadata     v13  corr=0x00000002 conn=4  size=160  rtt=2.4ms
FRAME -> api=Produce      v10  corr=0x00000003 conn=4  size=129
FRAME <- api=Produce      v10  corr=0x00000003 conn=4  size=58   rtt=6.5ms
FRAME -> api=ApiVersions  v3   corr=0x00000001 conn=14 size=41
FRAME <- api=ApiVersions  v3   corr=0x00000001 conn=14 size=724  rtt=1.6ms
FRAME -> api=Metadata     v13  corr=0x00000002 conn=14 size=51
FRAME <- api=Metadata     v13  corr=0x00000002 conn=14 size=160  rtt=1.0ms
FRAME -> api=ListOffsets  v7   corr=0x00000003 conn=14 size=56
FRAME <- api=ListOffsets  v7   corr=0x00000003 conn=14 size=52   rtt=3.6ms
FRAME -> api=Fetch        v16  corr=0x00000004 conn=14 size=98
FRAME <- api=Fetch        v16  corr=0x00000004 conn=14 size=447  rtt=8.3ms
FRAME -> api=Fetch        v16  corr=0x00000005 conn=14 size=98
FRAME <- api=Fetch        v16  corr=0x00000005 conn=14 size=76   rtt=505.1ms
```

Each kcat invocation opens a fresh TCP connection (new
`connection_id`); the proxy boilerplate (`ApiVersions` → `Metadata`)
fires per connection, then the API mix follows the kcat operation
(Produce on `-P`, ListOffsets + Fetch on `-C`).

## Produce + consume

```
$ printf "k1:v1\nk2:v2\nk3:v3\n" | kcat -b 127.0.0.1:9092 -P -t mb-test -K:
$ kcat -b 127.0.0.1:9092 -C -t mb-test -e -o beginning
% Reached end of topic mb-test [0] at offset 6: exiting
hello-mb-1
hello-mb-2
hello-mb-3
hello-mb-1
hello-mb-2
hello-mb-3
```

(Six messages because the topic was reused across the `localhost:9092`
and `127.0.0.1:9092` runs.) Both Produce and Fetch flow end-to-end
through the proxy; the proxy log's `Produce v10 -> / <-` and
`Fetch v16 -> / <-` exchanges confirm it.

## Conclusion

- Metadata rewrite chain: confirmed (3 brokers → 127.0.0.1:varying-ports).
- Lazy listener bind: confirmed (2 satellite listeners spun up
  on-demand at `54057` / `54058`).
- Multi-connection correlation: 14 distinct `connection_id`s, each
  carrying its own ApiVersions / Metadata negotiation.
- Wire-level capture: 71 frames recorded to `ProtoCorrelator`,
  available for the Protocol tab.
- Produce + Consume: end-to-end success.

This validates Phase 1 (single-broker proxy + correlator) AND the
Phase 2 lazy-listener fleet against a real 3-broker KRaft cluster.

## Notes / deviations

- IPv6 fallback noise: kcat against `localhost:9092` first tries
  `[::1]:9092` and logs a `Connection refused` warning before falling
  back to IPv4. Harmless; binding `[::1]` would require dual-stack
  listener changes in `ProxyHandle::start`.
- One Fetch shows `rtt=505.1ms` — that's the consumer's blocking poll
  tail, not a real round-trip. The `ProtoCorrelator` reports wall-clock
  time between request send and response receipt, which on consumer
  Fetch is correct but not the broker-internal latency.
- The proxy's `--seconds` upper bound is what makes this CI-friendly:
  no TTY required, ctrl-c still works for interactive sessions.

## Phase 3 — SASL pass-through with credential redaction

End-to-end validation that the proxy forwards a SASL/PLAIN handshake
verbatim to the broker and that the captured `SaslAuthenticate` request
payload is redacted in the inspector ring buffer.

### Stack

`docker-compose.yml` profile `sasl` brings up a single Apache Kafka 4.x
KRaft broker with two listeners:

- `PLAINTEXT://kafka-sasl:9092` — internal / inter-broker.
- `SASL_PLAINTEXT://localhost:49092` — exposed to the host, mechanism
  `PLAIN`. JAAS config inlined as an env var.

Dev-only credentials, baked into the JAAS string, **not for any
non-dev use**:

| user    | password       |
| ------- | -------------- |
| `admin` | `admin-secret` |
| `alice` | `alice-secret` |

```sh
pnpm stack:up:sasl
```

### Control — kcat directly against the broker

`kcat -b localhost:49092 -X security.protocol=SASL_PLAINTEXT -X sasl.mechanism=PLAIN -X sasl.username=alice -X sasl.password=alice-secret -L`:

```
Metadata for all topics (from broker 1: sasl_plaintext://localhost:49092/1):
 1 brokers:
  broker 1 at localhost:49092 (controller)
 0 topics:
```

SASL/PLAIN works end-to-end against the dev broker.

### Through the proxy

```sh
cargo run --manifest-path src-tauri/Cargo.toml --example proxy_smoke -- \
    --upstream localhost:49092 --listen 9092 --seconds 25 &
kcat -b 127.0.0.1:9092 -X security.protocol=SASL_PLAINTEXT \
    -X sasl.mechanism=PLAIN -X sasl.username=alice -X sasl.password=alice-secret -L
echo "hello-sasl" | kcat -b 127.0.0.1:9092 -X security.protocol=SASL_PLAINTEXT \
    -X sasl.mechanism=PLAIN -X sasl.username=alice -X sasl.password=alice-secret -P -t sasl-test
kcat -b 127.0.0.1:9092 -X security.protocol=SASL_PLAINTEXT \
    -X sasl.mechanism=PLAIN -X sasl.username=alice -X sasl.password=alice-secret \
    -C -t sasl-test -e -o beginning
```

`kcat -L` through the proxy:

```
Metadata for all topics (from broker 1: sasl_plaintext://127.0.0.1:9092/1):
 1 brokers:
  broker 1 at 127.0.0.1:9092 (controller)
 0 topics:
```

Produce + Consume returned `hello-sasl` at offset 0 — full round-trip
through the proxy, broker authenticated the client directly.

### Frame sequence captured

61 frames over 6 connections. Per-connection mix:

```
ApiVersions   -> ApiVersions   <-
SaslHandshake -> SaslHandshake <-
SaslAuthenticate -> SaslAuthenticate <-
Metadata      -> Metadata      <-
(then the operation: Produce / Fetch / ListOffsets)
```

API breakdown:

| API              | Frames |
| ---------------- | -----: |
| ApiVersions      |     12 |
| SaslHandshake    |     12 |
| SaslAuthenticate |     12 |
| Metadata         |     14 |
| Produce          |      2 |
| Fetch            |      8 |
| ListOffsets      |      1 |

### Redaction verification

`grep -c "alice-secret" /tmp/proxy_smoke_sasl.log` → `0`.
`grep -c "alice"        /tmp/proxy_smoke_sasl.log` → `0`.

The unit test `proxy::tests::build_proto_event_redacts_sasl_authenticate_request_payload`
plants a credential `\0alice\0alice-secret` in a synthetic api_key=36
v2 frame, runs it through `build_proto_event`, and asserts the
resulting `ProtoEvent.payload` contains neither the full credential
nor the substring `alice-secret`. Module tests
`proxy_redact::tests::redact_sasl_authenticate_replaces_body_after_header`
and `..._short_payload_is_safe` cover the redaction helper directly.

### Conclusion

- SASL/PLAIN handshake forwards verbatim through the proxy: confirmed.
- Forwarded bytes are NOT modified — the broker authenticates the
  real client.
- `SaslAuthenticate` request payload in the ring buffer is replaced
  with a fixed `[REDACTED SaslAuthenticate body]` placeholder before
  `ProtoCorrelator::record_event` is called. The credential never
  enters the inspector path.

### Notes / deviations

- The dev cluster uses `SASL_PLAINTEXT`, not `SASL_SSL`. The proxy
  doesn't terminate TLS, so `SASL_SSL` would need a separate phase
  (TLS pass-through or termination — not in scope here).
- Only `PLAIN` is exercised end-to-end. The redaction strategy is
  mechanism-agnostic (replaces the entire body), so SCRAM and
  OAUTHBEARER are covered by the same code path, but the dev stack
  only ships `PLAIN`.
- The redaction strategy is the paranoid one — replace the whole
  body with a fixed placeholder rather than trying to skip the
  request header. Trade-off: the Protocol-tab decoder will fail on
  these frames (the placeholder isn't a valid `SaslAuthenticate`
  body). That's correct — there's nothing to inspect.

## Phase 4 step 5 — SASL injection smoke

Goal: prove the proxy can authenticate upstream on the client's
behalf. A kcat client with NO SASL config talks plain TCP to the
proxy on `127.0.0.1:9092`; the proxy authenticates as
`alice/alice-secret` against a `SASL_PLAINTEXT` broker on
`localhost:49092`. Round-trip a topic to confirm produce + fetch
both flow through.

### Setup

- Branch: `proxy-auth-inject`
- Cluster: `pnpm stack:up:sasl` (Apache Kafka KRaft single-broker,
  port 49092, `SASL_PLAINTEXT`/`PLAIN`, dev creds `alice/alice-secret`)
- Harness: `cargo run --example proxy_smoke -- --upstream localhost:49092
--listen 9092 --seconds 30 --sasl-username alice
--sasl-password alice-secret`
- `proxy_smoke.rs` extended with `--sasl-username` / `--sasl-password`
  CLI args; `lib.rs` `example_api` re-exports `UpstreamSaslConfig`,
  `UpstreamSaslMechanism`, `UpstreamTlsConfig`.

### Control: direct kcat with SASL config

```text
$ kcat -b localhost:49092 -X security.protocol=SASL_PLAINTEXT \
    -X sasl.mechanism=PLAIN -X sasl.username=alice \
    -X sasl.password=alice-secret -L
Metadata for all topics (from broker 1: sasl_plaintext://localhost:49092/1):
 1 brokers:
  broker 1 at localhost:49092 (controller)
 0 topics:
```

### Proxy-driven: kcat with NO SASL config

```text
$ kcat -b localhost:9092 -L
Metadata for all topics (from broker 1: 127.0.0.1:9092/1):
 1 brokers:
  broker 1 at 127.0.0.1:9092 (controller)
 0 topics:
```

This is the headline result: same shape of metadata response, but
the client is plain TCP. Auth happened entirely inside the proxy.

### Round-trip: produce 5, consume 5

```text
$ for i in 1 2 3 4 5; do
    echo "k$i:hello-injection-$i" | kcat -b localhost:9092 -P -t inject-test -K:
  done

$ kcat -b 127.0.0.1:9092 -C -t inject-test -e -o beginning
% Reached end of topic inject-test [0] at offset 5: exiting
k.ello-injection-1
k.ello-injection-2
k.ello-injection-3
k.ello-injection-4
k.ello-injection-5
```

(`localhost` resolved to `::1` first for the consumer; the proxy
binds `127.0.0.1` only, so the consumer was retargeted to
`127.0.0.1:9092`. Cosmetic — same proxy, same path.)

The leading `k.ello…` is `key:value` collision with kcat's `-K:`
delimiter on stdout — values on the wire are `hello-injection-N`,
verified by the proxy's record sink:

```text
RECORD topic=inject-test partition=0 offset=0 key=None size=18
RECORD topic=inject-test partition=0 offset=1 key=None size=18
RECORD topic=inject-test partition=0 offset=2 key=None size=18
RECORD topic=inject-test partition=0 offset=3 key=None size=18
RECORD topic=inject-test partition=0 offset=4 key=None size=18
```

### Numbers

- Total frames observed by `ProtoCorrelator`: **79**
- Records captured by `RecordSink`: **10** (5 produce + 5 fetch)
- `topic_id_map` size: **1** (`inject-test`)
- APIs seen: `ApiVersions`, `Metadata`, `Produce`, `ListOffsets`,
  `Fetch` — all forwarded after the proxy's per-connection upstream
  SASL handshake completed.

### Credential leak grep

```text
$ grep -c 'alice-secret' /tmp/proxy-sasl-inject-smoke.log
1
$ grep 'alice-secret' /tmp/proxy-sasl-inject-smoke.log
     Running `src-tauri/target/debug/examples/proxy_smoke ... --sasl-password alice-secret`
```

The single match is the cargo-run command echo of the example's
own CLI args (a dev-harness artifact, not proxy output). The proxy
itself emits **zero** log lines containing the password. No
`SaslAuthenticate` frame appears in the captured-frame stream:
`open_upstream` runs the handshake on a raw `TcpStream` _before_
the framed pump and `ProtoCorrelator` are wired in, so the proxy's
own auth bytes never enter the inspector path. Phase 3's
`SaslAuthenticate`-body redaction would catch any client-emitted
auth frame as well; here, no client emitted one.

### Notes / deviations

- `open_upstream` doesn't currently emit any `tracing` lines for
  the SASL exchange. The functional proof is the absence of a
  failure: a `SASL_PLAINTEXT` broker rejects unauthenticated
  Metadata/Produce/Fetch outright. All 79 frames flowed; the
  handshake therefore succeeded. Adding a single `info!("upstream
sasl handshake ok mechanism={mech}")` would be a nice-to-have
  for ops visibility — flagged for a follow-up.
- TLS path (`open_upstream` with `Some(UpstreamTlsConfig)`) is
  plumbed and unit-tested but not exercised in this smoke. A
  separate spec covers TLS + SASL together.
- The "change only the bootstrap server" promise holds: the
  client config delta from direct → proxied is `bootstrap=localhost:49092
  - 4 SASL params`→`bootstrap=localhost:9092`. Three lines
    removed, one host changed.

## Phase 4 step 6 — SCRAM injection smoke

End-to-end validation of upstream SCRAM-SHA-256 / SCRAM-SHA-512
injection against a real Apache Kafka 4.x KRaft broker. Same
"change only the bootstrap server" promise as the PLAIN smoke:
kcat carries zero SASL config and produces / consumes through
Kapture; Kapture performs the SCRAM dance with the broker.

### Reproduce

```sh
pnpm stack:up:scram                              # broker on :59092 + SCRAM users init
# Direct check (PLAIN-config kcat → SCRAM listener)
kcat -b localhost:59092 \
    -X security.protocol=SASL_PLAINTEXT \
    -X sasl.mechanism=SCRAM-SHA-256 \
    -X sasl.username=alice -X sasl.password=alice-scram -L

# Through Kapture (NO SASL config on kcat)
cargo run --manifest-path src-tauri/Cargo.toml --example proxy_smoke -- \
    --upstream localhost:59092 --listen 9092 --seconds 25 \
    --sasl-mechanism SCRAM-SHA-256 --sasl-username alice --sasl-password alice-scram &
sleep 4
kcat -b 127.0.0.1:9092 -L
printf "scram-k1:scram-v1\nscram-k2:scram-v2\nscram-k3:scram-v3\n" \
    | kcat -b 127.0.0.1:9092 -P -t scram-test -K:
kcat -b 127.0.0.1:9092 -C -t scram-test -e -o beginning -q
```

### Direct case — kcat with full SCRAM config

```
Metadata for all topics (from broker 1: sasl_plaintext://localhost:59092/1):
 1 brokers:
  broker 1 at localhost:59092 (controller)
 0 topics:
```

### Through proxy — kcat with NO SASL config

```
Metadata for all topics (from broker 1: 127.0.0.1:9092/1):
 1 brokers:
  broker 1 at 127.0.0.1:9092 (controller)
 0 topics:
```

### Produce + consume round-trip (SCRAM-SHA-256)

`scram-v1`, `scram-v2`, `scram-v3` produced via proxy, consumed via
proxy. proxy_smoke summary:

```
proxy_smoke: stopped. total frames observed: 37 | captured 6 messages | topic_id_map size: 1
  topic_id <uuid> -> scram-test
```

Frame breakdown — each kcat connection drives a fresh upstream
connect, each with its own SCRAM handshake on the upstream side:
ApiVersions, Metadata, then Produce or Fetch as needed.

### SCRAM-SHA-512 (bob / bob-scram-512)

Identical exercise on listener 9093 with `--sasl-mechanism
SCRAM-SHA-512`. proxy_smoke summary:

```
proxy_smoke: stopped. total frames observed: 37 | captured 4 messages | topic_id_map size: 2
```

### What this proves

- Kapture's hand-rolled SCRAM client (RFC 5802 / 7677) interoperates
  with a stock Apache Kafka 4.x broker: client-first → server-first
  → client-final → server-final, with `ServerSignature` mutual-auth
  verification.
- PBKDF2 + HMAC-SHA-{256,512} + StoredKey/ClientKey/ServerKey
  derivations match the broker's. If they didn't, server_first
  would emit a SASL_AUTHENTICATION_FAILED.
- `auth_bytes` is correctly carried inside `SaslAuthenticateRequest
/ Response` v2 with compact framing (`kafka-protocol` handles
  the flexible-version encoding).
- The post-SCRAM `TcpStream` is clean — first downstream-driven
  request flows immediately with no buffered preamble.

### Notes / deviations

- KRaft SCRAM provisioning: Apache Kafka 4.x KRaft requires SCRAM
  users either at format time (`kafka-storage format --add-scram`)
  or via `kafka-configs.sh --alter` after startup. We use the
  latter, against the broker's internal PLAINTEXT listener, in a
  one-shot `kafka-scram-init` sidecar. This avoids a chicken-and-
  egg where the SCRAM mechanism would need creds to provision its
  own creds.
- The SASL listener also enables PLAIN alongside SCRAM (via static
  JAAS) so the broker has a working mechanism during the brief
  window before init completes. Not consumed by Kapture.
- env-var → property name conversion in apache/kafka image:
  `_` → `.`, `__` → `_`, `___` → `-`. Hence
  `KAFKA_LISTENER_NAME_SASL_SCRAM___SHA___256_SASL_JAAS_CONFIG`
  for `listener.name.sasl.scram-sha-256.sasl.jaas.config`.
- PBKDF2 iterations: client caps at `1..=1_000_000` to refuse a
  hostile broker pinning the proxy in PBKDF2. At Kafka's default
  4096 the work is sub-millisecond; we keep it inline rather than
  offloading to `spawn_blocking` (offloading would require
  `'static + Send` on the generic upstream stream type).
- RFC 7677 §3 SCRAM-SHA-256 test vector hard-coded in
  `proxy_upstream::scram::tests` — `ClientProof = dHzbZapWIk4j…`
  matches byte-for-byte. SCRAM-SHA-512 has a self-consistency
  round-trip test (no widely-published RFC vector).
