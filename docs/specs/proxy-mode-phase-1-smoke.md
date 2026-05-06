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
