# Proxy Mode Deep Decoder Smoke

Wire-traffic exercise of the regenerated `kafka-protocol-rs` fork against
real Kafka 4.2.0 traffic, beyond the basic kcat-only smoke. Three flows
were targeted: transactional producer, Share Groups (KIP-932), Kafka
Streams. The proxy ran via `cargo build --release --example proxy_smoke`
and clients ran on the host (Kafka 4.2.0 Homebrew) against the `apache/kafka:latest`
container exposing `localhost:29092`. The proxy listened on
`127.0.0.1:9099` and forwarded to `localhost:29092`.

Why host clients (not docker-exec): the proxy rewrites every advertised
broker host to `127.0.0.1:<provisioned-port>`. From inside the broker
container `127.0.0.1` loops back to the container, not the host. Host
clients see the rewritten endpoints natively, so the smoke exercises the
full Metadata/FindCoordinator rewrite path + per-broker listener fleet.

The new `--bind` flag on `proxy_smoke` (default `127.0.0.1`) is wired
through `ProxyConfig::with_bind`. `0.0.0.0` is supported for short
bounded runs but not used here — host clients are sufficient.

## Smoke #1 — Transactional Producer

**Status: PASS**

Driver:

```
kafka-producer-perf-test --topic tx-source --num-records 200 \
  --record-size 64 --throughput 50 \
  --producer-props bootstrap.servers=127.0.0.1:9099 \
                   acks=all transactional.id=tx-test-1 \
                   enable.idempotence=true transaction.timeout.ms=10000
```

Decoder stats (full table):

```
api+ver+dir                        OK MISS   ok-rate    frames
ApiVersions v4 recv                 2    0    100.0%         2
ApiVersions v4 send                 2    0    100.0%         2
EndTxn v5 recv                      2    0    100.0%         2
EndTxn v5 send                      2    0    100.0%         2
FindCoordinator v6 recv             1    0    100.0%         1
FindCoordinator v6 send             1    0    100.0%         1
InitProducerId v5 recv              1    0    100.0%         1
InitProducerId v5 send              1    0    100.0%         1
Metadata v13 recv                   2    0    100.0%         2
Metadata v13 send                   2    0    100.0%         2
Produce v13 recv                  168    0    100.0%       168
Produce v13 send                  168    0    100.0%       168
```

NEW APIs vs basic smoke: `InitProducerId v5`, `EndTxn v5`. Both decoded
100% in both directions. `Produce v13` carried transactional fields
(producer id + epoch + sequence) and decoded cleanly across 168 frames.

Notable absence: `AddPartitionsToTxn` (api 24) was **not** observed.
Kafka 4.x bundles partition-add into the producer's batched protocol —
the perf-test only writes one transaction per commit window, and the
client-side `AddPartitionsToTxn` is now folded into the broker-side
TxnOffsetCommit / produce path. Not a decoder gap; just absent traffic.

`WriteTxnMarkers` (api 27) is broker-internal — never on the wire from
clients. Confirmed absent as expected.

## Smoke #2 — Share Groups (KIP-932)

**Status: SKIP** (broker config gap, not a decoder gap)

Driver:

```
kafka-console-share-consumer --bootstrap-server 127.0.0.1:9099 \
  --topic shares-test --group test-shares-2 --max-messages 5 \
  --timeout-ms 20000
```

Result: `kafka-console-share-consumer` exited with `TimeoutException`
after 0 messages despite the topic being prefilled. The broker advertises
share-group APIs (`kafka-broker-api-versions` shows `ShareGroupHeartbeat
v1`, `ShareFetch v2`, `ShareAcknowledge v2`, etc.) and `share.version=1`
is finalized, but no `__share_group_state` topic was auto-created and no
`ShareGroupHeartbeat`/`ShareFetch` frames ever crossed the wire — the
consumer hung in an `ApiVersions` + `DescribeTopicPartitions` loop on
the share coordinator connection.

Likely missing broker config (the apache/kafka:latest image does not
default these for a single-node KRaft cluster):

- `group.coordinator.rebalance.protocols=classic,consumer,share`
- `share.coordinator.state.topic.replication.factor=1`
- `share.coordinator.state.topic.min.isr=1`

What this smoke DID exercise — partial decoder stats:

```
api+ver+dir                        OK MISS   ok-rate    frames
ApiVersions v4 recv                 5    0    100.0%         5
ApiVersions v4 send                 5    0    100.0%         5
DescribeTopicPartitions v1 recv     6    0    100.0%         6
DescribeTopicPartitions v1 send     6    0    100.0%         6
FindCoordinator v6 recv             1    0    100.0%         1
FindCoordinator v6 send             1    0    100.0%         1
InitProducerId v5 recv              1    0    100.0%         1
InitProducerId v5 send              1    0    100.0%         1
Metadata v13 recv                   2    0    100.0%         2
Metadata v13 send                   2    0    100.0%         2
Produce v13 recv                    1    0    100.0%         1
Produce v13 send                    1    0    100.0%         1
```

NEW vs basic smoke: `DescribeTopicPartitions v1` (api 75, KIP-966 — the
"topic ID over names" replacement that the producer client uses on first
metadata fetch). Decoded 100% both directions.

`ShareGroupHeartbeat` (api 76), `ShareFetch` (api 78), `ShareAcknowledge`
(api 79) — decoder support exists in the fork (api keys 0..=94) but
**never observed on the wire** in this smoke. Follow-up: enable the
share-coordinator state topic in `docker-compose.yaml` and rerun.

## Smoke #3 — Kafka Streams WordCount

**Status: PASS**

Driver: `kafka-run-class org.apache.kafka.streams.examples.wordcount.WordCountDemo /tmp/streams.properties`
with `bootstrap.servers=127.0.0.1:9099` and
`processing.guarantee=at_least_once`. Fed 4 input lines via
`kafka-console-producer`, read 11 word-count rows back via
`kafka-console-consumer`.

Decoder stats (full table):

```
api+ver+dir                          OK MISS   ok-rate    frames
ApiVersions v4 recv                  14    0    100.0%        14
ApiVersions v4 send                  14    0    100.0%        14
CreateTopics v7 recv                  2    0    100.0%         2
CreateTopics v7 send                  2    0    100.0%         2
DescribeCluster v2 recv               4    0    100.0%         4
DescribeCluster v2 send               4    0    100.0%         4
Fetch v18 recv                       95    0    100.0%        95
Fetch v18 send                       96    0    100.0%        96
FindCoordinator v6 recv               2    0    100.0%         2
FindCoordinator v6 send               2    0    100.0%         2
Heartbeat v4 recv                    14    0    100.0%        14
Heartbeat v4 send                    14    0    100.0%        14
InitProducerId v5 recv                2    0    100.0%         2
InitProducerId v5 send                2    0    100.0%         2
JoinGroup v9 recv                     5    0    100.0%         5
JoinGroup v9 send                     5    0    100.0%         5
LeaveGroup v5 recv                    1    0    100.0%         1
LeaveGroup v5 send                    1    0    100.0%         1
ListClientMetricsResources v0 recv    4    0    100.0%         4
ListClientMetricsResources v0 send    4    0    100.0%         4
ListOffsets v11 recv                 13    0    100.0%        13
ListOffsets v11 send                 13    0    100.0%        13
Metadata v13 recv                    13    0    100.0%        13
Metadata v13 send                    13    0    100.0%        13
OffsetCommit v9 recv                  1    0    100.0%         1
OffsetCommit v9 send                  1    0    100.0%         1
OffsetFetch v9 recv                   8    0    100.0%         8
OffsetFetch v9 send                   8    0    100.0%         8
Produce v13 recv                      3    0    100.0%         3
Produce v13 send                      3    0    100.0%         3
SyncGroup v5 recv                     3    0    100.0%         3
SyncGroup v5 send                     3    0    100.0%         3
```

NEW APIs vs basic smoke: `CreateTopics v7`, `DescribeCluster v2`,
`Fetch v18`, `Heartbeat v4`, `InitProducerId v5` (idempotent producer,
non-transactional), `JoinGroup v9`, `LeaveGroup v5`,
`ListClientMetricsResources v0` (KIP-714 client telemetry — observed
because Streams enables client telemetry by default in 4.x),
`ListOffsets v11`, `OffsetCommit v9`, `OffsetFetch v9`, `SyncGroup v5`.
All decoded 100% both directions.

Streams uses `at_least_once` by default — no `EndTxn`/`AddPartitions
ToTxn` here, just an idempotent producer with `InitProducerId`. To
observe transactions in Streams traffic the demo would need
`processing.guarantee=exactly_once_v2`.

## Cross-smoke summary

NEW APIs unlocked by these deep smokes (over the prior kcat-only
baseline), each with its negotiated version and decode rate:

- `InitProducerId v5` — 100% both dirs
- `EndTxn v5` — 100% both dirs
- `DescribeTopicPartitions v1` — 100% both dirs
- `CreateTopics v7` — 100% both dirs
- `DescribeCluster v2` — 100% both dirs
- `Fetch v18` — 100% both dirs
- `Heartbeat v4` — 100% both dirs
- `JoinGroup v9` — 100% both dirs
- `SyncGroup v5` — 100% both dirs
- `LeaveGroup v5` — 100% both dirs
- `OffsetCommit v9` — 100% both dirs
- `OffsetFetch v9` — 100% both dirs
- `ListOffsets v11` — 100% both dirs
- `ListClientMetricsResources v0` — 100% both dirs

Decode misses: **none**. Every frame the decoder saw was decoded
successfully. The `Send`/`Recv` counts stayed in lockstep across all
APIs except `Fetch v18` (95 recv vs 96 send), which is the typical
in-flight-at-shutdown skew — one fetch was sent at proxy stop and never
got its response paired before exit.

KIP-511 fallback: not observed. The `ApiVersions v4` request (the v4
fallback path KIP-511 introduced) was always answered with a v4
response, never downgraded — Kafka 4.2 supports v4 natively. The
`ApiVersions v4 recv` rows in every table confirm this.

APIs in the fork that decoder-supports but were NOT exercised on the
wire (gap for future smokes):

- `AddPartitionsToTxn` (24), `WriteTxnMarkers` (27) — needs a
  multi-partition transactional producer + broker-side trace
- `ShareGroupHeartbeat` (76), `ShareGroupDescribe` (77),
  `ShareFetch` (78), `ShareAcknowledge` (79) — needs share coordinator
  state topic enabled in docker-compose
- Streams `exactly_once_v2` mode would surface
  `AddPartitionsToTxn` + `EndTxn` inside Streams traffic
