# Kapture performance and observability plan

This plan turns the performance/eBPF audit into independently testable milestones.
The ordering is deliberate: Kapture must first be bounded and transparent under
overload, then become faster, then gain additional Linux/eBPF visibility.

## Success invariants

- Kafka traffic is never delayed to preserve inspector data.
- Every queue and retained history has both a count or byte bound.
- Observation loss is explicit and invalidates any stream that can no longer be
  decoded safely.
- Capture timestamps describe the observation point, not a later UI/IPC queue.
- Performance claims are backed by a reproducible, fixed-arrival-rate workload.

## M0 — Reproducible baseline

- [ ] Add a fixed-arrival-rate benchmark driver separate from the demo seed.
- [ ] Cover direct, proxy, and JVM-tap modes; plaintext and TLS where applicable.
- [ ] Record achieved rate, client latency p50/p95/p99/p999, CPU, RSS, queue
      drops, and capture-to-analysis lag.
- [ ] Add payload sizes 100 B, 1 KiB, 64 KiB, and 1 MiB, with the UI live and
      paused.
- [ ] Add a profiling build with symbols/frame pointers and document Linux,
      macOS, and JVM profiling commands.

Acceptance: the driver does not wait for one send to finish before scheduling
the next; a deliberately injected stall appears in the tail distribution and
does not reduce the intended arrival count.

## M1 — JVM tap safety and inactive fast path

- [ ] Do not allocate/copy payloads while no Kapture listener is connected.
- [ ] Reconnect on the writer thread with bounded exponential backoff, never
      once per Kafka read/write.
- [ ] Bound the queue by both frame count and total queued bytes.
- [ ] Reset the UDS session after any dropped/partially-written chunk so Rust
      never decodes a byte stream with a hole.
- [ ] Surface cumulative agent drops to Kapture capture-health metrics.
- [ ] Release connection-owner entries when Kafka transport objects die.
- [ ] Batch/gather header and payload writes where the platform permits it.

Acceptance: with Kapture stopped, a sustained Java workload performs no advice
payload allocations and no per-message connect syscalls; forcing a queue drop
produces an explicit unhealthy/incomplete capture state and no fabricated Kafka
frame after the gap.

## M2 — Bounded ingestion, analysis, and IPC

- [ ] Replace the unbounded message IPC channel with a bounded summary channel.
- [ ] Keep full messages only in the bounded backend ring.
- [ ] Bound the frontend pre-render queue even when `requestAnimationFrame` is
      suspended/minimized.
- [ ] Move protocol JSON/summary/hex work off the proxy forwarding task into a
      bounded analyzer worker.
- [ ] Distinguish ring eviction, oversized-record rejection, analyzer loss,
      agent loss, and UI-summary loss in capture health.
- [ ] Bound or immediately emit `acks=0` Produce records; never retain them for
      a response that cannot arrive.

Acceptance: pausing/minimizing the renderer or slowing analysis cannot grow RSS
without bound and cannot increase broker-facing request latency to preserve UI
history.

## M3 — Honest memory ownership and zero-copy paths

- [ ] Account retained heap bytes rather than Kafka payload bytes.
- [ ] Add a byte cap to the protocol-frame ring.
- [ ] Retain raw bytes once; derive spaced hex and decoded trees lazily.
- [ ] Pass `Bytes` through proxy record/protocol decoders without copying whole
      Fetch/Produce frames.
- [ ] Avoid materializing byte arrays in JSON merely to replace them with an
      elision marker.
- [ ] Keep pause snapshots within an explicit additional-memory budget.

Acceptance: filling either ring cannot exceed its configured retained-byte
budget by more than documented allocator/index overhead; a 100 MiB Fetch frame
is not copied wholesale merely for inspection.

## M4 — Incremental queries and renderer work

- [ ] Limit message snapshots server-side to the 5,000 rows the UI retains.
- [ ] Prefer newest-first lookup for current selections and schema patches; add
      an ID index if profiling still shows scans.
- [ ] Batch decoded-body lookup and negative-cache undecodable bodies.
- [ ] Poll protocol summaries by cursor/delta, with a reset response after ring
      eviction, instead of serializing all 5,000 rows every second.
- [ ] Lazily build/virtualize large hex and JSON detail trees.

Acceptance: steady traffic with fewer than 5,000 new frames per tick performs
work proportional to the delta; selecting a recent row does not scan 100,000
messages; an undecodable body is fetched at most once per ring lifetime.

## M5 — Time and tail-latency correctness

- [ ] Consume JVM `System.nanoTime()` timestamps and compute tap RTT at the
      Java observation boundary.
- [ ] Track capture-to-Rust and capture-to-render lag separately.
- [ ] Maintain mergeable fixed-memory latency histograms per API/broker.
- [ ] Track aging in-flight requests so requests without responses are not
      omitted from latency health.

Acceptance: artificial JVM-writer queueing changes capture lag but not measured
Kafka RTT; a request with no response becomes a timeout/hung-request sample.

## M6 — Expert signals

- [ ] Hung requests / in-flight saturation.
- [ ] Excessive in-flight idempotent Produce requests.
- [ ] `read_uncommitted` while transactional traffic is present.
- [ ] Partition byte/record skew.
- [ ] Generic retriable-error storm.
- [ ] Capture-incomplete health, kept separate from client anti-patterns.

Acceptance: each detector has positive, threshold-boundary, false-positive, and
state-expiry tests.

## M7 — Linux eBPF tap and system diagnosis

- [ ] PID-scoped OpenSSL `SSL_read`/`SSL_write` and `_ex` uprobe/uretprobe
      coverage for selected librdkafka-family clients.
- [ ] Minimal BPF program: validate return lengths, copy bounded chunks, attach
      connection/direction sequence numbers, decode Kafka only in userspace.
- [ ] Ring-buffer reservation/drop counters and stream invalidation on gaps.
- [ ] Optional TCP retransmit/connect and scheduler/off-CPU diagnostics tied to
      the selected PID and broker connection.
- [ ] Report BPF runtime/run-count and enforce an overhead budget in the M0
      benchmark before release.

Acceptance: no host-wide attachment by default; unsupported symbols/kernels
fail closed with an actionable status; an induced BPF ring-buffer loss marks
the affected stream incomplete; measured overhead stays within the documented
budget at the target event rate.

## Explicit non-goals until profiling proves otherwise

- Thread-per-core, DPDK, AF_XDP, io_uring, NUMA pinning, huge pages, SIMD, or a
  global allocator swap.
- Sampling plaintext payload events: Kafka framing requires every byte.
- Blocking Kafka traffic to make an inspector capture lossless.
