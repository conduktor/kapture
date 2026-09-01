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

- [x] Add a fixed-arrival-rate benchmark driver separate from the demo seed.
- [x] Cover direct, proxy, JVM-tap and Linux eBPF modes; plaintext and TLS where applicable.
- [x] Record achieved rate, client latency p50/p95/p99/p999, CPU, RSS, queue
      drops, and capture-to-analysis lag.
- [x] Add payload sizes 100 B, 1 KiB, 64 KiB, and 1 MiB, with the UI live and
      paused.
- [x] Add a profiling build with symbols/frame pointers and document Linux,
      macOS, and JVM profiling commands.

Acceptance: the driver does not wait for one send to finish before scheduling
the next; a deliberately injected stall appears in the tail distribution and
does not reduce the intended arrival count.

## M1 — JVM tap safety and inactive fast path

- [x] Do not allocate/copy payloads while no Kapture listener is connected.
- [x] Reconnect on the writer thread with bounded exponential backoff, never
      once per Kafka read/write.
- [x] Bound the queue by both frame count and total queued bytes.
- [x] Reset the UDS session after any dropped/partially-written chunk so Rust
      never decodes a byte stream with a hole.
- [x] Surface cumulative agent drops to Kapture capture-health metrics.
- [x] Release connection-owner entries when Kafka transport objects die.
- [x] Batch/gather header and payload writes where the platform permits it.

Acceptance: with Kapture stopped, a sustained Java workload performs no advice
payload allocations and no per-message connect syscalls; forcing a queue drop
produces an explicit unhealthy/incomplete capture state and no fabricated Kafka
frame after the gap.

## M2 — Bounded ingestion, analysis, and IPC

- [x] Replace the unbounded message IPC channel with a bounded summary channel.
- [x] Keep full messages only in the bounded backend ring.
- [x] Bound the frontend pre-render queue even when `requestAnimationFrame` is
      suspended/minimized.
- [x] Move protocol JSON/summary/hex work off the proxy forwarding task into a
      bounded analyzer worker.
- [x] Distinguish ring eviction, oversized-record rejection, analyzer loss,
      agent loss, and UI-summary loss in capture health.
- [x] Bound or immediately emit `acks=0` Produce records; never retain them for
      a response that cannot arrive.

Acceptance: pausing/minimizing the renderer or slowing analysis cannot grow RSS
without bound and cannot increase broker-facing request latency to preserve UI
history.

## M3 — Honest memory ownership and zero-copy paths

- [x] Account retained heap bytes rather than Kafka payload bytes.
- [x] Add a byte cap to the protocol-frame ring.
- [x] Retain raw bytes once; derive spaced hex and decoded trees lazily.
- [x] Pass `Bytes` through proxy record/protocol decoders without copying whole
      Fetch/Produce frames.
- [x] Avoid materializing byte arrays in JSON merely to replace them with an
      elision marker.
- [x] Keep pause snapshots within an explicit additional-memory budget.

Acceptance: filling either ring cannot exceed its configured retained-byte
budget by more than documented allocator/index overhead; a 100 MiB Fetch frame
is not copied wholesale merely for inspection.

## M4 — Incremental queries and renderer work

- [x] Limit message snapshots server-side to the 5,000 rows the UI retains.
- [x] Prefer newest-first lookup for current selections and schema patches; add
      an ID index if profiling still shows scans.
- [x] Batch decoded-body lookup and negative-cache undecodable bodies.
- [x] Poll protocol summaries by cursor/delta, with a reset response after ring
      eviction, instead of serializing all 5,000 rows every second.
- [x] Lazily build/virtualize large hex and JSON detail trees.

Acceptance: steady traffic with fewer than 5,000 new frames per tick performs
work proportional to the delta; selecting a recent row does not scan 100,000
messages; an undecodable body is fetched at most once per ring lifetime.

## M5 — Time and tail-latency correctness

- [x] Consume JVM `System.nanoTime()` timestamps and compute tap RTT at the
      Java observation boundary.
- [x] Track capture-to-Rust, capture-to-analysis and capture-to-render lag separately.
- [x] Maintain mergeable fixed-memory latency histograms per API/broker.
- [x] Track aging in-flight requests so requests without responses are not
      omitted from latency health.

Acceptance: artificial JVM-writer queueing changes capture lag but not measured
Kafka RTT; a request with no response becomes a timeout/hung-request sample.

## M6 — Expert signals

- [x] Hung requests / in-flight saturation.
- [x] Excessive in-flight idempotent Produce requests.
- [x] `read_uncommitted` while transactional traffic is present.
- [x] Partition byte/record skew.
- [x] Generic retriable-error storm.
- [x] Capture-incomplete health, kept separate from client anti-patterns.

Acceptance: each detector has positive, threshold-boundary, false-positive, and
state-expiry tests.

## M7 — Linux eBPF tap and system diagnosis

- [x] PID-scoped OpenSSL `SSL_read`/`SSL_write` and `_ex` uprobe/uretprobe
      coverage for selected librdkafka-family clients.
- [x] Minimal BPF program: validate return lengths, copy bounded chunks, attach
      connection/direction sequence numbers, decode Kafka only in userspace.
- [x] Ring-buffer reservation/drop counters and stream invalidation on gaps.
- [x] Optional TCP retransmit/connect and scheduler/off-CPU diagnostics tied to
      the selected PID and broker connection.
- [x] Report BPF runtime/run-count and enforce an overhead budget in the M0
      benchmark before release.

Acceptance: no host-wide attachment by default; unsupported symbols/kernels
fail closed with an actionable status; an induced BPF ring-buffer loss marks
the affected stream incomplete; measured overhead stays within the documented
budget at the target event rate.

## Remaining environment validation gates

The implementation is complete, but release claims still require measurements
on the target environments rather than on this macOS development host:

- [x] execute a rootful CO-RE load/attach, multi-chunk delivery, explicit-loss
      invalidation, and BPF runtime-accounting smoke on Linux 6.12/libssl 3;
- [x] execute a local direct/proxy payload smoke with fixed offered arrivals;
- [ ] repeat the full M0 matrix against representative Kafka/OpenSSL/JVM
      workloads on release hardware;
- [ ] publish repeated p99/p999 and target-process CPU/RSS deltas, and keep eBPF
      opt-in unless its p99 regression is below 5% with zero capture drops.

The dated evidence and its limitations are recorded in
[`perf/2026-09-01-local-validation.md`](perf/2026-09-01-local-validation.md).

## Explicit non-goals until profiling proves otherwise

- Thread-per-core, DPDK, AF_XDP, io_uring, NUMA pinning, huge pages, SIMD, or a
  global allocator swap.
- Sampling plaintext payload events: Kafka framing requires every byte.
- Blocking Kafka traffic to make an inspector capture lossless.
