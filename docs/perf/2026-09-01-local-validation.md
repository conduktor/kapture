# Local performance and eBPF validation — 2026-09-01

This is a development smoke, not a release benchmark. It proves that the
harness, bounded proxy path, Linux BPF loader, chunk delivery, and fail-closed
loss contract execute end to end. Release thresholds still require repeated
runs on representative native Linux hardware with the UI and real clients.

## Environment

- Revision: `112284b` (`main`)
- Host: macOS 26.4.1 (25E253), Apple M4 Max, arm64
- Node.js: 22.20.0
- Broker: Apache Kafka 4.3.1 in Docker Desktop
- Proxy: `profiling` profile, headless `proxy_smoke --quiet`
- BPF runner: Linux 6.12.76-linuxkit, aarch64, privileged host PID namespace
- OpenSSL: 3.0.20
- libbpf: 1.1.2

`localhost:19092` was already owned by an unrelated `kubectl port-forward`, so
the run deliberately used the repository's Apache Kafka listener on `29092`.
The 1 MiB topic used `max.message.bytes=2097152` to include record overhead.

## Harness checks

- A fresh topic completed setup before measurement; topic auto-creation did
  not leak failures into the sample.
- A forced 500 ms scheduler pause retained all 5,000 intended arrivals, with
  p99 scheduling lag at 460.39 ms and p999 at 502.06 ms.
- A forced `max-in-flight=1` overload counted 410 drops and exited non-zero.
- Every reported direct/proxy run had `failed=0` and `overloadDrops=0`.

## Direct versus headless proxy

The comparable 30-second run used 1 KiB records at 1,000 messages/second.

| Metric                 |          Direct |           Proxy |  Delta |
| ---------------------- | --------------: | --------------: | -----: |
| Offered / acknowledged | 30,000 / 30,000 | 30,000 / 30,000 | 0 loss |
| p99 response           |        1.878 ms |        1.878 ms |   0.0% |
| p999 response          |        2.139 ms |        2.543 ms | +18.9% |
| Client CPU             |         2.780 s |         2.831 s |  +1.8% |
| Client max RSS         |        89.65 MB |        90.54 MB |  +1.0% |

The proxy process captured all 30,010 messages including warm-up. Over its
35-second lifetime (30 seconds loaded), `/usr/bin/time -l` reported 0.74 s user
CPU, 1.69 s system CPU, and 24.35 MB maximum RSS.

Short payload-matrix smokes used five seconds per side except the 1 KiB row,
which used ten seconds per side:

| Payload / offered rate |  Direct p99 / p999 |   Proxy p99 / p999 | Result             |
| ---------------------- | -----------------: | -----------------: | ------------------ |
| 100 B / 1,000 s⁻¹      |   2.332 / 2.543 ms |   2.233 / 2.543 ms | 5,000/5,000 each   |
| 1 KiB / 1,000 s⁻¹      |   2.139 / 2.435 ms |   2.332 / 2.896 ms | 10,000/10,000 each |
| 64 KiB / 200 s⁻¹       |   2.435 / 2.774 ms |   2.435 / 2.896 ms | 1,000/1,000 each   |
| 1 MiB / 20 s⁻¹         | 13.193 / 15.689 ms | 13.193 / 13.193 ms | 100/100 each       |

These short rows are functional tails, not statistically stable comparisons.

## Rootful eBPF smoke

The CO-RE object and loader compiled with `-Werror`, loaded through the kernel
verifier, and attached to a PID-scoped Python/OpenSSL TLS server. The test then:

- reconstructed a 20,046-byte TLS read and a 70,048-byte TLS write from eight
  ring events, including 16 KiB multi-chunk delivery;
- observed zero ring drops and zero user-memory read faults;
- sent a distinct OpenSSL write above 1 MiB, observed `oversize_calls=1` and
  health transition `[0, 1]`, and verified that no partial oversize payload
  reached userspace;
- observed 17,748 ns of enabled BPF runtime accounting across the OpenSSL
  probes.

Docker Desktop did not expose tracefs events to the container, so connect and
scheduler tracepoints correctly remained optional and were not validated here.

## Gates still open

- Repeat each row for multiple 30-second runs on release hardware and publish
  distributions, not single samples.
- Run the UI live, hidden, and paused while collecting Kapture CPU/RSS and all
  capture-health counters.
- Measure the JVM agent stopped/disconnected/connected matrix with allocation,
  syscall, p99, and p999 profiles.
- Measure a real librdkafka/Kafka TLS workload with and without eBPF on native
  Linux; keep the feature opt-in until p99 regression is below 5% with zero
  capture loss.
- Validate optional retransmit/connect/off-CPU probes on a host exposing
  tracefs.
