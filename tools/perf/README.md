# Kapture performance harness

`open-loop-producer.mjs` offers Kafka messages at fixed wall-clock arrival
times. It does not wait for an acknowledgement before scheduling the next
message, so a proxy/UI/analyzer stall remains visible in p99/p999 instead of
being hidden by a lower request rate.

## Matrix

Run each payload (`100`, `1024`, `65536`, `1048576`) with the UI live, hidden,
and paused:

```sh
node tools/perf/open-loop-producer.mjs --broker localhost:19092 --rate 1000 --duration 30 --payload-bytes 1024
node tools/perf/open-loop-producer.mjs --broker localhost:<kapture-port> --rate 1000 --duration 30 --payload-bytes 1024
```

For a transport/analyzer-only proxy run without renderer or terminal-I/O
noise, start the headless smoke with `--quiet` in a separate shell:

```sh
cargo run --manifest-path src-tauri/Cargo.toml --profile profiling \
  --example proxy_smoke -- --upstream localhost:29092 --listen 39090 \
  --seconds 60 --quiet
```

The first command is the direct baseline; the second targets Kapture's proxy.
The harness performs a small setup-only warm-up before starting its clocks so
topic auto-creation and initial metadata discovery do not contaminate the
latency distribution. Override its size with `--warmup-messages`; use the same
value for every side of a comparison.

The 1 MiB payload case needs broker/topic `max.message.bytes` above 1 MiB to
leave room for Kafka record-batch overhead (2 MiB is sufficient for this
harness). A `MESSAGE_TOO_LARGE` result is a configuration failure, not a
latency sample.

Verify the harness itself with a deliberate 500 ms scheduler stall: `offered`
must remain `rate × duration` and p99/p999 scheduling/response latency must show
the pause instead of hiding it as reduced throughput.

```sh
node tools/perf/open-loop-producer.mjs --rate 1000 --duration 10 --inject-stall-at 3 --inject-stall-ms 500
```

For JVM tap overhead, build the open-loop Java producer, then run the same
workload once without the agent, once with the agent while Kapture is stopped,
and once connected:

```sh
mvn -q -DskipTests -f tools/perf/jvm-producer/pom.xml package
java -jar tools/perf/jvm-producer/target/kapture-jvm-perf.jar \
  --broker localhost:29092 --rate 1000 --duration 30 --payload-bytes 1024
java -javaagent:agents/jvm-tap/target/kapture-jvm-agent.jar \
  --add-opens java.base/java.nio=ALL-UNNAMED \
  -jar tools/perf/jvm-producer/target/kapture-jvm-perf.jar \
  --broker localhost:29092 --rate 1000 --duration 30 --payload-bytes 1024
```

The Java driver uses a dedicated bounded dispatch thread so a blocking
`KafkaProducer.send()` cannot slow the arrival scheduler and hide coordinated
omission. It reports process CPU and peak used heap; wrap it in
`/usr/bin/time -l` (macOS) or `/usr/bin/time -v` (Linux) for process RSS.
As with the Node driver, any send failure or overload drop exits non-zero.

On Linux, repeat the direct workload with the PID-scoped OpenSSL agent from
`agents/ebpf-tap`. Capture its per-program run/runtime counters at exit in
addition to the normal latency, CPU, RSS and loss metrics.

Keep offered rate, duration, payload, acknowledgements, compression and JVM
fixed. Report the harness JSON plus Kapture CPU/RSS and capture-health counters.
Any `failed` or `overloadDrops` value makes the command exit non-zero. It is a
failed run, not throughput; `failureReasons` classifies measured send failures.

## Profiling build

Build symbols and frame pointers:

```sh
RUSTFLAGS="-C force-frame-pointers=yes" cargo build --manifest-path src-tauri/Cargo.toml --profile profiling
```

- Linux CPU: `perf record -F 999 -g --call-graph dwarf -p <kapture-pid>`;
  off-CPU/run queue: `offcputime-bpfcc -p <pid>` and `runqlat-bpfcc`.
- macOS: Instruments → Time Profiler + Allocations, targeting the profiling
  Kapture binary.
- JVM: `async-profiler -e cpu -d 30 -f cpu.html <pid>` and repeat with
  `-e alloc`; use JFR `jdk.SocketWrite` events to verify that an inactive agent
  performs no per-message UDS connect.

Record environment (OS/kernel, CPU, Kafka version, TLS, JVM, git revision) with
every result. Do not compare numbers from different machines as a regression.
