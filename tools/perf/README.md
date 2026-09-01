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

The first command is the direct baseline; the second targets Kapture's proxy.
For JVM tap overhead, run the same Java producer workload once without the
agent, once with the agent while Kapture is stopped, and once connected:

```sh
java -jar producer-benchmark.jar
java -javaagent:agents/jvm-tap/target/kapture-jvm-agent.jar -jar producer-benchmark.jar
```

Keep offered rate, duration, payload, acknowledgements, compression and JVM
fixed. Report the harness JSON plus Kapture CPU/RSS and capture-health counters.
An `overloadDrops` value is a failed run, not throughput.

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
