# Kapture Linux eBPF/OpenSSL tap

This optional agent captures plaintext Kafka bytes at a selected process's
OpenSSL boundary. The BPF side does the minimum possible work: PID filtering,
return-length validation, bounded user-memory copies (16 KiB per chunk), a
per-stream sequence number, and ring-buffer submission. Kafka framing and all
decoding remain in Kapture's bounded userspace analyzer.

The implementation covers `SSL_read`, `SSL_write`, `SSL_read_ex`, and
`SSL_write_ex`. It also collects PID-scoped connect failures, best-effort TCP
retransmits, and scheduler off-CPU time. BPF program run counts and runtime are
printed when the loader exits (when supported by the kernel).

## Build on Linux

Requirements: Linux 5.17 or newer (for bounded `bpf_loop` chunking),
clang/LLVM, bpftool, libbpf development headers, libelf and zlib.
For example, on Ubuntu:

```sh
sudo apt install clang llvm bpftool libbpf-dev libelf-dev zlib1g-dev pkg-config
make -C agents/ebpf-tap
```

The build generates `vmlinux.h` from the running kernel's BTF, compiles the
CO-RE object, generates a libbpf skeleton, then links
`agents/ebpf-tap/build/kapture-ebpf-tap`.

## Use

Open Kapture, choose **Tap a Linux OpenSSL process**, and select a discovered
PID. Kapture performs the loader's `--check` preflight before it opens a capture
session. The target must already map `libssl.so`; statically linked TLS,
GnuTLS/NSS, kTLS-only traffic, stripped/missing OpenSSL symbols, or a kernel
without BTF fail closed with an actionable error.

The loader requires root or an equivalent `CAP_BPF`/`CAP_PERFMON` setup. Do not
grant those capabilities to the desktop application itself in a packaged
release; install the small loader with the narrow privilege policy appropriate
to the host.

Manual preflight is also available:

```sh
make -C agents/ebpf-tap check PID=1234 SSL=/usr/lib/x86_64-linux-gnu/libssl.so.3
```

On a disposable rootful Linux runner, exercise byte delivery, multi-chunk
reassembly, and fail-closed oversize loss against a real TLS connection:

```sh
sudo make -C agents/ebpf-tap rootful-smoke
```

A containerized runner must be privileged and share the initial PID namespace
(for example, Docker `--privileged --pid=host`). BPF helpers expose host PIDs;
without `--pid=host`, a container-local target PID cannot pass the BPF filter.

## Loss and safety contract

- Attachments are PID-scoped; there is no host-wide default.
- The BPF ring is 16 MiB and payload events are capped at 16 KiB.
- One OpenSSL call is captured in as many as 64 chunks (1 MiB). A larger call
  emits no partial data: it increments `oversize_calls` and invalidates the UDS
  session so a truncated Kafka stream can never be decoded as complete.
- Sequence numbers advance before ring reservation. A reservation failure or
  user-memory read fault therefore produces a detectable gap.
- On any gap, the loader sends a health frame and closes the UDS session.
  Kapture discards all partial reassembly state; later bytes are never appended
  to a Kafka stream containing a hole.
- Stopping Kapture signals and kills the loader, guaranteeing probe detach even
  when the target is idle.

## Overhead gate

Use `tools/perf/open-loop-producer.mjs` at the same fixed offered rate with and
without the tap. Record target CPU, Kapture/loader CPU, RSS, achieved rate,
p99/p999, BPF `runs`/`runtime_ns`, and all loss counters. Release packaging must
document its tested kernel/libssl pair and should not enable the mode by default
until the measured p99 regression stays below 5% and there are zero capture
drops at the target event rate.

This source is buildable only on Linux. The normal Kapture Rust/TypeScript test
suite remains platform-independent; Linux CI should additionally run `make`
and a rootful integration job against a small OpenSSL Kafka client.
