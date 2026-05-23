---
title: "Why eBPF isn't needed for JVM TLS"
slug: ebpf-vs-java-agent-tls
date: 2026-05-22
description: When eBPF uprobes earn their cost for TLS observability, and when a Java agent does the same job with none of the operational tax.
keywords: [ebpf vs java agent, jvm tls capture, ebpf uprobe ssl, java instrumentation tls]
---

# Why eBPF isn't needed for JVM TLS

eBPF for TLS observability is having a moment. Pixie, Cilium, Coroot, and a long tail of newer tools all run the same playbook: attach uprobes to `SSL_write` and `SSL_read` in the OpenSSL shared object, ship the pre-encrypt / post-decrypt bytes through a BPF map, decode in userspace. Overhead is small, usually single-digit percent, and the approach scales to whatever happens to be running on the node. For a JVM Kafka client it's still the wrong tool.

## What eBPF actually buys you

eBPF earns its complexity when the target binary is opaque. A Go program that statically links `crypto/tls` exposes no stable userspace symbol to attach to without scanning the binary for function offsets, and even then you're stuck with `BPF_KRETPROBE`-style return-path tricks because Go strips frame pointers. A C++ daemon vendoring its own BoringSSL has the same shape. eBPF reaches past the application from a privileged vantage point the process can't refuse, and that is sometimes the only way in.

What you pay for that vantage point:

- `CAP_BPF`, usually `CAP_PERFMON`, often `CAP_SYS_ADMIN`. Fine on a laptop, a security review on a customer's prod node.
- Linux only. Half the Kafka client developers I know run macOS.
- Kernel version coupling. Modern uprobe features land in 5.x and 6.x. Older RHEL fleets get a different probe or none.
- The verifier. Loop bounds, pointer arithmetic, stack depth, all checked, all unforgiving. Reading verifier rejection messages is a separate skill.
- A second runtime to ship and operate: probe in C-ish BPF, loader in userspace, decoder somewhere else. Three places to change for one feature.

For a Go static binary, that tax is the price of entry. Nothing else gets plaintext out of `crypto/tls` short of recompiling.

## A JVM is not opaque

Every class is loadable. Every method has a stable name and signature. The JDK ships an Instrumentation API specifically for runtime bytecode rewriting, and ByteBuddy puts a typed advice API on top. An agent runs as the JVM user, on any OS the JVM runs on, with no kernel module, no probe loader, no verifier.

For our Kapture POC against the Kafka Java client:

- The hook points (`SslTransportLayer.write` and `.read`) are public Java methods with a stable contract.
- The plaintext bytes are already in a `ByteBuffer` we can read by name. No memory scanning, no offset hunting, no symbol resolution.
- The agent installs via `-javaagent` at startup. No privilege escalation.
- The same JAR runs on macOS, Linux, Windows.
- The advice is plain Java that the JIT compiles. Overhead measured around 0.8% on the producer's hot path.

Try to do this with eBPF and the first question is which `libssl` to hook. The JVM doesn't dynamically link OpenSSL for its TLS stack, so there is no `SSL_write` symbol to attach to. The TLS handshake state lives in Java objects on the heap, not in `libsunec.so` or the JDK's native PKCS11 bindings. A week of offset chasing replaces twenty lines of ByteBuddy advice, and the result still won't see what happens above the JCE provider boundary.

> **Visual:** decision tree. Root: "Is your target a JVM?" → Yes branch: "Use a Java agent. ByteBuddy + Instrumentation API. Done." → No branch: "Is it statically linked (Go, C++ with vendored OpenSSL)?" → Yes: "Use eBPF uprobes. AgentSight pattern." → No: "Is it dynamically linking libssl?" → Yes: "Either eBPF uprobes on libssl OR an LD_PRELOAD shim. Both work. eBPF if you want one tool across many processes." → No: "Use a proxy (and break TLS)."

## Match the tool to the target

eBPF isn't bad. It's a kernel-side observability bus for userspace code the kernel doesn't naturally see, which is exactly what you want when you have to watch many unknown processes on the same node. Pixie and Coroot built around that case for good reason: "tell me what's happening on this host, whatever's running" really is best answered from below the process.

Kapture's job is narrower. Planned coverage:

| Target                                                                  | Hook technique                          | Why                                            |
| ----------------------------------------------------------------------- | --------------------------------------- | ---------------------------------------------- |
| `kafka-clients` (Java)                                                  | ByteBuddy agent on `SslTransportLayer`  | Native JVM trick, zero kernel ask              |
| `librdkafka` (C, used by Python / Node / Ruby / .NET / Go via bindings) | eBPF uprobe on `SSL_write` / `SSL_read` | Stable libssl symbols, target is opaque        |
| Sarama (Go, static `crypto/tls`)                                        | eBPF uprobe with RET-scan               | Go strips frame pointers, need offset scanning |
| confluent-kafka-go (cgo over librdkafka)                                | Same as librdkafka                      | Bytes go through OpenSSL underneath            |

Roughly two-thirds of production Kafka traffic sits in the first row. The rest splits across the next three, and on Linux that's where eBPF carries its weight. macOS developers fall back to the proxy for non-JVM clients until we ship a key log import or an `LD_PRELOAD` shim.

## Pick the affordance the runtime already gives you

A JVM was built to be observed from inside; reaching for eBPF on it pays kernel-level costs to do what the Instrumentation API gives away. The mirror case is a ByteBuddy agent against a Go static binary: there's no agent API to attach to and the JAR doesn't help at all. Same principle, opposite answer.

For Kapture this falls out naturally. Java tap mode ships first, every OS, no privileges. librdkafka and Go tap modes ship after, Linux only, eBPF underneath. Both paths feed the same Kafka wire decoder, and users pick the tap that fits their client.

---

_Next: [Kafka wire decode end-to-end without MITM](./04-kafka-wire-decode-no-mitm.md) — three observation modes, one decoder, no broken TLS._
