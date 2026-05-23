---
title: "Why eBPF isn't needed for JVM TLS"
slug: ebpf-vs-java-agent-tls
date: 2026-05-22
description: When eBPF uprobes earn their cost for TLS observability, and when a Java agent does the same job with none of the operational tax.
keywords: [ebpf vs java agent, jvm tls capture, ebpf uprobe ssl, java instrumentation tls]
---

# Why eBPF isn't needed for JVM TLS

eBPF for TLS observability is having a moment. Pixie, Cilium, Coroot, and a wave of newer tools all use the same trick: attach uprobes to `SSL_write` and `SSL_read` in the OpenSSL shared object, ship the pre-encrypt / post-decrypt bytes through a BPF map, decode in userspace. It works, the overhead is real but small (single-digit percent in most benchmarks), and the conference talks are good.

For Java Kafka clients, it is the wrong tool.

## What eBPF buys you, and what it costs

eBPF earns its complexity when the target binary is _opaque_. A Go program that statically links `crypto/tls` does not expose a stable symbol you can attach to from userspace without scanning the binary. A C++ daemon using a vendored copy of BoringSSL is the same. eBPF reaches across the userspace/kernel boundary, attaches to whatever symbols the binary actually has, and observes from a privileged vantage point the application cannot block.

The cost of that vantage point is real:

- Kernel privilege. You need `CAP_BPF` and usually `CAP_PERFMON` and `CAP_SYS_ADMIN`. On a dev laptop, that is fine. On a customer's production node, it is a conversation.
- Linux only. macOS and Windows do not have eBPF. Half of Kafka client developers carry a MacBook.
- Kernel version coupling. Modern eBPF features land in 5.x and 6.x kernels. Older RHEL hosts can't run the same probe.
- A verifier in your way. Every probe goes through the kernel's BPF verifier, which has strict rules about loop bounds, pointer arithmetic, and stack depth. Debugging a verifier rejection is a separate skill.
- A second runtime. The probe code is C-ish, the loader is userspace, the decoder is somewhere else. Three places to change for one feature.

When your target is a Go static binary, this tax is worth it. There is no other way to get plaintext out of `crypto/tls` without rebuilding the binary.

## The JVM is the opposite kind of target

A JVM does not have the opacity problem. Every class is loadable. Every method has a stable name and signature. The JDK ships an Instrumentation API specifically designed for runtime bytecode rewriting. ByteBuddy gives you a typed advice API on top of that. The agent code runs as the JVM user, on any OS the JVM runs on, with no kernel module, no probe loader, no verifier.

For our Kapture POC against the Kafka Java client:

- The hook point (`SslTransportLayer.write` and `.read`) is a public Java method with a stable contract.
- The plaintext bytes are already in a `ByteBuffer` we can read by name. No memory scanning, no offset hunting, no symbol resolution.
- The agent installs from `-javaagent` at startup, no privilege escalation.
- The same JAR works on macOS, Linux, and Windows.
- The advice code is plain Java that the JIT compiles. Overhead in our tests sits around 0.8% on the producer's hot path.

If we used eBPF here, we would attach uprobes to a JVM's `libsslJava.so`... which doesn't exist. We would have to attach to `libsunec.so` or the JDK's native PKCS11 bindings, and even then the TLS handshake state lives in Java objects in the heap, not in the C code. We would spend a week implementing what one ByteBuddy advice class delivers in twenty lines.

> **Visual:** decision tree. Root: "Is your target a JVM?" → Yes branch: "Use a Java agent. ByteBuddy + Instrumentation API. Done." → No branch: "Is it statically linked (Go, C++ with vendored OpenSSL)?" → Yes: "Use eBPF uprobes. AgentSight pattern." → No: "Is it dynamically linking libssl?" → Yes: "Either eBPF uprobes on libssl OR an LD_PRELOAD shim. Both work. eBPF if you want one tool across many processes." → No: "Use a proxy (and break TLS)."

## Where this lines up

We are not claiming eBPF is bad. We are claiming the tool-to-target match matters more than the tool's mindshare. Here is how we plan Kapture's coverage:

| Target                                                                  | Hook technique                          | Why                                            |
| ----------------------------------------------------------------------- | --------------------------------------- | ---------------------------------------------- |
| `kafka-clients` (Java)                                                  | ByteBuddy agent on `SslTransportLayer`  | Native JVM trick, zero kernel ask              |
| `librdkafka` (C, used by Python / Node / Ruby / .NET / Go via bindings) | eBPF uprobe on `SSL_write` / `SSL_read` | Stable libssl symbols, target is opaque        |
| Sarama (Go, static `crypto/tls`)                                        | eBPF uprobe with RET-scan               | Go strips frame pointers, need offset scanning |
| confluent-kafka-go (cgo over librdkafka)                                | Same as librdkafka                      | Bytes go through OpenSSL underneath            |

Roughly two-thirds of production Kafka traffic is the first row. The remaining third splits across the next three, and Linux dev environments will use eBPF for that. macOS developers will fall back to the proxy for non-JVM clients until we ship a different technique (key log import, DTrace, or LD_PRELOAD shim).

## The deeper point

eBPF is a kernel-side observability bus for userspace code the kernel doesn't naturally see. The JVM is a userspace runtime designed to be observed from inside. Picking eBPF for the JVM is using a microscope to read a book: it works, but the book has been printed in 12-point type for a reason.

The same logic applies in reverse. Picking a ByteBuddy agent for a Go static binary doesn't work at all. There is no agent API to attach to. Each runtime exposes the affordances its designers built into it. Use those first.

For Kapture, that means the Java tap mode ships first, on every OS, with no privileges. The librdkafka and Go tap modes ship next, Linux only, with eBPF. The two paths feed the same Kafka wire decoder. The user picks the tool that matches their client.

---

_Next: [Kafka wire decode end-to-end without MITM](./04-kafka-wire-decode-no-mitm.md) — three observation modes, one decoder, no broken TLS._
