---
title: "Kafka wire decode end-to-end without MITM"
slug: kafka-wire-decode-no-mitm
date: 2026-05-22
description: Kapture is becoming a three-mode observation platform (proxy, JVM tap, eBPF tap) sharing one Kafka wire decoder. Here is the shape that emerges.
keywords: [kafka traffic capture, kafka observability, kafka wire protocol, kafka client debugging]
---

# Kafka wire decode end-to-end without MITM

Kapture started as a Kafka proxy with a wire dissector. The proxy works: point your client at `127.0.0.1:9092`, Kapture forwards to your real broker, the inspector decodes every byte. It has a ceiling, though. To intercept TLS, it has to terminate TLS. There are dev environments where "debug tool that changes the certificate chain" is too invasive. [We covered the costs in the first post](./01-kafka-tls-debug-without-proxy.md).

The plan, now that the JVM tap POC works, is to make Kapture a three-mode observation platform with one decoder underneath.

## Three modes, one wire decoder

The trick that holds this together is that Kapture's wire decoder doesn't care where the bytes come from. It takes Kafka frames, returns decoded structures with topic / partition / RTT / errors / anti-pattern signals. The source can be a proxy connection, a Java agent socket, or an eBPF ringbuf.

Three modes, same downstream:

| Mode | Where bytes come from | Where it runs | TLS posture |
|---|---|---|---|
| Proxy | TLS-terminating TCP proxy in front of the broker | Anywhere | Re-encrypts, breaks pinning |
| JVM tap | ByteBuddy agent inside the Kafka Java client | Same host as client | Untouched, client talks to real broker |
| eBPF tap | uprobes on `libssl` / `crypto/tls` symbols | Same host as client, Linux only | Untouched, single TLS session |

> **Visual:** three lanes, top-down. Each lane shows a client → broker arrow. Lane 1 (Proxy): the arrow bends through a "Kapture proxy" box that re-encrypts; the box owns the cert. Lane 2 (JVM tap): the arrow goes straight client-to-broker; a dotted line branches from inside the client box to a "Kapture" box on the side. Lane 3 (eBPF tap): same straight arrow; the branch comes from a kernel layer beneath the client. Underneath all three lanes: one wide block labeled "Kafka wire decoder" with arrows in from each lane.

## Where each mode wins

**Proxy mode** is best when:
- You don't have access to the client process (running in someone else's container, on a different host, behind a service mesh).
- The client refuses to use any custom JVM flags or load any agent (compliance reasons).
- You want chaos injection — drop connections, return error codes, fake `NOT_LEADER`. Tap modes are observation-only, the proxy is a knob.
- You want to debug TLS itself — handshake failures, cert chain errors, SASL drift. The proxy sees both sides of the handshake.

**JVM tap** is best when:
- The client is a Java Kafka app running on your machine.
- The target broker uses TLS that you cannot proxy (mTLS with cert chains you don't control, pinning, restricted CA).
- You want zero changes to the client's network config — no listener swap, no DNS rewrite, no cert install.
- You are demoing Kapture against Confluent Cloud or MSK without provisioning anything.

**eBPF tap** (planned) is best when:
- The client uses `librdkafka` (Python, Node, Ruby, .NET, the C apps), Go static binaries, or any non-JVM TLS path.
- You are on Linux with `CAP_BPF`.
- You want a single tool that catches every process on the host that talks Kafka, regardless of language.

The three are not mutually exclusive. A typical Kapture session against a polyglot client fleet might use the JVM tap for the Spring Boot service, eBPF for the Python ingester, and the proxy for the .NET admin tool running on Windows.

## What you see is the same thing

We kept the decoded output identical across modes. The Protocol tab renders the same columns: corr_id, RTT, API key, version, request size, decoded body. The Messages tab still flattens records out of `ProduceRequest` and `FetchResponse`. The Expert tab still fires on the same 25 anti-pattern detectors — overcommit, producer-per-record, rebalance loop, stale-leader producing, throttle pressure, the lot.

The only visible difference is a `source` badge per frame: `proxy`, `tap-jvm`, or `tap-ebpf`. RTT calculation differs slightly between modes. The proxy measures from `proxy ← client` to `proxy → client` (a TCP-level round trip). The tap measures from `SslTransportLayer.write` exit to the matching `SslTransportLayer.read` entry with the same corr_id (client-perceived, includes encrypt + decrypt). We document the difference where it matters.

> **Visual:** screenshot of the Kapture Protocol tab. Half the rows have a small `proxy` badge in the left column. Half have a `tap-jvm` badge. Otherwise the rows look identical: same corr_id format, same RTT column, same decoded body on the right. Caption: "Mode-agnostic UI. Wire bytes are wire bytes."

## What this unlocks for users

Three categories of users were gated by the proxy-only constraint:

1. **Confluent Cloud / MSK users with strict TLS** could not point a dev producer at `127.0.0.1:9092` without disabling cert validation. JVM tap removes the gate.

2. **Production-shape staging environments** with the same TLS / SASL / mTLS posture as prod. Provisioning a proxy with all the right certs is enough friction to skip Kapture; the agent works with whatever credentials the client already has.

3. **Multi-language fleets debugged from one laptop.** Today, a Python producer and a Java consumer need two different debug setups. With JVM tap shipping now and eBPF tap next, both show up in the same Kapture window.

Adding a fourth mode later (a `.pcap` import, an `SSLKEYLOGFILE` consumer, a Wireshark plugin export) becomes a question of writing the source adapter, not rewriting the inspector.

## What ships next

The JVM tap POC is real code in `experiments/jvm-tap/` against a real SSL-enabled Kafka broker. Next: bump ByteBuddy to support Java 25 cleanly, wire the receiver into Kapture's decoder, surface tap sessions in the Connections sidebar. After that, eBPF tap targeting `libssl` for the librdkafka family.

To play with the POC today: `run-baseline.sh` spins up an SSL broker in Docker, builds the agent, runs producer + consumer through the tap.

---

*Next: [Building dev tools that don't break TLS](./05-dev-tools-that-dont-break-tls.md) — the broader principle this POC instances.*
