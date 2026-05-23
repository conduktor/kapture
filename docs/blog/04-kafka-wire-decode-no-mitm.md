---
title: "Kafka wire decode end-to-end without MITM"
slug: kafka-wire-decode-no-mitm
date: 2026-05-22
description: Kapture is becoming a three-mode observation platform (proxy, JVM tap, eBPF tap) sharing one Kafka wire decoder. Here is the shape that emerges.
keywords: [kafka traffic capture, kafka observability, kafka wire protocol, kafka client debugging]
---

# Kafka wire decode end-to-end without MITM

Kapture started as a Kafka proxy with a wire dissector. Point your client at `127.0.0.1:9092`, Kapture forwards to your real broker, the inspector decodes every byte. That works, up to a point. To intercept TLS, the proxy has to terminate TLS, and in plenty of dev environments a debug tool that rewrites the certificate chain is too invasive to deploy. [The first post in this series went through the costs in detail](./01-kafka-tls-debug-without-proxy.md).

The JVM tap POC works now, so Kapture is growing two more capture modes alongside the proxy. Three modes, one decoder.

## Three modes, one wire decoder

The decoder doesn't care where the bytes come from. Feed it Kafka frames, get back decoded structures: topic, partition, RTT, errors, anti-pattern signals. The source can be a proxy connection, a Java agent socket, or an eBPF ringbuf.

| Mode     | Where bytes come from                            | Where it runs                   | TLS posture                            |
| -------- | ------------------------------------------------ | ------------------------------- | -------------------------------------- |
| Proxy    | TLS-terminating TCP proxy in front of the broker | Anywhere                        | Re-encrypts, breaks pinning            |
| JVM tap  | ByteBuddy agent inside the Kafka Java client     | Same host as client             | Untouched, client talks to real broker |
| eBPF tap | uprobes on `libssl` / `crypto/tls` symbols       | Same host as client, Linux only | Untouched, single TLS session          |

> **Visual:** three lanes, top-down. Each lane shows a client → broker arrow. Lane 1 (Proxy): the arrow bends through a "Kapture proxy" box that re-encrypts; the box owns the cert. Lane 2 (JVM tap): the arrow goes straight client-to-broker; a dotted line branches from inside the client box to a "Kapture" box on the side. Lane 3 (eBPF tap): same straight arrow; the branch comes from a kernel layer beneath the client. Underneath all three lanes: one wide block labeled "Kafka wire decoder" with arrows in from each lane.

## Where each mode wins

Proxy mode fits when:

- You don't have access to the client process. It's in someone else's container, on a different host, behind a service mesh.
- The client refuses custom JVM flags or any loaded agent (usually compliance).
- You want chaos injection. Drop connections, return error codes, fake `NOT_LEADER`. Tap modes are observation-only; the proxy is a knob.
- You're debugging TLS itself: handshake failures, cert chain errors, SASL drift. The proxy sees both sides of the handshake.

JVM tap fits when:

- The client is a Java Kafka app on your machine.
- The broker uses TLS you can't proxy: mTLS with cert chains you don't control, pinning, a restricted CA.
- You want zero changes to the client's network config. No listener swap, no DNS rewrite, no cert install.
- You're demoing against Confluent Cloud or MSK without provisioning anything.

eBPF tap (planned) fits when:

- The client uses `librdkafka` (Python, Node, Ruby, .NET, C), Go static binaries, or any non-JVM TLS path.
- You're on Linux with `CAP_BPF`.
- You want one tool that picks up every Kafka-talking process on the host, regardless of language.

The modes compose. A session against a polyglot client fleet might use JVM tap for the Spring Boot service, eBPF for the Python ingester, and the proxy for a .NET admin tool running on Windows.

## What you see is the same thing

The decoded output is identical across modes. The Protocol tab renders the same columns: corr_id, RTT, API key, version, request size, decoded body. The Messages tab still flattens records out of `ProduceRequest` and `FetchResponse`. The Expert tab still fires on the same 25 anti-pattern detectors: overcommit, producer-per-record, rebalance loop, stale-leader producing, throttle pressure, all of it.

The only visible difference is a `source` badge per frame (`proxy`, `tap-jvm`, or `tap-ebpf`) and how RTT is measured. The proxy measures `proxy ← client` to `proxy → client`, a TCP-level round trip. The tap measures `SslTransportLayer.write` exit to the matching `SslTransportLayer.read` entry with the same corr_id, which is client-perceived and includes encrypt and decrypt time. The docs flag the difference wherever it shows up.

> **Visual:** screenshot of the Kapture Protocol tab. Half the rows have a small `proxy` badge in the left column. Half have a `tap-jvm` badge. Otherwise the rows look identical: same corr_id format, same RTT column, same decoded body on the right. Caption: "Mode-agnostic UI. Wire bytes are wire bytes."

## Who the proxy-only constraint locked out

Three groups couldn't use Kapture before the JVM tap landed:

1. **Confluent Cloud / MSK users with strict TLS.** Pointing a dev producer at `127.0.0.1:9092` meant disabling cert validation. JVM tap removes that step.

2. **Production-shape staging.** Same TLS, SASL, and mTLS posture as prod. Provisioning a proxy with the right certs is enough friction that most people skip Kapture and reach for log statements. The agent reuses whatever credentials the client already has.

3. **Multi-language fleets debugged from one laptop.** A Python producer and a Java consumer need two different debug setups today. JVM tap ships now, eBPF tap is next; both surface in the same Kapture window.

Adding a fourth mode later (`.pcap` import, `SSLKEYLOGFILE` consumer, Wireshark plugin export) is a source adapter, not a decoder rewrite.

## What ships next

The JVM tap is in Kapture proper. The agent lives at `agents/jvm-tap/`, the in-process listener is `src-tauri/src/jvm_tap.rs`, and the Tauri commands `start_jvm_tap` / `stop_jvm_tap` claim the same capture slot the proxy uses. Protocol, Messages, and Expert tabs render tap-sourced frames through the existing `ProtoCorrelator`. Next up: bump ByteBuddy for clean Java 25 support, surface tap sessions in the Connections sidebar, then ship the eBPF tap against `libssl` for the librdkafka family.

To try the JVM tap today: bring up the SSL Kafka cluster with `docker compose --profile ssl up -d`, build the agent and test client (`mvn package` in `agents/jvm-tap/` and `src-tauri/tests/fixtures/jvm-test-client/`), call `start_jvm_tap`, then run the test client with `-javaagent:agents/jvm-tap/target/kapture-jvm-agent.jar`. Captured frames show up in the existing tabs.

---

_Next: [Building dev tools that don't break TLS](./05-dev-tools-that-dont-break-tls.md), the broader principle behind this POC._
