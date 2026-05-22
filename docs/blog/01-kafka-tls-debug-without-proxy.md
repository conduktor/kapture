---
title: "Decrypting Kafka TLS without a proxy"
slug: kafka-tls-debug-without-proxy
date: 2026-05-22
description: Why MITM proxies fall short for modern Kafka debugging, and how an in-process boundary hook captures plaintext without breaking encryption.
keywords: [kafka tls debug, kafka ssl inspect, kafka traffic capture, mitm proxy alternative]
---

# Decrypting Kafka TLS without a proxy

Most Kafka debugging tools live in one of two worlds. Topic browsers see data at rest. Wire-level proxies see data in flight, but only if they can break TLS. Both are fine until you actually need to see what a TLS-enabled client is doing against Confluent Cloud, MSK, or any production-shaped broker.

We hit this wall with Kapture. The proxy works. It terminates TLS from the client, opens a fresh TLS connection to the broker, decodes the wire in the middle. The problem is everything you have to give up to make that work.

## What the MITM proxy costs you

To intercept TLS, Kapture has to present a certificate the client trusts. That means:

- Provisioning a CA the client's truststore accepts, or disabling cert validation locally. Both are footguns nobody wants in their dev workflow.
- Re-terminating mTLS upstream. The proxy now holds the client's private key, which most security teams won't allow even for ten minutes.
- Breaking certificate pinning. Some Kafka clients pin the broker SAN. The proxy's fake cert fails the pin check, and you get an opaque handshake error that looks nothing like the actual issue.
- Making Confluent Cloud / Azure Event Hubs / MSK harder than necessary. These platforms have specific cert chains and IAM-flavored auth that fight back when you try to MITM.

> **Visual:** side-by-side diagram of two architectures. Left: client → Kapture (cert A) → broker (cert B). Two separate TLS sessions, two distinct cert validations, the proxy holding both private keys. Right: client → broker, single TLS session with the real broker cert, Kapture observing from inside the client process via a dotted line labeled "boundary hook." Label the left "TLS broken" and the right "TLS preserved."

We use the proxy every day and it earns its keep. But every senior Kafka engineer we showed it to asked the same question: can you do this without standing in the middle?

## The alternative: hook the client, not the wire

If you observe the application _before_ it hands bytes to its TLS library, the bytes are still in cleartext. Same on the way back: after the TLS library decrypts, before the app sees the result. The boundary between the application code and the TLS code is where the protocol becomes readable.

For Java Kafka clients (which are roughly two-thirds of the production market), that boundary is exactly one class: `org.apache.kafka.common.network.SslTransportLayer`. Its `write(ByteBuffer[], int, int)` method receives the plaintext bytes the client is about to encrypt. Its `read(ByteBuffer)` method receives the bytes that just came out of the SSL decrypt. Two hooks, full visibility.

We built a small Java agent that attaches at startup with `-javaagent`, uses ByteBuddy to instrument those two methods, and ships the captured buffers over a Unix domain socket to Kapture. The agent stays inside the JVM. The TLS connection stays end-to-end between the client and the real broker. There is no second TLS session. There is no certificate to install. mTLS, pinning, and the real broker's cert chain all behave exactly as they would in production, because they are still in production mode.

## What the POC actually does

We tested against an Apache Kafka broker running with an SSL listener on `localhost:39093`, a self-signed cert, and a Java producer/consumer pair using `kafka-clients` 3.8.1. With the agent attached, here is what the receiver decoded out of the wire bytes, all in cleartext:

```
[conn=1 W] ApiVersionsRequest v3 — client_id=jvm-tap-producer
[conn=1 R] ApiVersionsResponse — 720 bytes of advertised versions
[conn=2 W] ProduceRequest v11 — topic=tap-test, key=0, value=msg-0, header tenant=acme
[conn=2 R] ProduceResponse — partition 0, offset 30
[conn=3 R] FetchResponse v11 — 3513 bytes containing all 10 records
```

Every byte the receiver printed lined up with what the Java client actually sent and received over TLS. The producer reported "OK, sent 10 messages." The consumer reported "received 10/10 messages." Neither client knew the agent was there.

> **Visual:** screenshot or terminal capture showing the receiver output side-by-side with the producer/consumer console output. Highlight matching `msg-0`, `tenant=acme`, and corr_id pairs across both windows.

## Where this breaks, and where it doesn't

The honest tradeoffs:

- Same-host only. The agent runs inside the JVM. If your client lives on a different machine than your dev box, you need either a proxy or an SSH session.
- JVM only for now. The boundary trick generalizes — `librdkafka` exposes `SSL_write` and `SSL_read` for the same purpose — but we have not shipped that yet.
- Dynamic agent attach has warnings on Java 21+. Premain (start-time attach) does not. Plan for the start-time path in production-shaped dev environments.
- The Java agent cannot detach cleanly. Once injected, the bytecode lives in the JVM until the next restart. The eBPF probes used by Pixie and Coroot have the same property for different reasons.

What it gets you in return is real. We have used the tap mode on a Confluent Cloud bootstrap that pins the broker SAN, no cert provisioning required. We have used it on a kafka-clients build with mTLS upstream, no private key shared with the dev tool. The handshakes look like production because they _are_ production.

## Why this matters for Kafka debugging

The hard Kafka bugs hide on the wire. Rebalance loops, stale leaders, mixed `api_version`, SASL session breaks on a 2-hour cadence. Every one of them is invisible from logs and trivial to spot on a wire dump. Until now, dumping the wire under TLS meant breaking the TLS. That made the dev tool's environment subtly different from the prod environment, which is the worst property a debug tool can have.

The boundary hook gives back the property we wanted from the start: see the protocol, change nothing else.

---

_Next in this series: [Hooking SslTransportLayer via ByteBuddy](./02-hooking-ssl-transport-layer-bytebuddy.md) — the code that made this work, and the two traps that ate two hours of our day._
