---
title: "Decrypting Kafka TLS without a proxy"
slug: kafka-tls-debug-without-proxy
date: 2026-05-22
description: Why MITM proxies fall short for modern Kafka debugging, and how an in-process boundary hook captures plaintext without breaking encryption.
keywords: [kafka tls debug, kafka ssl inspect, kafka traffic capture, mitm proxy alternative]
---

# Decrypting Kafka TLS without a proxy

Kafka debugging tools split into two camps. Topic browsers read data at rest. Wire-level proxies read it in flight, but only after breaking TLS. Fine for a local broker. Useless the moment a client talks to Confluent Cloud, MSK, or anything that looks like production.

Kapture started in the second camp. The proxy terminates TLS from the client, opens a new TLS session to the broker, decodes the frames in between. It works. The price is what you have to give up to make it work.

## What standing in the middle costs

To intercept TLS, Kapture presents a certificate the client trusts. To get there:

- A CA in the client's truststore, or `ssl.endpoint.identification.algorithm=` set to empty. Both are footguns nobody wants in a dev workflow.
- Re-termination of mTLS upstream, which hands the client's private key to the proxy. Most security teams refuse this for ten seconds, let alone for a debugging session.
- Pinning breaks. Clients that pin the broker SAN see the proxy's fake cert, the handshake fails, and the error surfaces as a generic SSL exception that has nothing to do with the actual bug.
- Confluent Cloud, Azure Event Hubs, and MSK push back hard. Their cert chains and IAM-flavored auth schemes are not happy to be MITMed.

> **Visual:** side-by-side diagram of two architectures. Left: client → Kapture (cert A) → broker (cert B). Two separate TLS sessions, two distinct cert validations, the proxy holding both private keys. Right: client → broker, single TLS session with the real broker cert, Kapture observing from inside the client process via a dotted line labeled "boundary hook." Label the left "TLS broken" and the right "TLS preserved."

The proxy earns its keep. Every senior Kafka engineer we showed it to asked the same question anyway: can you do this without standing in the middle?

## Hook the client, not the wire

Observe the application before it hands bytes to its TLS library and the bytes are still cleartext. Same coming back: after the TLS library decrypts, before the application reads. Where the application code meets the TLS code, the protocol is readable.

For Java Kafka clients, around two-thirds of production traffic, that boundary is one class: `org.apache.kafka.common.network.SslTransportLayer`. `write(ByteBuffer[], int, int)` receives the plaintext the client is about to encrypt. `read(ByteBuffer)` receives what just came out of decrypt. Two hooks, full visibility.

The tap mode is a Java agent attached with `-javaagent`. ByteBuddy instruments those two methods. Captured buffers ship over a Unix domain socket to Kapture. The agent stays inside the JVM, the TLS connection stays end-to-end between client and real broker, no second TLS session, no cert to install. mTLS, pinning, the broker's cert chain all behave as they do in production, because they still are in production.

## What the POC does

Apache Kafka broker, SSL listener on `localhost:39093`, self-signed cert, Java producer/consumer pair on `kafka-clients` 3.8.1. With the agent attached, the receiver decoded this out of the captured buffers:

```
[conn=1 W] ApiVersionsRequest v3 — client_id=jvm-tap-producer
[conn=1 R] ApiVersionsResponse — 720 bytes of advertised versions
[conn=2 W] ProduceRequest v11 — topic=tap-test, key=0, value=msg-0, header tenant=acme
[conn=2 R] ProduceResponse — partition 0, offset 30
[conn=3 R] FetchResponse v11 — 3513 bytes containing all 10 records
```

Every byte the receiver printed lined up with what the Java client sent and received over TLS. Producer reported "OK, sent 10 messages." Consumer reported "received 10/10 messages." Neither client noticed the agent.

> **Visual:** screenshot or terminal capture showing the receiver output side-by-side with the producer/consumer console output. Highlight matching `msg-0`, `tenant=acme`, and corr_id pairs across both windows.

## What it doesn't do

- Same-host only. The agent runs inside the JVM. Remote client means proxy or SSH session.
- JVM only so far. The boundary trick generalizes to `librdkafka` via `SSL_write` and `SSL_read`, but that work has not shipped.
- Dynamic attach warns on Java 21+. Premain attach does not. Use premain in dev environments shaped like production.
- The Java agent cannot detach cleanly. Once injected, the bytecode stays until the JVM restarts. eBPF probes in Pixie and Coroot carry the same constraint for different reasons.

In exchange: tap mode against a Confluent Cloud bootstrap that pins the broker SAN, no cert provisioning. Tap mode on a kafka-clients build with mTLS upstream, no private key handed to the dev tool. The handshakes match production because they _are_ production.

## Why the wire matters

Hard Kafka bugs hide on the wire. Rebalance loops, stale leaders, mismatched `api_version`, SASL sessions that drop every two hours. Logs hide all of it, a wire dump shows it in seconds. Dumping under TLS used to mean breaking TLS, which made the debug environment subtly different from the production one. The worst property a debugger can have.

The boundary hook keeps the protocol visible and changes nothing else.

---

_Next in this series: [Hooking SslTransportLayer via ByteBuddy](./02-hooking-ssl-transport-layer-bytebuddy.md) — the code that made this work, and the two traps that ate two hours of our day._
