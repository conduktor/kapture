---
title: "Building dev tools that don't break TLS"
slug: dev-tools-that-dont-break-tls
date: 2026-05-22
description: Every dev tool that intercepts TLS pays a hidden tax. Here is what we lose, what we get back when we stop interposing, and the three techniques that observe without changing the wire.
keywords: [tls visibility, passive tls decryption, dev tool design, mitm proxy alternative]
---

# Building dev tools that don't break TLS

A whole category of dev tooling exists to show you what is happening on a network. For HTTP that's Fiddler, Charles, mitmproxy. For databases, ORM-aware debuggers. For Kafka, until recently, mostly Kapture's proxy mode. They share one mechanism: they terminate TLS to read it.

That mechanism used to be cheap. It isn't anymore. This post is about why, and about three ways to observe TLS that don't touch the wire.

## The MITM tax

When a debug tool terminates TLS and opens a second TLS session to the real server, it owes five things to the system it claims to be observing.

1. A certificate the client trusts. Custom CA in the truststore, or `--insecure`, or a wildcard for `*.localhost`. Every option is friction. Every option has been a CVE somewhere.

2. Credentials the original client owned. mTLS clients need their private key reachable from the debug tool. Some compliance regimes refuse this outright, and they are right to.

3. A way around pinning. If the client pins the broker's SAN or fingerprint, the proxy's fake cert fails the pin. The failure surfaces as an opaque handshake error and you waste an afternoon on it.

4. A passable impression of production. The client now talks to a TLS endpoint with different cipher negotiation, different SANs, different OCSP behavior. The bug you came to debug may not reproduce against the proxy because the proxy isn't the thing you were debugging.

5. The loss of certificate transparency. The dev tool's cert never appears in CT logs. A client that checks CT will accept the proxy where it would have rejected a real attacker. The dev tool now succeeds in a scenario where production would have failed loudly.

> **Visual:** two columns labeled "Production wire" and "Dev tool wire." Each row is a property: cert chain, mTLS, pinning, CT, cipher negotiation, OCSP. The production column is all green. The dev tool column has yellow or red marks on most rows with one-line explanations of how each property changed.

No single item kills you. Together they make the dev tool's view of TLS a different system from the production view of TLS. When the bug lives in one of the rows that changed, you can't reproduce it. The tool that was supposed to help you debug becomes the thing you have to debug around.

## What the proxy was solving

The reasoning was straightforward. TLS is opaque from outside the endpoints, so to read what's inside, stand inside. That argument was correct in 2012. It is still correct _when there is no other way_. The interesting question is whether there's another way.

There are three.

## Technique one: cooperative key log

The client writes its TLS session keys to a file (`SSLKEYLOGFILE`). The observer reads the file and decrypts the captured wire bytes. The client has to opt in (closed-source SDKs usually won't), keys live in plaintext on disk somewhere, and the inspector turns into Wireshark.

For Kafka, no major client implements this natively. librdkafka has an open issue from 2021. Theoretical until someone files the PR.

## Technique two: in-process boundary hook

Observe inside the client, at the seam between application code and TLS code. The [Kapture JVM tap](./02-hooking-ssl-transport-layer-bytebuddy.md) sits there. TLS stays end-to-end with the real server, real cert, real handshake. Plaintext is read from inside the same process before it hits the encrypt path, or after it leaves the decrypt path.

Cost: code runs in the target process (Java agent, LD*PRELOAD shim, language plugin), same host only, same UID by default. Benefit: every TLS property is unchanged. Production cert chains, production mTLS, production pinning. Your debugger sees what the application sees, because it \_is* the application.

## Technique three: kernel-side uprobe

The kernel attaches a probe to a userspace function (eBPF uprobe on Linux, DTrace on BSDs, ETW on Windows). The probe reads the function's arguments, including buffer pointers. For libssl exposing `SSL_write` / `SSL_read`, that means plaintext without touching the client at all.

Cost: privilege (`CAP_BPF` on Linux), OS coupling, opacity for statically-linked TLS where the symbols aren't exported. Used by AgentSight, Pixie, Coroot, ecapture. TLS itself stays untouched.

> **Visual:** matrix. Rows are the three techniques. Columns: "TLS unchanged?", "Privilege needed?", "Cross-host?", "Cross-OS?", "Client cooperation?", "Real-time?". Cells either tick, cross, or have a short note. Underneath: "Pick the lowest cost that still answers your question."

## When the MITM proxy is still the right answer

Mostly when one of the other three isn't available.

- The client runs on a host you can't touch.
- The runtime offers no instrumentation API and links statically to a TLS library you can't uprobe.
- You need to _modify_ the traffic, not just watch it: chaos injection, fault testing, latency simulation. Tap modes only observe.
- The TLS layer _is_ the bug. You want two handshakes side by side, deliberately diverging.

For Kapture that means the proxy mode stays. We keep recommending it for remote debugging, chaos testing, and staging environments where the dev tool's TLS difference doesn't matter to the question being asked. Tap modes ship alongside it, not instead of it.

## The principle

The dev tools I trust observe without changing what they observe. Strace doesn't rewrite syscalls. Wireshark doesn't rewrite packets. Perf doesn't move threads around. Tools that change the system to fit the observer end up reporting on a system slightly different from the one their user runs in production, and that gap is where the unreproducible bugs live.

For TLS the principle is precise: the encrypted bytes on the wire should be the bytes that would have been there without the tool. The three techniques above preserve that. A MITM proxy explicitly breaks it.

I'm not evangelical about it. The proxy stays useful, and we still ship one. But every feature we now add to Kapture starts with the same question: can we get the signal without standing in the middle? For years the honest answer was no. With JVM tap shipping today and eBPF tap shipping next, more and more often it's yes.

A dev tool that doesn't break TLS is one that watches the TLS the application actually negotiated, with the cert the application actually presented, against the server the application actually reached. If you're looking at anything else, you're looking at the tool, not the system.

---

_This is the end of the five-part series on Kapture's tap mode. The whole series in order:_

1. _[Decrypting Kafka TLS without a proxy](./01-kafka-tls-debug-without-proxy.md)_
2. _[Hooking SslTransportLayer via ByteBuddy](./02-hooking-ssl-transport-layer-bytebuddy.md)_
3. _[Why eBPF isn't needed for JVM TLS](./03-ebpf-vs-java-agent-tls.md)_
4. _[Kafka wire decode end-to-end without MITM](./04-kafka-wire-decode-no-mitm.md)_
5. _Building dev tools that don't break TLS (this post)_
