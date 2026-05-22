---
title: "Building dev tools that don't break TLS"
slug: dev-tools-that-dont-break-tls
date: 2026-05-22
description: Every dev tool that intercepts TLS pays a hidden tax. Here is what we lose, what we get back when we stop interposing, and the three techniques that observe without changing the wire.
keywords: [tls visibility, passive tls decryption, dev tool design, mitm proxy alternative]
---

# Building dev tools that don't break TLS

There is a category of dev tool (proxies, debuggers, traffic inspectors) that exists to show you what is happening on a network. For HTTP, it's Fiddler, Charles, mitmproxy. For databases, it's various ORM-aware debuggers. For Kafka, until now, it has mostly been Kapture's proxy mode. They all share a property: they intercept TLS by terminating it.

That property used to be free. Today, it has a measurable cost. This post is about the cost, and the techniques that avoid it.

## The MITM tax

When a debug tool terminates TLS and re-establishes a second TLS session to the real server, the tool has to do five things:

1. Present a certificate the client trusts. This means a custom CA in the client's truststore, or `--insecure` mode, or a wildcard cert for `*.localhost`. Every option is friction. Every option has been a CVE somewhere.

2. Re-encrypt to the upstream server with credentials the original client owned. mTLS clients need their private key reachable from the debug tool. Some compliance regimes refuse this outright.

3. Defeat any pinning the client does. If the client pins the broker's SAN or fingerprint, the proxy's fake cert fails the pin. The failure surfaces as an opaque handshake error.

4. Mismatch the production environment subtly. The client now talks to a TLS endpoint with a different cipher suite negotiation, different SAN, different OCSP behavior. The bug you are debugging in prod may not reproduce against the proxy.

5. Lose certificate transparency. The dev tool's cert never appears in CT logs. If a client uses CT to verify the broker, the proxy is invisible to it, which means the proxy succeeds where prod would have failed.

> **Visual:** two columns labeled "Production wire" and "Dev tool wire." Each row is a property: cert chain, mTLS, pinning, CT, cipher negotiation, OCSP. The production column is all green. The dev tool column has yellow or red marks on most rows with one-line explanations of how each property changed.

None of these are deal-breakers individually. Together, they make the dev tool's view of TLS a different system from the production view of TLS. When the bug lives in one of the rows that changed, you cannot reproduce it.

## What you give up to get visibility

The reasoning behind the proxy is straightforward: TLS is opaque from the outside, so to read what is inside, you stand inside. That logic was correct in 2012. It is still correct *when there is no other way*. The new question is whether there is another way.

There are three.

## Technique one: cooperative key log

The client emits its TLS session keys to a file (`SSLKEYLOGFILE`). The observer reads the file, decrypts the captured wire bytes. The client has to opt in (closed-source SDKs often won't), keys live in plaintext somewhere, the inspector becomes Wireshark.

For Kafka, no major client implements this natively. librdkafka has an open issue from 2021. Theoretical until that changes.

## Technique two: in-process boundary hook

Observe inside the client, at the boundary between application code and TLS code. The [Kapture JVM tap](./02-hooking-ssl-transport-layer-bytebuddy.md) does this. TLS stays end-to-end with the real server, real cert, real handshake. Plaintext read from inside the same process.

Cost: needs to run code in the target process (Java agent, LD_PRELOAD shim, language plugin), same-host only, same-UID by default. The benefit: every TLS property stays unchanged. Production cert chains, production mTLS, production pinning. Your debugger sees what the application sees.

## Technique three: kernel-side uprobe

The kernel attaches a probe to a userspace function (eBPF uprobe on Linux, DTrace on BSDs, ETW on Windows). The probe reads the function's arguments, including buffer pointers. For libssl exposing `SSL_write` / `SSL_read`, plaintext without touching the client.

Cost: privilege (`CAP_BPF` on Linux), OS coupling, opacity issues for statically-linked TLS. Used by AgentSight, Pixie, Coroot, ecapture. TLS untouched.

> **Visual:** matrix. Rows are the three techniques. Columns: "TLS unchanged?", "Privilege needed?", "Cross-host?", "Cross-OS?", "Client cooperation?", "Real-time?". Cells either tick, cross, or have a short note. Underneath: "Pick the lowest cost that still answers your question."

## When the MITM proxy is still the right answer

Mostly when one of the other three is not available:

- The client runs on a host you cannot touch.
- The client's runtime offers no instrumentation API and is dynamically linked to a system libssl you cannot uprobe.
- You need to *modify* the traffic, not just observe it (chaos injection, fault testing, latency simulation). Tap modes only observe.
- The TLS bug *is* the bug you are debugging. You want to see two TLS handshakes side by side.

For Kapture, that means we keep the proxy mode and keep recommending it for remote debugging, chaos testing, and staging-like environments where the dev tool's TLS difference doesn't matter. The tap modes ship alongside, not instead.

## The deeper principle

The good dev tools observe without changing what they observe. Strace doesn't change syscalls. Wireshark doesn't change packets. Perf doesn't change scheduling. The bad ones change the system to fit the observer, and then the observer reports on a system slightly different from the one the user runs.

For TLS, "observation that changes nothing" means the encrypted bytes on the wire are exactly what they would have been without the tool. The three techniques above all preserve that property. The proxy explicitly breaks it.

We are not going to be evangelical about this. The proxy stays useful. But every time we ship a feature in Kapture, we ask the question: can we get this without standing in the middle? Most of the time the answer used to be no. With JVM tap shipping now and eBPF tap shipping next, the answer is increasingly yes.

That is what we mean by a dev tool that doesn't break TLS. Not a no-TLS tool, not a TLS-replacement tool. A tool that observes the TLS as it actually happens.

---

*This is the end of the five-part series on Kapture's tap mode. The whole series in order:*

1. *[Decrypting Kafka TLS without a proxy](./01-kafka-tls-debug-without-proxy.md)*
2. *[Hooking SslTransportLayer via ByteBuddy](./02-hooking-ssl-transport-layer-bytebuddy.md)*
3. *[Why eBPF isn't needed for JVM TLS](./03-ebpf-vs-java-agent-tls.md)*
4. *[Kafka wire decode end-to-end without MITM](./04-kafka-wire-decode-no-mitm.md)*
5. *Building dev tools that don't break TLS (this post)*
