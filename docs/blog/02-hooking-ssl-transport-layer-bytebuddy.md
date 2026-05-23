---
title: "Hooking SslTransportLayer via ByteBuddy"
slug: hooking-ssl-transport-layer-bytebuddy
date: 2026-05-22
description: A concrete walkthrough of instrumenting the Kafka Java client's TLS boundary with ByteBuddy, including the two overload traps that nearly killed the POC.
keywords:
  [
    java agent ssl interception,
    bytebuddy tls,
    kafka client instrumentation,
    java instrumentation api,
  ]
---

# Hooking SslTransportLayer via ByteBuddy

This post is the deep-technical companion to [Decrypting Kafka TLS without a proxy](./01-kafka-tls-debug-without-proxy.md). Same POC, this time with the code that actually made it work and the two missteps that cost us the most time.

## Choosing where to hook

There are three reasonable places to intercept Kafka client TLS:

| Hook point                                          | What you see                                           | What you don't                    |
| --------------------------------------------------- | ------------------------------------------------------ | --------------------------------- |
| `SSLEngine.wrap` / `unwrap` (JDK)                   | All TLS traffic from any consumer of the engine        | Application-level framing context |
| Socket layer (`SocketChannel.read/write`)           | Encrypted bytes                                        | Plaintext                         |
| `org.apache.kafka.common.network.SslTransportLayer` | Plaintext Kafka wire bytes, scoped to the Kafka client | Anything outside the Kafka client |

We picked `SslTransportLayer`. It is the narrowest possible cut: Kafka traffic only, plaintext already, single class to instrument, stable surface across kafka-clients releases since 2.x. Hooking `SSLEngine` would have given us every TLS user in the JVM (the JMX console, the Schema Registry HTTP client, anything else), which is more visibility than we want and slower to filter.

> **Visual:** a JVM call stack diagram. Bottom: `SocketChannel.write(ByteBuffer)` — encrypted bytes. Middle: `SSLEngine.wrap(ByteBuffer)` — the encryption boundary. Top: `SslTransportLayer.write(ByteBuffer[], int, int)` — the plaintext entry point. Highlight the top frame as the hook target.

## What ByteBuddy is doing

ByteBuddy is a bytecode rewriter. Combined with the JDK's Java Instrumentation API, it can redefine classes at load time or retransform classes already loaded. We use it to insert two pieces of advice (entry and exit code) into the methods we care about, without modifying their source or recompiling anything.

The core of the agent is one `AgentBuilder` call wired from a `premain` entry point:

```java
public static void premain(String args, Instrumentation inst) {
    TapPublisher.start(); // boots the UDS writer thread

    new AgentBuilder.Default()
        .disableClassFormatChanges()
        .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
        .with(AgentBuilder.InitializationStrategy.NoOp.INSTANCE)
        .with(AgentBuilder.TypeStrategy.Default.REDEFINE)
        .ignore(nameStartsWith("net.bytebuddy."))
        .ignore(nameStartsWith("io.kapture.tap."))
        .type(named("org.apache.kafka.common.network.SslTransportLayer"))
        .transform((builder, type, cl, module, pd) ->
            builder
                .visit(Advice.to(ReadAdvice.class)
                    .on(named("read").and(takesArguments(ByteBuffer.class))))
                .visit(Advice.to(WriteAdvice.class)
                    .on(named("write")
                        .and(takesArgument(0, ByteBuffer[].class))
                        .and(takesArguments(3))))
        )
        .installOn(inst);
}
```

The `ReadAdvice` class captures the buffer after `read` returns. The `WriteAdvice` class captures the buffer array before `write` runs. Both push captured bytes onto a bounded queue that a dedicated writer thread drains to a Unix domain socket.

## Trap one: which `write` overload do you actually hook

`SslTransportLayer` exposes three `write` methods, inherited from `GatheringByteChannel`:

```
public int  write(ByteBuffer src)
public long write(ByteBuffer[] srcs)
public long write(ByteBuffer[] srcs, int offset, int length)
```

The Kafka client calls `write(srcs, 0, length)` from `KafkaChannel.write()`. Internally, the `(ByteBuffer[])` overload delegates to the three-arg form. The single-buffer form is rarely used by Kafka client code itself but fires inside the TLS wrap loop.

We started by hooking all three. The receiver dumped every frame two or three times in a row. Each duplicate had the exact same byte content. The agent was firing on the public method, on the delegated method, and on the inner loop method. Same data, three captures.

The fix: hook only the three-argument form, the one that actually does the work:

```java
.on(named("write")
    .and(takesArgument(0, ByteBuffer[].class))
    .and(takesArguments(3)))
```

Before: 90 captured frames for a 10-message producer. After: 14. The 14 number is correct: three handshake RPCs, then one Produce per message after batching settles.

> **Visual:** before/after timeline of captured frames. Before: 30 unique writes shown as triple-stacked bars. After: 14 single bars. Title: "Pick the inner-most method that does the actual byte movement."

## Trap two: ByteBuddy 1.14.x does not support Java 25

We are on a JDK 25 dev box. ByteBuddy 1.14.19, the latest stable at the time of writing, officially supports up to Java 23. The premain installed fine and printed our "installed" banner. The first time `SslTransportLayer` loaded, ByteBuddy threw:

```
java.lang.IllegalArgumentException: Java 25 (69) is not supported by the current
version of Byte Buddy which officially supports Java 23 (67) — update Byte
Buddy or set io.kapture.tap.shaded.bytebuddy.experimental as a VM property
```

Silently, because we had no `AgentBuilder.Listener` wired up. The class loaded as if no instrumentation had been requested. Hours of "why does the matcher not fire" later, we added a listener:

```java
.with(new AgentBuilder.Listener.Adapter() {
    @Override public void onError(String typeName, ClassLoader cl,
                                   JavaModule m, boolean loaded, Throwable th) {
        if (typeName.contains("TransportLayer")) {
            System.err.println("[agent] error on " + typeName + ": " + th);
        }
    }
})
```

The first run after that change told us exactly what was wrong. Two fixes work: bump to ByteBuddy 1.15+ when it ships full Java 25 support, or pass `-Dio.kapture.tap.shaded.bytebuddy.experimental=true` until then.

> **Visual:** a stylized terminal showing the silent failure (transformed message missing, no warning) on top, and the loud failure (error listener firing with full diagnostic) on the bottom. Caption: "Always wire a Listener.onError on AgentBuilder. Always."

## What the advice looks like

`ReadAdvice` runs at method exit and slices the freshly-decrypted bytes out of the destination buffer:

```java
public class ReadAdvice {
    @Advice.OnMethodEnter
    public static int enter(@Advice.Argument(0) ByteBuffer dst) {
        return dst == null ? -1 : dst.position();
    }

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void exit(@Advice.This Object self,
                            @Advice.Argument(0) ByteBuffer dst,
                            @Advice.Enter int oldPos) {
        if (dst == null || oldPos < 0) return;
        int n = dst.position() - oldPos;
        if (n <= 0) return;
        byte[] payload = new byte[n];
        ByteBuffer dup = dst.duplicate();
        dup.position(oldPos);
        dup.limit(oldPos + n);
        dup.get(payload);
        TapPublisher.capture(self, (byte) 1, payload);
    }
}
```

Two things matter here. First, `dst.duplicate()` so the advice never modifies the original buffer's position. Second, `suppress = Throwable.class` so a bug in our code never bubbles up into the client's hot path. The Kafka client must never see an exception that came from observation code.

## What is left on the floor

The agent has a bounded 8192-frame queue and a dedicated writer thread, enough for localhost. High-throughput brokers will need batching or shared memory. Next steps: bump ByteBuddy, replace the toy Rust receiver with Kapture's wire decoder, surface tap sessions in the Protocol tab with a `source: tap` badge.

---

_Next: [Why eBPF isn't needed for JVM TLS](./03-ebpf-vs-java-agent-tls.md) — and where it actually is the right tool._
