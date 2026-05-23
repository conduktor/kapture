package io.kapture.tap;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.matcher.ElementMatchers;

import java.lang.instrument.Instrumentation;

/**
 * Kapture JVM tap agent.
 *
 * Hooks both Kafka client transport layers:
 *   {@code org.apache.kafka.common.network.SslTransportLayer}        (TLS path)
 *   {@code org.apache.kafka.common.network.PlaintextTransportLayer}  (PLAINTEXT path)
 *
 * Same advice for both — the read(ByteBuffer)/write(ByteBuffer[], int, int)
 * signatures come from the shared TransportLayer interface (extends
 * GatheringByteChannel + ScatteringByteChannel). For SSL the read buffer
 * contains freshly-decrypted plaintext; for plaintext it contains the
 * wire bytes directly. The Rust listener reassembles either stream the
 * same way.
 *
 * Each captured chunk is enqueued on {@link TapPublisher}, which a single writer
 * thread drains over a Unix Domain Socket at /tmp/kapture-tap.sock.
 *
 * Required JVM flags (modern JDKs):
 *   -javaagent:/path/to/kapture-jvm-agent.jar
 *   --add-opens java.base/java.nio=ALL-UNNAMED
 */
public final class Agent {

    private Agent() {}

    public static void premain(String args, Instrumentation inst) {
        applyAgentArgs(args);
        install(inst);
    }

    public static void agentmain(String args, Instrumentation inst) {
        applyAgentArgs(args);
        install(inst);
    }

    /**
     * Parse the agent args string (the part after {@code =} in
     * {@code -javaagent:foo.jar=k=v,k2=v2}, or the second arg passed
     * to {@code VirtualMachine.loadAgent(jar, args)} on dynamic
     * attach) and copy each {@code k=v} pair to a system property.
     *
     * Done BEFORE {@link #install(Instrumentation)} so the
     * {@link TapPublisher} class-load picks up the resulting
     * properties in its static initialiser (notably
     * {@code kapture.tap.socket}). Idempotent and tolerant of an
     * empty/null args string.
     */
    /**
     * Reads advice-class bytecode straight from the agent JAR on disk.
     * Dynamic attach (agentmain) puts the agent JAR on a classloader
     * whose resource resolution can fail — we saw
     *   IllegalStateException: Could not locate class file for io.kapture.tap.ReadAdvice
     * with `ClassFileLocator.ForClassLoader.of(...)` because the
     * dynamic-attach loader path differs from the static one.
     *
     * Resolution order:
     *   1. `kapture.tap.agentJar` system property — set by the Attacher
     *      who knows the JAR path it just loaded. Bulletproof.
     *   2. ProtectionDomain CodeSource on the Agent class — works in
     *      `-javaagent` premain.
     *   3. Last-resort fallback to the classloader-based locator.
     */
    private static net.bytebuddy.dynamic.ClassFileLocator buildAdviceLocator() {
        String fromProperty = System.getProperty("kapture.tap.agentJar");
        if (fromProperty != null && !fromProperty.isEmpty()) {
            try {
                return net.bytebuddy.dynamic.ClassFileLocator.ForJarFile.of(
                        new java.io.File(fromProperty));
            } catch (Exception e) {
                System.err.println("[kapture-jvm-agent] WARN: jar locator from "
                        + "kapture.tap.agentJar=" + fromProperty + " failed: " + e.getMessage());
            }
        }
        try {
            java.security.CodeSource cs = Agent.class.getProtectionDomain().getCodeSource();
            if (cs != null && cs.getLocation() != null) {
                java.io.File selfJar = new java.io.File(cs.getLocation().toURI());
                return net.bytebuddy.dynamic.ClassFileLocator.ForJarFile.of(selfJar);
            }
        } catch (Exception e) {
            System.err.println("[kapture-jvm-agent] WARN: jar locator from CodeSource failed: "
                    + e.getMessage());
        }
        System.err.println("[kapture-jvm-agent] WARN: falling back to classloader locator");
        return net.bytebuddy.dynamic.ClassFileLocator.ForClassLoader.of(
                Agent.class.getClassLoader());
    }

    private static void applyAgentArgs(String args) {
        if (args == null || args.isEmpty()) return;
        for (String kv : args.split(",")) {
            int eq = kv.indexOf('=');
            if (eq <= 0 || eq == kv.length() - 1) continue;
            String key = kv.substring(0, eq).trim();
            String value = kv.substring(eq + 1).trim();
            if (!key.isEmpty()) {
                System.setProperty(key, value);
            }
        }
    }

    private static void install(Instrumentation inst) {
        // Boot publisher (lazy socket connect happens on first frame).
        TapPublisher.start();

        final net.bytebuddy.dynamic.ClassFileLocator adviceLocator = buildAdviceLocator();

        new AgentBuilder.Default()
                .disableClassFormatChanges()
                .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
                .with(AgentBuilder.InitializationStrategy.NoOp.INSTANCE)
                .with(AgentBuilder.TypeStrategy.Default.REDEFINE)
                .with(new AgentBuilder.Listener.Adapter() {
                    @Override public void onTransformation(net.bytebuddy.description.type.TypeDescription t,
                                                            ClassLoader cl,
                                                            net.bytebuddy.utility.JavaModule m,
                                                            boolean loaded,
                                                            net.bytebuddy.dynamic.DynamicType dt) {
                        if (t.getName().contains("TransportLayer")) {
                            System.err.println("[kapture-jvm-agent] transformed " + t.getName() + " (loaded=" + loaded + ")");
                        }
                    }
                    @Override public void onError(String typeName, ClassLoader cl,
                                                  net.bytebuddy.utility.JavaModule m,
                                                  boolean loaded, Throwable th) {
                        if (typeName.contains("TransportLayer")) {
                            System.err.println("[kapture-jvm-agent] error transforming " + typeName + ": " + th);
                        }
                    }
                })
                .ignore(ElementMatchers.nameStartsWith("net.bytebuddy."))
                .ignore(ElementMatchers.nameStartsWith("io.kapture.tap."))
                // Two transport layers, two transforms — different
                // bytecode shapes mean the right write-overload to hook
                // is per-class:
                //   * SslTransportLayer.write(BB[]) delegates to
                //     .write(BB[], int, int); hooking the 3-arg form
                //     dedups (we'd see 2x otherwise via the bridge).
                //   * PlaintextTransportLayer.write(BB[]) calls
                //     socketChannel.write() directly — bypassing the
                //     3-arg form entirely — so we MUST hook the 1-arg
                //     form there. Verified by reading kafka-clients
                //     3.x bytecode.
                .type(ElementMatchers.named("org.apache.kafka.common.network.SslTransportLayer"))
                .transform((builder, typeDescription, classLoader, module, pd) ->
                        builder
                                .visit(Advice.to(ReadAdvice.class, adviceLocator)
                                        .on(ElementMatchers.named("read")
                                                .and(ElementMatchers.takesArguments(java.nio.ByteBuffer.class))))
                                .visit(Advice.to(WriteAdvice.class, adviceLocator)
                                        .on(ElementMatchers.named("write")
                                                .and(ElementMatchers.takesArgument(0, java.nio.ByteBuffer[].class))
                                                .and(ElementMatchers.takesArguments(3))))
                )
                .type(ElementMatchers.named("org.apache.kafka.common.network.PlaintextTransportLayer"))
                .transform((builder, typeDescription, classLoader, module, pd) ->
                        builder
                                .visit(Advice.to(ReadAdvice.class, adviceLocator)
                                        .on(ElementMatchers.named("read")
                                                .and(ElementMatchers.takesArguments(java.nio.ByteBuffer.class))))
                                // PlaintextTransportLayer.write(BB[]) is the actual
                                // entry point Kafka's NetworkSend uses. Hooking the
                                // 1-arg form catches it; the 3-arg form is never
                                // called on plaintext.
                                .visit(Advice.to(WriteAdviceOneArg.class, adviceLocator)
                                        .on(ElementMatchers.named("write")
                                                .and(ElementMatchers.takesArguments(java.nio.ByteBuffer[].class))))
                )
                .installOn(inst);

        System.err.println("[kapture-jvm-agent] installed; tap socket = /tmp/kapture-tap.sock");
    }
}
