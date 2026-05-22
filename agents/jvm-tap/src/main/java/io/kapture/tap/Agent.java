package io.kapture.tap;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.matcher.ElementMatchers;

import java.lang.instrument.Instrumentation;

/**
 * Kapture JVM tap agent.
 *
 * Hooks {@code org.apache.kafka.common.network.SslTransportLayer}:
 *   - read(ByteBuffer dst)        — OnMethodExit  → captures plaintext just decrypted into dst
 *   - write(ByteBuffer[] srcs)    — OnMethodEnter → captures plaintext before encryption
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
        install(inst);
    }

    public static void agentmain(String args, Instrumentation inst) {
        install(inst);
    }

    private static void install(Instrumentation inst) {
        // Boot publisher (lazy socket connect happens on first frame).
        TapPublisher.start();

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
                .type(ElementMatchers.named("org.apache.kafka.common.network.SslTransportLayer"))
                .transform((builder, typeDescription, classLoader, module, pd) ->
                        builder
                                // read(ByteBuffer dst) — exactly one frame per actual byte movement
                                .visit(Advice.to(ReadAdvice.class)
                                        .on(ElementMatchers.named("read")
                                                .and(ElementMatchers.takesArguments(java.nio.ByteBuffer.class))))
                                // write(ByteBuffer[], int, int) — the gathering-channel inner impl;
                                // the (BB[]) and (BB) overloads in SslTransportLayer ultimately
                                // delegate here, so matching only this signature avoids 2-3x duplicates.
                                .visit(Advice.to(WriteAdvice.class)
                                        .on(ElementMatchers.named("write")
                                                .and(ElementMatchers.takesArgument(0, java.nio.ByteBuffer[].class))
                                                .and(ElementMatchers.takesArguments(3))))
                )
                .installOn(inst);

        System.err.println("[kapture-jvm-agent] installed; tap socket = /tmp/kapture-tap.sock");
    }
}
