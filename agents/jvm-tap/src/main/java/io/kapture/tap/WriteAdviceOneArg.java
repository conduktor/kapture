package io.kapture.tap;

import net.bytebuddy.asm.Advice;

import java.nio.ByteBuffer;

/**
 * Advice for {@code PlaintextTransportLayer.write(ByteBuffer[] srcs)}.
 *
 * Variant of {@link WriteAdvice} for the 1-arg gathering-write form.
 * Used only on {@link org.apache.kafka.common.network.PlaintextTransportLayer}
 * because that class's 1-arg method calls {@code socketChannel.write(srcs)}
 * directly — it does NOT delegate to the 3-arg form like
 * {@code SslTransportLayer} does. Hooking the 3-arg form there would
 * miss every write. See {@code Agent.java} for the per-class split.
 *
 * Captures the union of {@code srcs[*].remaining()} bytes on entry,
 * before they go out the wire. Same shape as {@link WriteAdvice}
 * minus the offset/length window because there is none in the 1-arg
 * overload.
 */
public class WriteAdviceOneArg {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void enter(@Advice.This Object self,
                             @Advice.Argument(0) ByteBuffer[] srcs) {
        if (srcs == null) return;
        int total = 0;
        for (ByteBuffer b : srcs) {
            if (b != null) total += b.remaining();
        }
        if (total == 0) return;
        byte[] payload = new byte[total];
        int off = 0;
        for (ByteBuffer b : srcs) {
            if (b == null) continue;
            int n = b.remaining();
            if (n == 0) continue;
            ByteBuffer dup = b.duplicate();
            dup.get(payload, off, n);
            off += n;
        }
        TapPublisher.capture(self, (byte) 0, payload);
    }
}
