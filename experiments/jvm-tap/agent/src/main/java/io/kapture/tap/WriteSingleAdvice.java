package io.kapture.tap;

import net.bytebuddy.asm.Advice;

import java.nio.ByteBuffer;

/**
 * Advice for SslTransportLayer.write(ByteBuffer src).
 *
 * Some Kafka client code paths use the single-buffer overload from the
 * WritableByteChannel API instead of the gathering ByteBuffer[] form.
 */
public class WriteSingleAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void enter(@Advice.This Object self,
                             @Advice.Argument(0) ByteBuffer src) {
        if (src == null) return;
        int n = src.remaining();
        if (n == 0) return;
        byte[] payload = new byte[n];
        ByteBuffer dup = src.duplicate();
        dup.get(payload);
        TapPublisher.capture(self, (byte) 0, payload);
    }
}
