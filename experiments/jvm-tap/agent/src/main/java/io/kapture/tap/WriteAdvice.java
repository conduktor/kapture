package io.kapture.tap;

import net.bytebuddy.asm.Advice;

import java.nio.ByteBuffer;

/**
 * Advice for SslTransportLayer.write(ByteBuffer[] srcs).
 *
 * On entry, capture the readable region of every buffer in srcs[]. These are the
 * plaintext bytes the Kafka client is about to hand to SSLEngine.wrap().
 */
public class WriteAdvice {

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
