package io.kapture.tap;

import net.bytebuddy.asm.Advice;

import java.nio.ByteBuffer;

/**
 * Advice for SslTransportLayer.write(ByteBuffer[] srcs, int offset, int length).
 *
 * On entry, capture the readable region of srcs[offset .. offset+length).
 * These are the plaintext bytes the Kafka client is about to hand to
 * SSLEngine.wrap(). Iterating the full array (the earlier shape) was a
 * correctness bug: buffers outside the (offset, length) window may be
 * partially filled from a previous gathering-write and would be copied
 * twice, corrupting the reassembled wire stream the Rust correlator
 * relies on being a strict prefix of what Kafka actually sent.
 */
public class WriteAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void enter(@Advice.This Object self,
                             @Advice.Argument(0) ByteBuffer[] srcs,
                             @Advice.Argument(1) int offset,
                             @Advice.Argument(2) int length) {
        if (srcs == null) return;
        // Defensive bounds — the gathering-channel contract says
        // [offset, offset+length) must be within [0, srcs.length],
        // but a malformed caller would otherwise crash here.
        if (offset < 0 || length <= 0 || offset > srcs.length - length) return;
        int end = offset + length;
        int total = 0;
        for (int i = offset; i < end; i++) {
            ByteBuffer b = srcs[i];
            if (b != null) total += b.remaining();
        }
        if (total == 0) return;
        byte[] payload = new byte[total];
        int off = 0;
        for (int i = offset; i < end; i++) {
            ByteBuffer b = srcs[i];
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
