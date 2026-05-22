package io.kapture.tap;

import net.bytebuddy.asm.Advice;

import java.nio.ByteBuffer;

/**
 * Advice for SslTransportLayer.read(ByteBuffer dst).
 *
 * On entry: snapshot dst.position() (the "old" position).
 * On exit:  slice dst[oldPos .. newPos] — those are the freshly-decrypted plaintext bytes.
 */
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
        int newPos = dst.position();
        int n = newPos - oldPos;
        if (n <= 0) return;
        byte[] payload = new byte[n];
        // Copy without disturbing the buffer state.
        ByteBuffer dup = dst.duplicate();
        dup.position(oldPos);
        dup.limit(newPos);
        dup.get(payload);
        TapPublisher.capture(self, (byte) 1, payload);
    }
}
