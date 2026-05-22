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

    // suppress = Throwable.class on enter as well as exit: any exception
    // escaping into the Kafka client's hot path would degrade
    // observability into actual breakage. The enter step is trivial
    // (a position() read) but a custom ByteBuffer subclass could throw.
    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static int enter(@Advice.Argument(0) ByteBuffer dst) {
        return dst == null ? -1 : dst.position();
    }

    // onThrowable: skip capture when the wrapped read threw — the
    // buffer state on exception is undefined and a naive position()
    // diff can yield garbage or negative lengths.
    @Advice.OnMethodExit(suppress = Throwable.class, onThrowable = Throwable.class)
    public static void exit(@Advice.This Object self,
                            @Advice.Argument(0) ByteBuffer dst,
                            @Advice.Enter int oldPos,
                            @Advice.Thrown Throwable thr) {
        if (thr != null) return;
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
