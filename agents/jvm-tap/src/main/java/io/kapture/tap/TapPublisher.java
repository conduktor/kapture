package io.kapture.tap;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single dedicated writer thread draining a bounded queue onto a Unix Domain Socket.
 *
 * Frame layout (little-endian):
 *   u8   direction  (0 = outgoing/write, 1 = incoming/read)
 *   u64  nanos_since_epoch
 *   u32  connection_id
 *   u32  payload_len
 *   ...  payload bytes
 *
 * Failure policy: if the socket isn't there, frames are dropped silently.
 * One stderr warning per N drops so we don't spam the host app.
 */
public final class TapPublisher {

    private static final String SOCKET_PATH =
            System.getProperty("kapture.tap.socket", "/tmp/kapture-tap.sock");

    private static final int QUEUE_CAPACITY = 8192;
    /**
     * Per-payload cap in bytes. Anything bigger is DROPPED (not
     * truncated), and the (connection, direction) reassembly stream
     * on the Rust side is signalled as desynced — see capture() below.
     *
     * Matches the Rust listener's MAX_KAFKA_FRAME_LEN (16 MiB) so a
     * legitimate single-ByteBuffer ProduceRequest up to that size goes
     * through whole. Truncating in a length-prefixed protocol
     * guaranteed corruption: the Rust side would read N bytes of a
     * frame whose prefix announced N+M bytes and graft the next
     * unrelated chunk onto the missing tail.
     */
    private static final int MAX_PAYLOAD = 16 * 1024 * 1024;

    private static final LinkedBlockingQueue<Frame> QUEUE = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private static final ConcurrentHashMap<Object, Integer> CONN_IDS = new ConcurrentHashMap<>();
    private static final AtomicInteger NEXT_ID = new AtomicInteger(1);
    private static final AtomicBoolean STARTED = new AtomicBoolean(false);
    private static final AtomicLong DROPPED = new AtomicLong();
    private static final AtomicBoolean WARNED_NO_SOCKET = new AtomicBoolean(false);

    private TapPublisher() {}

    static final class Frame {
        final byte direction;
        final long nanos;
        final int connId;
        final byte[] payload;

        Frame(byte direction, long nanos, int connId, byte[] payload) {
            this.direction = direction;
            this.nanos = nanos;
            this.connId = connId;
            this.payload = payload;
        }
    }

    public static void start() {
        if (!STARTED.compareAndSet(false, true)) return;
        Thread t = new Thread(TapPublisher::runLoop, "kapture-tap-writer");
        t.setDaemon(true);
        t.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            long d = DROPPED.get();
            if (d > 0) System.err.println("[kapture-jvm-agent] dropped frames: " + d);
        }, "kapture-tap-shutdown"));
    }

    /** Called from inlined advice — keep it tiny and exception-proof. */
    public static void capture(Object owner, byte direction, byte[] payload) {
        if (payload == null || payload.length == 0) return;
        if (payload.length > MAX_PAYLOAD) {
            // DROP the payload — do NOT truncate. The Rust listener
            // reassembles per (connection, direction) using the Kafka
            // 4-byte length prefix; a truncated payload would leave
            // it expecting the missing tail and would graft the next
            // unrelated capture onto it, corrupting all subsequent
            // frames in that stream. Counted under DROPPED so the
            // shutdown hook surfaces it; the (conn, direction) is
            // now desynced and the user should re-attach the agent.
            DROPPED.incrementAndGet();
            return;
        }
        Integer id = CONN_IDS.get(owner);
        if (id == null) {
            id = NEXT_ID.getAndIncrement();
            Integer prev = CONN_IDS.putIfAbsent(owner, id);
            if (prev != null) id = prev;
        }
        Frame f = new Frame(direction, System.nanoTime(), id, payload);
        if (!QUEUE.offer(f)) {
            DROPPED.incrementAndGet();
        }
    }

    private static void runLoop() {
        SocketChannel ch = null;
        ByteBuffer header = ByteBuffer.allocate(1 + 8 + 4 + 4).order(ByteOrder.LITTLE_ENDIAN);
        while (true) {
            Frame f;
            try {
                f = QUEUE.take();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
            if (ch == null || !ch.isOpen()) {
                ch = tryConnect();
                if (ch == null) {
                    if (WARNED_NO_SOCKET.compareAndSet(false, true)) {
                        System.err.println("[kapture-jvm-agent] tap socket not available at "
                                + SOCKET_PATH + " — dropping frames silently");
                    }
                    DROPPED.incrementAndGet();
                    continue;
                }
                WARNED_NO_SOCKET.set(false);
            }
            try {
                header.clear();
                header.put(f.direction);
                header.putLong(f.nanos);
                header.putInt(f.connId);
                header.putInt(f.payload.length);
                header.flip();
                writeFully(ch, header);
                writeFully(ch, ByteBuffer.wrap(f.payload));
            } catch (IOException ioe) {
                try { ch.close(); } catch (IOException ignored) {}
                ch = null;
                DROPPED.incrementAndGet();
            }
        }
    }

    private static SocketChannel tryConnect() {
        try {
            SocketChannel c = SocketChannel.open(StandardProtocolFamily.UNIX);
            c.connect(UnixDomainSocketAddress.of(Path.of(SOCKET_PATH)));
            c.configureBlocking(true);
            return c;
        } catch (IOException e) {
            return null;
        }
    }

    private static void writeFully(SocketChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) ch.write(buf);
    }
}
