package io.kapture.tap;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Single dedicated writer thread draining a bounded queue onto a Unix Domain Socket.
 *
 * Frame layout (little-endian):
 *   u8   direction  (0 = outgoing/write, 1 = incoming/read)
 *   u64  observed_monotonic_nanos
 *   u64  emitted_monotonic_nanos
 *   u32  connection_id
 *   u32  payload_len
 *   ...  payload bytes
 *
 * The advice first reserves capacity through {@link #tryReserve(long)}. When Kapture
 * is not connected that check is a single volatile read: no payload allocation, copy,
 * connection-id lookup, or queue node is created in the application hot path.
 *
 * Once capture is active, a dropped chunk makes the Kafka byte stream impossible to
 * reassemble safely. We therefore close the UDS and clear the queue on overload/write
 * failure. Rust observes EOF and discards its partial streams before the writer
 * reconnects; it never appends a later chunk to an incomplete Kafka frame.
 */
public final class TapPublisher {

    private static final String SOCKET_PATH =
            System.getProperty("kapture.tap.socket", "/tmp/kapture-tap.sock");

    private static final int QUEUE_CAPACITY = 8192;
    private static final long QUEUE_BYTE_CAPACITY = 64L * 1024 * 1024;
    private static final int MAX_PAYLOAD = 16 * 1024 * 1024;
    private static final long MIN_RECONNECT_BACKOFF_MS = 100;
    private static final long MAX_RECONNECT_BACKOFF_MS = 5_000;
    private static final long SHUTDOWN_DRAIN_TIMEOUT_MS = 2_000;
    private static final long SHUTDOWN_FORCE_TIMEOUT_MS = 250;

    private static final ArrayBlockingQueue<Frame> QUEUE =
            new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private static final AtomicLong QUEUED_BYTES = new AtomicLong();
    private static final Map<Object, Integer> CONN_IDS =
            Collections.synchronizedMap(new WeakHashMap<>());
    private static final AtomicInteger NEXT_ID = new AtomicInteger(1);
    private static final AtomicBoolean STARTED = new AtomicBoolean(false);
    private static final AtomicBoolean CONNECTED = new AtomicBoolean(false);
    private static final AtomicBoolean RESET_REQUIRED = new AtomicBoolean(false);
    private static final AtomicBoolean SHUTDOWN_REQUESTED = new AtomicBoolean(false);
    private static final AtomicBoolean SHUTDOWN_REPORTED = new AtomicBoolean(false);
    private static final AtomicLong DROPPED = new AtomicLong();
    private static final AtomicBoolean WARNED_NO_SOCKET = new AtomicBoolean(false);
    private static final AtomicReference<SocketChannel> ACTIVE_CHANNEL = new AtomicReference<>();
    private static volatile Thread writerThread;

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
        Thread writer = new Thread(TapPublisher::runLoop, "kapture-tap-writer");
        writer.setDaemon(true);
        writerThread = writer;
        Runtime.getRuntime().addShutdownHook(
                new Thread(TapPublisher::shutdownAndDrain, "kapture-tap-shutdown"));
        writer.start();
    }

    /**
     * Reserve the bytes before advice allocates its copy. Returns false while capture
     * is inactive or when the bounded byte budget cannot accept the complete chunk.
     */
    public static boolean tryReserve(long payloadLength) {
        if (!CONNECTED.get() || payloadLength <= 0) return false;
        if (payloadLength > MAX_PAYLOAD) {
            poisonStream();
            return false;
        }

        while (true) {
            long current = QUEUED_BYTES.get();
            if (current > QUEUE_BYTE_CAPACITY - payloadLength) {
                poisonStream();
                return false;
            }
            if (QUEUED_BYTES.compareAndSet(current, current + payloadLength)) return true;
        }
    }

    /** Complete a reservation made by {@link #tryReserve(long)}. */
    public static void publishReserved(Object owner, byte direction, byte[] payload) {
        if (payload == null || payload.length == 0) return;
        if (!CONNECTED.get() || RESET_REQUIRED.get()) {
            releaseReservation(payload.length);
            return;
        }

        Frame frame = new Frame(direction, System.nanoTime(), connectionId(owner), payload);
        if (!QUEUE.offer(frame)) {
            releaseReservation(payload.length);
            poisonStream();
        }
    }

    /** Release a reservation when advice could not finish its payload copy. */
    public static void releaseReservation(long payloadLength) {
        if (payloadLength > 0) QUEUED_BYTES.addAndGet(-payloadLength);
    }

    private static int connectionId(Object owner) {
        synchronized (CONN_IDS) {
            Integer id = CONN_IDS.get(owner);
            if (id != null) return id;
            int newId = NEXT_ID.getAndIncrement();
            CONN_IDS.put(owner, newId);
            return newId;
        }
    }

    private static void poisonStream() {
        DROPPED.incrementAndGet();
        CONNECTED.set(false);
        RESET_REQUIRED.set(true);
    }

    private static void runLoop() {
        SocketChannel ch = null;
        long reconnectBackoffMs = MIN_RECONNECT_BACKOFF_MS;
        ByteBuffer header = ByteBuffer.allocate(1 + 8 + 8 + 4 + 4).order(ByteOrder.LITTLE_ENDIAN);

        while (!Thread.currentThread().isInterrupted()) {
            if (SHUTDOWN_REQUESTED.get() && QUEUE.isEmpty()) break;

            if (RESET_REQUIRED.getAndSet(false)) {
                CONNECTED.set(false);
                closeTrackedChannel(ch);
                ch = null;
                clearQueue();
            }

            if (ch == null || !ch.isOpen()) {
                if (SHUTDOWN_REQUESTED.get()) {
                    clearQueue();
                    break;
                }
                ch = tryConnect();
                if (ch == null) {
                    CONNECTED.set(false);
                    if (WARNED_NO_SOCKET.compareAndSet(false, true)) {
                        System.err.println("[kapture-jvm-agent] tap socket not available at "
                                + SOCKET_PATH + " — capture inactive; reconnecting in background");
                    }
                    try {
                        Thread.sleep(reconnectBackoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    reconnectBackoffMs = Math.min(MAX_RECONNECT_BACKOFF_MS, reconnectBackoffMs * 2);
                    continue;
                }
                ACTIVE_CHANNEL.set(ch);
                reconnectBackoffMs = MIN_RECONNECT_BACKOFF_MS;
                WARNED_NO_SOCKET.set(false);
                try {
                    writeHealth(ch, header);
                } catch (IOException ioe) {
                    closeTrackedChannel(ch);
                    ch = null;
                    continue;
                }
                if (!SHUTDOWN_REQUESTED.get()) CONNECTED.set(true);
            }

            Frame frame;
            try {
                frame = QUEUE.poll(250, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
            if (frame == null) continue;
            releaseReservation(frame.payload.length);

            try {
                header.clear();
                header.put(frame.direction);
                header.putLong(frame.nanos);
                header.putLong(System.nanoTime());
                header.putInt(frame.connId);
                header.putInt(frame.payload.length);
                header.flip();
                writeFully(ch, new ByteBuffer[] {header, ByteBuffer.wrap(frame.payload)});
            } catch (IOException ioe) {
                DROPPED.incrementAndGet();
                CONNECTED.set(false);
                closeTrackedChannel(ch);
                ch = null;
                clearQueue();
            }
        }

        CONNECTED.set(false);
        closeTrackedChannel(ch);
        clearQueue();
    }

    /**
     * Stop accepting new frames and give the writer a bounded window to flush the
     * queue. Closing the active channel after the deadline unblocks a stalled write;
     * any abandoned frames are then counted as drops rather than silently lost.
     */
    static boolean shutdownAndDrain() {
        CONNECTED.set(false);
        SHUTDOWN_REQUESTED.set(true);

        Thread writer = writerThread;
        boolean stopped = writer == null || !writer.isAlive();
        if (!stopped && writer != Thread.currentThread()) {
            stopped = joinWriter(writer, SHUTDOWN_DRAIN_TIMEOUT_MS);
            if (!stopped) {
                closeQuietly(ACTIVE_CHANNEL.getAndSet(null));
                writer.interrupt();
                stopped = joinWriter(writer, SHUTDOWN_FORCE_TIMEOUT_MS);
            }
        }
        if (!stopped) clearQueue();
        reportDropsOnce();
        return stopped;
    }

    private static boolean joinWriter(Thread writer, long timeoutMillis) {
        try {
            writer.join(timeoutMillis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        return !writer.isAlive();
    }

    private static void reportDropsOnce() {
        if (!SHUTDOWN_REPORTED.compareAndSet(false, true)) return;
        long dropped = DROPPED.get();
        if (dropped > 0) {
            System.err.println("[kapture-jvm-agent] dropped frames: " + dropped);
        }
    }

    private static SocketChannel tryConnect() {
        try {
            SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX);
            channel.connect(UnixDomainSocketAddress.of(Path.of(SOCKET_PATH)));
            channel.configureBlocking(true);
            return channel;
        } catch (IOException e) {
            return null;
        }
    }

    private static void writeFully(SocketChannel ch, ByteBuffer[] buffers) throws IOException {
        while (buffers[0].hasRemaining() || buffers[1].hasRemaining()) ch.write(buffers);
    }

    /** Report losses accumulated since the previous successful session. */
    private static void writeHealth(SocketChannel ch, ByteBuffer header) throws IOException {
        long drops = DROPPED.get();
        long now = System.nanoTime();
        ByteBuffer payload = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        payload.putLong(drops).flip();
        header.clear();
        header.put((byte) 2);
        header.putLong(now);
        header.putLong(System.nanoTime());
        header.putInt(0);
        header.putInt(Long.BYTES);
        header.flip();
        writeFully(ch, new ByteBuffer[] {header, payload});
        if (drops > 0) DROPPED.addAndGet(-drops);
    }

    private static void clearQueue() {
        Frame frame;
        while ((frame = QUEUE.poll()) != null) {
            releaseReservation(frame.payload.length);
            DROPPED.incrementAndGet();
        }
    }

    private static void closeQuietly(SocketChannel ch) {
        if (ch == null) return;
        try {
            ch.close();
        } catch (IOException ignored) {
            // Best effort on the telemetry-only path.
        }
    }

    private static void closeTrackedChannel(SocketChannel ch) {
        if (ch == null) return;
        ACTIVE_CHANNEL.compareAndSet(ch, null);
        closeQuietly(ch);
    }
}
