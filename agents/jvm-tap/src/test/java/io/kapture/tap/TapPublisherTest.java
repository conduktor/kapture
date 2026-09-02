package io.kapture.tap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

final class TapPublisherTest {

    private static final int HEADER_SIZE = 1 + 8 + 8 + 4 + 4;
    private static final int FRAME_COUNT = 512;
    private static final int PAYLOAD_SIZE = 8 * 1024;

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void shutdownDrainsEveryQueuedFrame() throws Exception {
        Path directory = Files.createTempDirectory("kapture-publisher-test-");
        Path socketPath = directory.resolve("tap.sock");
        System.setProperty("kapture.tap.socket", socketPath.toString());

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch allowReads = new CountDownLatch(1);
        try (ServerSocketChannel server = ServerSocketChannel.open(StandardProtocolFamily.UNIX)) {
            server.bind(UnixDomainSocketAddress.of(socketPath));
            Future<List<Integer>> received = executor.submit(() -> receiveFrames(server, allowReads));

            TapPublisher.start();
            awaitConnection();
            Object owner = new Object();
            for (int index = 0; index < FRAME_COUNT; index++) {
                assertTrue(TapPublisher.tryReserve(PAYLOAD_SIZE));
                byte[] payload = new byte[PAYLOAD_SIZE];
                ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN).putInt(index);
                TapPublisher.publishReserved(owner, (byte) 0, payload);
            }

            Future<Boolean> shutdown = executor.submit(TapPublisher::shutdownAndDrain);
            awaitShutdownRequest();
            allowReads.countDown();
            assertTrue(
                    shutdown.get(5, TimeUnit.SECONDS),
                    "writer did not stop within its drain bound");
            List<Integer> frameIds = received.get(5, TimeUnit.SECONDS);
            assertEquals(FRAME_COUNT, frameIds.size());
            for (int index = 0; index < FRAME_COUNT; index++) {
                assertEquals(index, frameIds.get(index));
            }
        } finally {
            allowReads.countDown();
            TapPublisher.shutdownAndDrain();
            executor.shutdownNow();
            Files.deleteIfExists(socketPath);
            Files.deleteIfExists(directory);
        }
    }

    private static void awaitShutdownRequest() throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (System.nanoTime() < deadline) {
            if (!TapPublisher.tryReserve(1)) return;
            TapPublisher.releaseReservation(1);
            Thread.sleep(1);
        }
        throw new AssertionError("publisher did not begin shutdown");
    }

    private static void awaitConnection() throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (TapPublisher.tryReserve(1)) {
                TapPublisher.releaseReservation(1);
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("publisher did not connect to the test socket");
    }

    private static List<Integer> receiveFrames(ServerSocketChannel server, CountDownLatch allowReads)
            throws IOException, InterruptedException {
        List<Integer> frameIds = new ArrayList<>();
        try (SocketChannel channel = server.accept()) {
            allowReads.await();
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            while (readHeader(channel, header)) {
                byte direction = header.get();
                header.getLong();
                header.getLong();
                header.getInt();
                int payloadLength = header.getInt();
                ByteBuffer payload =
                        ByteBuffer.allocate(payloadLength).order(ByteOrder.LITTLE_ENDIAN);
                readFully(channel, payload);
                payload.flip();
                if (direction == 0) frameIds.add(payload.getInt());
            }
        }
        return frameIds;
    }

    private static boolean readHeader(SocketChannel channel, ByteBuffer header) throws IOException {
        header.clear();
        while (header.hasRemaining()) {
            int read = channel.read(header);
            if (read < 0) {
                if (header.position() == 0) return false;
                throw new EOFException("partial tap frame header");
            }
        }
        header.flip();
        return true;
    }

    private static void readFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            if (channel.read(buffer) < 0) throw new EOFException("partial tap frame payload");
        }
    }
}
