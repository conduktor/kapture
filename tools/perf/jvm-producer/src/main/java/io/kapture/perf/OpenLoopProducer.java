package io.kapture.perf;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Fixed-arrival-rate Kafka producer used to measure JVM tap overhead. */
public final class OpenLoopProducer {
  private OpenLoopProducer() {}

  private static final class Config {
    String broker = "localhost:29092";
    String topic = "kapture.perf.jvm";
    int rate = 1_000;
    double durationSeconds = 30;
    int payloadBytes = 1_024;
    int maxInFlight = 10_000;
    int warmupMessages = 10;
    double injectStallAtSeconds = -1;
    long injectStallMillis;
  }

  private static final class LogHistogram {
    private final long[] buckets = new long[512];
    private long count;
    private double minimum = Double.POSITIVE_INFINITY;
    private double maximum;

    synchronized void recordNanos(long nanos) {
      double milliseconds = Math.max(1, nanos) / 1_000_000.0;
      double micros = Math.max(1, milliseconds * 1_000);
      int index = (int) Math.floor((Math.log(micros) / Math.log(2)) * 16);
      index = Math.max(0, Math.min(buckets.length - 1, index));
      buckets[index] += 1;
      count += 1;
      minimum = Math.min(minimum, milliseconds);
      maximum = Math.max(maximum, milliseconds);
    }

    synchronized String json() {
      return String.format(
          Locale.ROOT,
          "{\"count\":%d,\"minMs\":%.6f,\"p50Ms\":%.6f,\"p95Ms\":%.6f,"
              + "\"p99Ms\":%.6f,\"p999Ms\":%.6f,\"maxMs\":%.6f}",
          count,
          count == 0 ? 0 : minimum,
          quantile(0.50),
          quantile(0.95),
          quantile(0.99),
          quantile(0.999),
          maximum);
    }

    private double quantile(double quantile) {
      if (count == 0) return 0;
      long target = Math.max(1, (long) Math.ceil(count * quantile));
      long seen = 0;
      for (int index = 0; index < buckets.length; index += 1) {
        seen += buckets[index];
        if (seen >= target) return Math.pow(2, (index + 1) / 16.0) / 1_000;
      }
      return maximum;
    }
  }

  public static void main(String[] args) throws Exception {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
    Config config = parseArgs(args);
    byte[] value = new byte[config.payloadBytes];
    java.util.Arrays.fill(value, (byte) 'a');

    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.broker);
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, "kapture-jvm-open-loop-perf");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
    properties.put(ProducerConfig.RETRIES_CONFIG, 0);
    properties.put(ProducerConfig.LINGER_MS_CONFIG, 0);
    properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
    properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60_000);
    properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60_000);

    boolean valid;
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
      warmUp(producer, config, value);
      valid = runMeasured(producer, config, value);
    }
    if (!valid) System.exit(1);
  }

  private static void warmUp(
      KafkaProducer<byte[], byte[]> producer, Config config, byte[] value) throws Exception {
    for (int index = 0; index < config.warmupMessages; index += 1) {
      producer
          .send(new ProducerRecord<>(config.topic, null, value))
          .get(60, TimeUnit.SECONDS);
    }
  }

  private static boolean runMeasured(
      KafkaProducer<byte[], byte[]> producer, Config config, byte[] value) throws Exception {
    long target = (long) Math.floor(config.rate * config.durationSeconds);
    long intervalNanos = 1_000_000_000L / config.rate;
    LogHistogram schedulingLag = new LogHistogram();
    LogHistogram responseLatency = new LogHistogram();
    LongAdder acknowledged = new LongAdder();
    LongAdder failed = new LongAdder();
    LongAdder overloadDrops = new LongAdder();
    AtomicInteger outstanding = new AtomicInteger();
    AtomicInteger maxObservedInFlight = new AtomicInteger();
    Map<String, LongAdder> failureReasons = new ConcurrentHashMap<>();

    ThreadPoolExecutor dispatcher =
        new ThreadPoolExecutor(
            1,
            1,
            0,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(config.maxInFlight),
            runnable -> {
              Thread thread = new Thread(runnable, "kapture-perf-dispatch");
              thread.setDaemon(true);
              return thread;
            },
            new ThreadPoolExecutor.AbortPolicy());

    AtomicLong maxHeapUsed = new AtomicLong(heapUsed());
    ScheduledExecutorService sampler =
        new ScheduledThreadPoolExecutor(
            1,
            runnable -> {
              Thread thread = new Thread(runnable, "kapture-perf-memory");
              thread.setDaemon(true);
              return thread;
            });
    sampler.scheduleAtFixedRate(
        () -> maxHeapUsed.accumulateAndGet(heapUsed(), Math::max), 0, 100, TimeUnit.MILLISECONDS);

    long cpuStarted = processCpuNanos();
    long started = System.nanoTime();
    boolean stallInjected = false;
    long sequence = 0;
    while (sequence < target) {
      long now = System.nanoTime();
      if (!stallInjected
          && config.injectStallAtSeconds >= 0
          && config.injectStallMillis > 0
          && now - started >= secondsToNanos(config.injectStallAtSeconds)) {
        stallInjected = true;
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(config.injectStallMillis));
        now = System.nanoTime();
      }
      long intended = started + sequence * intervalNanos;
      if (now < intended) {
        LockSupport.parkNanos(Math.min(TimeUnit.MILLISECONDS.toNanos(1), intended - now));
        continue;
      }
      while (sequence < target && started + sequence * intervalNanos <= System.nanoTime()) {
        launch(
            producer,
            config,
            value,
            sequence,
            started + sequence * intervalNanos,
            schedulingLag,
            responseLatency,
            acknowledged,
            failed,
            overloadDrops,
            outstanding,
            maxObservedInFlight,
            failureReasons,
            dispatcher);
        sequence += 1;
      }
    }

    dispatcher.shutdown();
    if (!dispatcher.awaitTermination(60, TimeUnit.SECONDS)) {
      throw new IllegalStateException("dispatcher did not drain within 60 seconds");
    }
    long drainDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (outstanding.get() != 0 && System.nanoTime() < drainDeadline) {
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
    if (outstanding.get() != 0) {
      throw new IllegalStateException("Kafka callbacks did not drain within 60 seconds");
    }
    sampler.shutdownNow();
    long wallNanos = System.nanoTime() - started;
    long cpuNanos = Math.max(0, processCpuNanos() - cpuStarted);

    String json =
        String.format(
            Locale.ROOT,
            "{\n"
                + "  \"config\":{\"broker\":\"%s\",\"topic\":\"%s\",\"rate\":%d,"
                + "\"durationSeconds\":%.3f,\"payloadBytes\":%d,\"maxInFlight\":%d,"
                + "\"warmupMessages\":%d,\"injectStallAtSeconds\":%.3f,"
                + "\"injectStallMs\":%d},\n"
                + "  \"offered\":%d,\"acknowledged\":%d,\"failed\":%d,"
                + "\"failureReasons\":%s,\"overloadDrops\":%d,\n"
                + "  \"achievedPerSecond\":%.6f,\"wallSeconds\":%.6f,"
                + "\"maxObservedInFlight\":%d,\n"
                + "  \"responseLatency\":%s,\n"
                + "  \"schedulingLag\":%s,\n"
                + "  \"processCpuSeconds\":%.6f,\"maxHeapUsedBytes\":%d\n"
                + "}",
            jsonEscape(config.broker),
            jsonEscape(config.topic),
            config.rate,
            config.durationSeconds,
            config.payloadBytes,
            config.maxInFlight,
            config.warmupMessages,
            config.injectStallAtSeconds,
            config.injectStallMillis,
            target,
            acknowledged.sum(),
            failed.sum(),
            failureReasonsJson(failureReasons),
            overloadDrops.sum(),
            acknowledged.sum() / (wallNanos / 1_000_000_000.0),
            wallNanos / 1_000_000_000.0,
            maxObservedInFlight.get(),
            responseLatency.json(),
            schedulingLag.json(),
            cpuNanos / 1_000_000_000.0,
            maxHeapUsed.get());
    System.out.println(json);
    return failed.sum() == 0 && overloadDrops.sum() == 0;
  }

  private static void launch(
      KafkaProducer<byte[], byte[]> producer,
      Config config,
      byte[] value,
      long sequence,
      long intendedNanos,
      LogHistogram schedulingLag,
      LogHistogram responseLatency,
      LongAdder acknowledged,
      LongAdder failed,
      LongAdder overloadDrops,
      AtomicInteger outstanding,
      AtomicInteger maxObservedInFlight,
      Map<String, LongAdder> failureReasons,
      ThreadPoolExecutor dispatcher) {
    if (outstanding.incrementAndGet() > config.maxInFlight) {
      outstanding.decrementAndGet();
      overloadDrops.increment();
      return;
    }
    maxObservedInFlight.accumulateAndGet(outstanding.get(), Math::max);
    schedulingLag.recordNanos(System.nanoTime() - intendedNanos);
    try {
      dispatcher.execute(
          () -> {
            ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(
                    config.topic,
                    null,
                    null,
                    null,
                    value,
                    Collections.singletonList(
                        new RecordHeader(
                            "kapture-intended-ns",
                            Long.toString(intendedNanos).getBytes(StandardCharsets.US_ASCII))));
            try {
              producer.send(
                  record,
                  (metadata, exception) -> {
                    responseLatency.recordNanos(System.nanoTime() - intendedNanos);
                    if (exception == null) {
                      acknowledged.increment();
                    } else {
                      recordFailure(exception, failed, failureReasons);
                    }
                    outstanding.decrementAndGet();
                  });
            } catch (RuntimeException exception) {
              responseLatency.recordNanos(System.nanoTime() - intendedNanos);
              recordFailure(exception, failed, failureReasons);
              outstanding.decrementAndGet();
            }
          });
    } catch (RejectedExecutionException rejected) {
      outstanding.decrementAndGet();
      overloadDrops.increment();
    }
  }

  private static void recordFailure(
      Exception exception,
      LongAdder failed,
      Map<String, LongAdder> failureReasons) {
    failed.increment();
    failureReasons
        .computeIfAbsent(exception.getClass().getSimpleName(), ignored -> new LongAdder())
        .increment();
  }

  private static Config parseArgs(String[] args) {
    Config config = new Config();
    for (int index = 0; index < args.length; index += 1) {
      String name = args[index];
      if ("--help".equals(name) || "-h".equals(name)) {
        usage();
        System.exit(0);
      }
      if (index + 1 >= args.length) throw new IllegalArgumentException("missing value for " + name);
      String value = args[++index];
      switch (name) {
        case "--broker": config.broker = value; break;
        case "--topic": config.topic = value; break;
        case "--rate": config.rate = Integer.parseInt(value); break;
        case "--duration": config.durationSeconds = Double.parseDouble(value); break;
        case "--payload-bytes": config.payloadBytes = Integer.parseInt(value); break;
        case "--max-in-flight": config.maxInFlight = Integer.parseInt(value); break;
        case "--warmup-messages": config.warmupMessages = Integer.parseInt(value); break;
        case "--inject-stall-at": config.injectStallAtSeconds = Double.parseDouble(value); break;
        case "--inject-stall-ms": config.injectStallMillis = Long.parseLong(value); break;
        default: throw new IllegalArgumentException("unknown argument: " + name);
      }
    }
    if (config.broker.isEmpty()
        || config.topic.isEmpty()
        || config.rate <= 0
        || config.rate > 1_000_000_000
        || config.durationSeconds <= 0
        || config.rate * config.durationSeconds < 1
        || config.payloadBytes < 0
        || config.maxInFlight <= 0
        || config.warmupMessages <= 0
        || config.injectStallMillis < 0) {
      throw new IllegalArgumentException("invalid benchmark configuration");
    }
    return config;
  }

  private static void usage() {
    System.out.println(
        "Usage: java -jar kapture-jvm-perf.jar [--broker HOST:PORT] [--topic NAME] "
            + "[--rate N] [--duration SECONDS] [--payload-bytes N] [--max-in-flight N] "
            + "[--warmup-messages N] [--inject-stall-at SECONDS] [--inject-stall-ms N]");
  }

  private static long secondsToNanos(double seconds) {
    return (long) (seconds * 1_000_000_000L);
  }

  private static long heapUsed() {
    Runtime runtime = Runtime.getRuntime();
    return runtime.totalMemory() - runtime.freeMemory();
  }

  private static long processCpuNanos() {
    java.lang.management.OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
    return bean instanceof OperatingSystemMXBean
        ? ((OperatingSystemMXBean) bean).getProcessCpuTime()
        : 0;
  }

  private static String failureReasonsJson(Map<String, LongAdder> failureReasons) {
    List<String> names = new ArrayList<>(failureReasons.keySet());
    Collections.sort(names);
    StringBuilder output = new StringBuilder("{");
    for (int index = 0; index < names.size(); index += 1) {
      if (index > 0) output.append(',');
      String name = names.get(index);
      output
          .append('"')
          .append(jsonEscape(name))
          .append("\":")
          .append(failureReasons.get(name).sum());
    }
    return output.append('}').toString();
  }

  private static String jsonEscape(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
