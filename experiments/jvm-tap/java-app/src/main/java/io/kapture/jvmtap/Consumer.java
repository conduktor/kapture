package io.kapture.jvmtap;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Consumes from {@code tap-test} on the SSL listener. Exits when 10
 * records have been seen or 10s of poll-idle time has elapsed —
 * whichever comes first. Exit code is 0 on success (>=10 records), 1
 * otherwise. {@link #BOOTSTRAP} and the truststore path mirror the
 * producer.
 */
public final class Consumer {

  static final String TOPIC = "tap-test";
  static final String BOOTSTRAP = System.getProperty("bootstrap", "localhost:39093");
  static final int N_EXPECTED = 10;
  static final Duration TOTAL_TIMEOUT = Duration.ofSeconds(10);

  public static void main(String[] args) {
    String truststore =
        System.getProperty("truststore",
            Path.of(System.getProperty("user.dir")).resolve("../certs/client.truststore.jks")
                .normalize().toString());
    String truststorePass = System.getProperty("truststore.password", "kapture");

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "jvm-tap-consumer-" + System.currentTimeMillis());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore);
    props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePass);
    props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
    props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");

    System.out.println("[consumer] bootstrap=" + BOOTSTRAP + " truststore=" + truststore);

    int seen = 0;
    long deadline = System.nanoTime() + TOTAL_TIMEOUT.toNanos();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(TOPIC));
      while (seen < N_EXPECTED && System.nanoTime() < deadline) {
        ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> r : batch) {
          String tenant = headerString(r, "tenant");
          System.out.println(
              "[consumer] got " + r.value()
                  + " key=" + r.key()
                  + " tenant=" + tenant
                  + " " + r.topic() + "-" + r.partition() + "@" + r.offset());
          seen++;
          if (seen >= N_EXPECTED) break;
        }
      }
    }

    System.out.println("[consumer] received " + seen + "/" + N_EXPECTED + " messages.");
    if (seen >= N_EXPECTED) {
      System.exit(0);
    } else {
      System.exit(1);
    }
  }

  private static String headerString(ConsumerRecord<?, ?> r, String key) {
    Header h = r.headers().lastHeader(key);
    return h == null ? "<none>" : new String(h.value(), StandardCharsets.UTF_8);
  }
}
