package io.kapture.jvmtap;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Produces 10 messages to topic {@link #TOPIC} over the SSL listener on
 * {@code localhost:39093}. Each record carries a {@code tenant=acme}
 * header so the JVM-agent tap has something non-trivial to capture
 * beyond just the key/value bytes.
 */
public final class Producer {

  static final String TOPIC = "tap-test";
  static final String BOOTSTRAP = System.getProperty("bootstrap", "localhost:39093");
  static final int N_MESSAGES = 10;

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    String truststore =
        System.getProperty("truststore",
            // Default: ../certs/ — the cert folder sitting alongside this client
            // under src-tauri/tests/fixtures/. The Rust e2e test overrides with
            // -Dtruststore=<abs-path> so this fallback only matters for `mvn exec`.
            Path.of(System.getProperty("user.dir")).resolve("../certs/client.truststore.jks")
                .normalize().toString());
    String truststorePass = System.getProperty("truststore.password", "kapture");

    String securityProtocol = System.getProperty("security.protocol", "SSL");

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "jvm-tap-producer");
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);

    if ("SSL".equals(securityProtocol)) {
      // SSL — one-way auth, the broker presents a self-signed cert that the
      // truststore vouches for. No client cert (ssl.client.auth=none broker-side).
      props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore);
      props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePass);
      props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
      // The broker cert SAN includes `localhost`, so HTTPS-style endpoint ID works.
      props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");
    }

    System.out.println("[producer] bootstrap=" + BOOTSTRAP + " truststore=" + truststore);

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < N_MESSAGES; i++) {
        String value = "msg-" + i;
        ProducerRecord<String, String> rec =
            new ProducerRecord<>(TOPIC, Integer.toString(i), value);
        rec.headers().add(new RecordHeader("tenant", "acme".getBytes(StandardCharsets.UTF_8)));
        RecordMetadata md = producer.send(rec).get();
        System.out.println(
            "[producer] sent " + value
                + " -> " + md.topic() + "-" + md.partition() + "@" + md.offset());
      }
      producer.flush();
    }
    System.out.println("[producer] OK, sent " + N_MESSAGES + " messages.");
  }
}
