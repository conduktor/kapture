package io.kapture.demo;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

final class KafkaClients {
  private KafkaClients() {}

  static KafkaProducer<String, String> producer(String broker, String clientId) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
    // Gzip keeps the stage process dependency-free and avoids Java 25's
    // native-access warning from the optional lz4 implementation.
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
    props.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, Duration.ofMinutes(2).toMillis());
    props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15_000);
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 15_000);
    return new KafkaProducer<>(props);
  }

  static KafkaConsumer<String, String> consumer(
      String broker, String clientId, String groupId) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000);
    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3_000);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
    return new KafkaConsumer<>(props);
  }
}
