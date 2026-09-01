package io.kapture.demo;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;

final class OffsetCommitScenario {
  private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);
  private static final Duration ASSIGNMENT_TIMEOUT = Duration.ofSeconds(15);
  private static final Duration CONSUME_TIMEOUT = Duration.ofSeconds(20);

  private OffsetCommitScenario() {}

  static void run(DemoOptions options) throws Exception {
    try (KafkaProducer<String, String> seeder =
            KafkaClients.producer(options.broker(), "kapture-demo-offset-seeder");
        KafkaConsumer<String, String> consumer = KafkaClients.consumer(
            options.broker(),
            "kapture-demo-consumer-" + options.mode().cliName(),
            options.groupId())) {
      // Create the topic before subscribing, then establish the group's
      // position at the current end so this run consumes exactly its own records.
      seeder.send(DemoRecords.record(
              options.topic(), options.scenario().cliName(), "warmup", 0))
          .get(15, TimeUnit.SECONDS);
      seeder.flush();

      consumer.subscribe(List.of(options.topic()));
      awaitAssignment(consumer);
      // A rehearsal may reuse this stable group id after other modes have
      // appended records. Ignore every previous offset explicitly so each
      // run processes only the records it is about to seed.
      consumer.seekToEnd(consumer.assignment());
      // seekToEnd is lazy: force each position lookup now, before new records
      // exist, or a later poll could resolve "end" after seeding and skip the
      // very records this run is meant to consume.
      for (TopicPartition partition : consumer.assignment()) {
        consumer.position(partition);
      }

      for (int i = 1; i <= options.count(); i++) {
        seeder.send(DemoRecords.record(
            options.topic(), options.scenario().cliName(), options.mode().cliName(), i));
      }
      seeder.flush();

      if (options.mode() == DemoOptions.Mode.BAD) {
        consumeAndCommitEveryRecord(consumer, options);
      } else {
        consumeAndCommitOnce(consumer, options);
      }
    }
  }

  private static void awaitAssignment(KafkaConsumer<String, String> consumer) {
    long deadline = System.nanoTime() + ASSIGNMENT_TIMEOUT.toNanos();
    while (consumer.assignment().isEmpty() && System.nanoTime() < deadline) {
      consumer.poll(POLL_TIMEOUT);
    }
    if (consumer.assignment().isEmpty()) {
      throw new IllegalStateException("consumer group did not receive an assignment within 15s");
    }
  }

  private static void consumeAndCommitEveryRecord(
      KafkaConsumer<String, String> consumer, DemoOptions options) {
    System.out.println("BAD: processing records and calling commitSync() after every one.");
    System.out.println("Each application record produces its own OffsetCommit request.\n");

    int consumed = 0;
    long deadline = System.nanoTime() + CONSUME_TIMEOUT.toNanos();
    while (consumed < options.count() && System.nanoTime() < deadline) {
      ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
      for (ConsumerRecord<String, String> record : records) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        consumer.commitSync(Map.of(partition, new OffsetAndMetadata(record.offset() + 1)));
        consumed++;
        System.out.printf("  %02d/%02d record processed → commitSync()%n",
            consumed, options.count());
        if (consumed == options.count()) {
          break;
        }
      }
    }
    requireAllRecords(consumed, options.count());
    System.out.println("\nExpected Kapture finding: Overcommit.");
  }

  private static void consumeAndCommitOnce(
      KafkaConsumer<String, String> consumer, DemoOptions options) {
    System.out.println("FIXED: processing the same records and committing the batch once.");
    System.out.println("Application progress and broker commits are no longer one-to-one.\n");

    int consumed = 0;
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    long deadline = System.nanoTime() + CONSUME_TIMEOUT.toNanos();
    while (consumed < options.count() && System.nanoTime() < deadline) {
      ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
      for (ConsumerRecord<String, String> record : records) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        offsets.put(partition, new OffsetAndMetadata(record.offset() + 1));
        consumed++;
        if (consumed == options.count()) {
          break;
        }
      }
    }
    requireAllRecords(consumed, options.count());
    consumer.commitSync(offsets);
    System.out.printf("  %d records processed → 1 commitSync()%n", consumed);
    System.out.println("\nExpected Kapture result: one OffsetCommit, no Overcommit finding.");
  }

  private static void requireAllRecords(int consumed, int expected) {
    if (consumed != expected) {
      throw new IllegalStateException(
          "timed out after consuming " + consumed + "/" + expected + " demo records");
    }
  }
}
