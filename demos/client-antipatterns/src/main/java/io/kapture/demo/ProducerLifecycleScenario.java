package io.kapture.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;

final class ProducerLifecycleScenario {
  private ProducerLifecycleScenario() {}

  static void run(DemoOptions options) throws Exception {
    if (options.mode() == DemoOptions.Mode.BAD) {
      runBad(options);
    } else {
      runFixed(options);
    }
  }

  private static void runBad(DemoOptions options) throws Exception {
    System.out.println("BAD: constructing, connecting and closing one KafkaProducer per record.");
    System.out.println("Every record pays ApiVersions + Metadata + InitProducerId again.\n");

    for (int i = 1; i <= options.count(); i++) {
      String clientId = "kapture-demo-producer-bad-" + String.format("%02d", i);
      try (KafkaProducer<String, String> producer =
          KafkaClients.producer(options.broker(), clientId)) {
        producer.send(DemoRecords.record(
                options.topic(), options.scenario().cliName(), options.mode().cliName(), i))
            .get(15, TimeUnit.SECONDS);
      }
      System.out.printf("  %02d/%02d producer created → record sent → producer closed%n",
          i, options.count());
    }

    System.out.println("\nExpected Kapture finding: Producer-instance leak.");
  }

  private static void runFixed(DemoOptions options) throws Exception {
    System.out.println("FIXED: constructing one KafkaProducer and reusing it for every record.");
    System.out.println("The sends share one negotiated connection and can form a real batch.\n");

    try (KafkaProducer<String, String> producer =
        KafkaClients.producer(options.broker(), "kapture-demo-producer-fixed")) {
      List<Future<RecordMetadata>> sends = new ArrayList<>(options.count());
      for (int i = 1; i <= options.count(); i++) {
        sends.add(producer.send(DemoRecords.record(
            options.topic(), options.scenario().cliName(), options.mode().cliName(), i)));
      }
      producer.flush();
      for (Future<RecordMetadata> send : sends) {
        send.get(15, TimeUnit.SECONDS);
      }
    }

    System.out.printf("  one producer → %d records → one close%n", options.count());
    System.out.println("\nExpected Kapture result: one handshake, no Producer-instance leak.");
  }
}
