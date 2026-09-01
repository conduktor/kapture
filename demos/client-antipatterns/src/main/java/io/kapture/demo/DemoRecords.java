package io.kapture.demo;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

final class DemoRecords {
  private DemoRecords() {}

  static ProducerRecord<String, String> record(
      String topic, String scenario, String mode, int sequence) {
    String value = """
        {"scenario":"%s","mode":"%s","sequence":%d,"createdAt":"%s"}
        """.formatted(scenario, mode, sequence, Instant.now()).trim();
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, "conference-demo", value);
    record.headers().add(header("demo-scenario", scenario));
    record.headers().add(header("demo-mode", mode));
    record.headers().add(header("demo-sequence", Integer.toString(sequence)));
    return record;
  }

  private static RecordHeader header(String name, String value) {
    return new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8));
  }
}
