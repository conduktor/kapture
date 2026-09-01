#!/usr/bin/env node
/**
 * Kapture dev consumer.
 *
 * Companion to `tools/seed.mjs`. The seed produces messages but
 * doesn't consume; this program runs a real KafkaJS consumer that
 * subscribes to the seeded topics, consumes every message, and
 * commits offsets back to the broker. The point is to exercise the
 * full consumer-coordination dance — JoinGroup / SyncGroup /
 * Heartbeat / OffsetCommit / OffsetFetch — and to leave a real
 * `__consumer_offsets` trail you can inspect with `kafka-consumer-
 * groups.sh` or Redpanda Console.
 *
 * This is a separate process from Kapture. Point `--broker` at
 * Kapture's proxy listener (normally 127.0.0.1:9092) to make this
 * consumer's traffic visible in Protocol / Session / Expert. Pointing
 * it directly at the upstream broker bypasses proxy capture; JVM tap
 * is the other observation option for a Java client.
 *
 * Usage:
 *   pnpm seed:consumer                     # Redpanda, default group id
 *   pnpm seed:consumer:kafka               # Apache Kafka stack
 *   node tools/consumer.mjs --broker 127.0.0.1:9092  # through Kapture
 *   node tools/consumer.mjs --broker host  # explicit
 */

import { Kafka, logLevel } from "kafkajs";

const args = process.argv.slice(2);
const arg = (flag, fallback) => {
  const i = args.indexOf(flag);
  return i >= 0 && args[i + 1] !== undefined ? args[i + 1] : fallback;
};

const BROKER = arg("--broker", "localhost:19092");
const GROUP_ID = arg("--group", "kapture-dev-consumer");
const TOPICS = [
  "orders.raw",
  "orders.enriched",
  "users.events",
  "orders.avro",
  "orders.jsonschema",
];

const kafka = new Kafka({
  clientId: "kapture-consumer",
  brokers: [BROKER],
  logLevel: logLevel.NOTHING,
  connectionTimeout: 5_000,
  retry: { retries: 5, initialRetryTime: 200 },
});

const consumer = kafka.consumer({
  groupId: GROUP_ID,
  // Default kafkajs auto-commit interval is 5 s — leaves a clean
  // OffsetCommit cadence in __consumer_offsets and on the wire.
});

let count = 0;
const start = Date.now();

async function main() {
  console.log(`broker:  ${BROKER}`);
  console.log(`group:   ${GROUP_ID}`);
  console.log(`topics:  ${TOPICS.join(", ")}`);
  await consumer.connect();
  for (const topic of TOPICS) {
    await consumer.subscribe({ topic, fromBeginning: false });
  }
  console.log("connected — Ctrl-C to stop");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      count++;
      // Periodic progress without flooding stdout.
      if (count % 100 === 0) {
        const elapsed = (Date.now() - start) / 1000;
        const rate = (count / elapsed).toFixed(1);
        console.log(
          `consumed ${count} (${rate}/s) — last @ ${topic}/${partition} offset ${message.offset}`,
        );
      }
    },
  });
}

const shutdown = async (sig) => {
  console.log(`\n${sig}, disconnecting…`);
  await consumer.disconnect();
  process.exit(0);
};
process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

main().catch((err) => {
  console.error("consumer failed:", err);
  process.exitCode = 1;
});
