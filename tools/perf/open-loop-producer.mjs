#!/usr/bin/env node

/**
 * Fixed-arrival-rate Kafka producer for Kapture performance checks.
 * Scheduling never waits for the previous send: broker/proxy stalls
 * therefore appear in the response-latency tail instead of silently
 * lowering the offered rate (coordinated omission).
 */
import { performance } from "node:perf_hooks";
import { Kafka, logLevel } from "kafkajs";

const argv = process.argv.slice(2);
const arg = (name, fallback) => {
  const index = argv.indexOf(name);
  return index >= 0 && argv[index + 1] !== undefined ? argv[index + 1] : fallback;
};

const broker = arg("--broker", "localhost:19092");
const topic = arg("--topic", "kapture.perf");
const rate = Number(arg("--rate", "1000"));
const durationSeconds = Number(arg("--duration", "30"));
const payloadBytes = Number(arg("--payload-bytes", "1024"));
const maxInFlight = Number(arg("--max-in-flight", "10000"));

if (
  !Number.isFinite(rate) ||
  rate <= 0 ||
  !Number.isFinite(durationSeconds) ||
  durationSeconds <= 0 ||
  !Number.isSafeInteger(payloadBytes) ||
  payloadBytes < 0 ||
  !Number.isSafeInteger(maxInFlight) ||
  maxInFlight <= 0
) {
  throw new Error("rate/duration must be positive; payload-bytes/max-in-flight valid integers");
}

class LogHistogram {
  constructor() {
    this.buckets = new BigUint64Array(512);
    this.count = 0;
    this.min = Number.POSITIVE_INFINITY;
    this.max = 0;
  }

  record(milliseconds) {
    const micros = Math.max(1, milliseconds * 1000);
    const index = Math.min(this.buckets.length - 1, Math.max(0, Math.floor(Math.log2(micros) * 16)));
    this.buckets[index] += 1n;
    this.count += 1;
    this.min = Math.min(this.min, milliseconds);
    this.max = Math.max(this.max, milliseconds);
  }

  quantile(q) {
    if (this.count === 0) return 0;
    const target = Math.max(1, Math.ceil(this.count * q));
    let seen = 0;
    for (let index = 0; index < this.buckets.length; index += 1) {
      seen += Number(this.buckets[index]);
      if (seen >= target) return 2 ** ((index + 1) / 16) / 1000;
    }
    return this.max;
  }

  summary() {
    return {
      count: this.count,
      minMs: this.count === 0 ? 0 : this.min,
      p50Ms: this.quantile(0.5),
      p95Ms: this.quantile(0.95),
      p99Ms: this.quantile(0.99),
      p999Ms: this.quantile(0.999),
      maxMs: this.max,
    };
  }
}

const sleep = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));
const kafka = new Kafka({
  clientId: "kapture-open-loop-perf",
  brokers: [broker],
  logLevel: logLevel.NOTHING,
  connectionTimeout: 10_000,
  requestTimeout: 60_000,
  retry: { retries: 0 },
});
const producer = kafka.producer({ allowAutoTopicCreation: true });
const value = Buffer.alloc(payloadBytes, 0x61);
const responseLatency = new LogHistogram();
const schedulingLag = new LogHistogram();
const inFlight = new Set();
const target = Math.floor(rate * durationSeconds);
const intervalMs = 1000 / rate;
let acknowledged = 0;
let failed = 0;
let overloadDrops = 0;
let maxObservedInFlight = 0;
let maxRss = process.memoryUsage().rss;

await producer.connect();
const cpuStart = process.cpuUsage();
const startedAt = performance.now();
const rssTimer = setInterval(() => {
  maxRss = Math.max(maxRss, process.memoryUsage().rss);
}, 100);

const launch = (sequence, intendedAt) => {
  if (inFlight.size >= maxInFlight) {
    overloadDrops += 1;
    return;
  }
  const launchedAt = performance.now();
  schedulingLag.record(launchedAt - intendedAt);
  let operation;
  operation = producer
    .send({
      topic,
      messages: [
        {
          key: String(sequence),
          value,
          headers: { "kapture-intended-ns": String(Math.round(intendedAt * 1_000_000)) },
        },
      ],
    })
    .then(() => {
      acknowledged += 1;
      responseLatency.record(performance.now() - intendedAt);
    })
    .catch(() => {
      failed += 1;
      responseLatency.record(performance.now() - intendedAt);
    })
    .finally(() => inFlight.delete(operation));
  inFlight.add(operation);
  maxObservedInFlight = Math.max(maxObservedInFlight, inFlight.size);
};

let sequence = 0;
while (sequence < target) {
  const now = performance.now();
  const intendedAt = startedAt + sequence * intervalMs;
  if (now < intendedAt) {
    await sleep(Math.min(10, intendedAt - now));
    continue;
  }
  // Catch up after scheduler stalls. The explicit max-in-flight bound
  // turns overload into a counted drop, never a hidden rate reduction.
  while (sequence < target && startedAt + sequence * intervalMs <= performance.now()) {
    launch(sequence, startedAt + sequence * intervalMs);
    sequence += 1;
  }
}

await Promise.allSettled([...inFlight]);
clearInterval(rssTimer);
await producer.disconnect();
const wallSeconds = (performance.now() - startedAt) / 1000;
const cpu = process.cpuUsage(cpuStart);

console.log(
  JSON.stringify(
    {
      config: { broker, topic, rate, durationSeconds, payloadBytes, maxInFlight },
      offered: target,
      acknowledged,
      failed,
      overloadDrops,
      achievedPerSecond: acknowledged / wallSeconds,
      wallSeconds,
      maxObservedInFlight,
      responseLatency: responseLatency.summary(),
      schedulingLag: schedulingLag.summary(),
      clientCpuSeconds: (cpu.user + cpu.system) / 1_000_000,
      clientMaxRssBytes: maxRss,
    },
    null,
    2,
  ),
);
