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
const warmupMessages = Number(arg("--warmup-messages", "10"));
const injectStallAtSeconds = Number(arg("--inject-stall-at", "-1"));
const injectStallMs = Number(arg("--inject-stall-ms", "0"));

if (
  !Number.isFinite(rate) ||
  rate <= 0 ||
  !Number.isFinite(durationSeconds) ||
  durationSeconds <= 0 ||
  !Number.isSafeInteger(payloadBytes) ||
  payloadBytes < 0 ||
  !Number.isSafeInteger(maxInFlight) ||
  maxInFlight <= 0 ||
  !Number.isSafeInteger(warmupMessages) ||
  warmupMessages <= 0 ||
  !Number.isFinite(injectStallAtSeconds) ||
  !Number.isFinite(injectStallMs) ||
  injectStallMs < 0
) {
  throw new Error(
    "rate/duration must be positive; payload-bytes/max-in-flight/warmup-messages valid integers",
  );
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
    const index = Math.min(
      this.buckets.length - 1,
      Math.max(0, Math.floor(Math.log2(micros) * 16)),
    );
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
const failureReasons = new Map();
const target = Math.floor(rate * durationSeconds);
const intervalMs = 1000 / rate;
let acknowledged = 0;
let failed = 0;
let overloadDrops = 0;
let maxObservedInFlight = 0;
let maxRss = process.memoryUsage().rss;
let stallInjected = false;

await producer.connect();

// Topic auto-creation and the first metadata refresh are control-plane work,
// not steady-state data-plane latency. Complete them before starting either
// the clock or CPU/RSS accounting. A few brokers briefly return
// UNKNOWN_TOPIC_OR_PARTITION while auto-creation converges, hence the bounded
// setup-only retry even though measured sends never retry.
let warmupError;
for (let attempt = 1; attempt <= 10; attempt += 1) {
  try {
    await producer.send({
      topic,
      messages: Array.from({ length: warmupMessages }, (_, sequence) => ({
        key: `warmup-${sequence}`,
        value,
      })),
    });
    warmupError = undefined;
    break;
  } catch (error) {
    warmupError = error;
    if (attempt < 10) await sleep(250);
  }
}
if (warmupError) {
  await producer.disconnect();
  throw new Error(`Kafka warm-up failed after 10 attempts: ${warmupError.message}`, {
    cause: warmupError,
  });
}

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
    .catch((error) => {
      failed += 1;
      const reason = error?.name ?? error?.constructor?.name ?? "UnknownError";
      failureReasons.set(reason, (failureReasons.get(reason) ?? 0) + 1);
      responseLatency.record(performance.now() - intendedAt);
    })
    .finally(() => inFlight.delete(operation));
  inFlight.add(operation);
  maxObservedInFlight = Math.max(maxObservedInFlight, inFlight.size);
};

let sequence = 0;
while (sequence < target) {
  const now = performance.now();
  if (
    !stallInjected &&
    injectStallAtSeconds >= 0 &&
    injectStallMs > 0 &&
    now - startedAt >= injectStallAtSeconds * 1000
  ) {
    stallInjected = true;
    // Deliberately stop the scheduler thread. The catch-up loop below
    // still offers every intended arrival afterwards, so the pause
    // appears in scheduling/response tails rather than as a lower rate.
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, injectStallMs);
  }
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
      config: {
        broker,
        topic,
        rate,
        durationSeconds,
        payloadBytes,
        maxInFlight,
        warmupMessages,
        injectStallAtSeconds,
        injectStallMs,
      },
      offered: target,
      acknowledged,
      failed,
      failureReasons: Object.fromEntries(
        [...failureReasons.entries()].sort(([left], [right]) => left.localeCompare(right)),
      ),
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

// A benchmark with hidden loss is not a successful benchmark. Keep the JSON
// available to CI and humans, but surface the invalid run through the status.
if (failed > 0 || overloadDrops > 0) process.exitCode = 1;
