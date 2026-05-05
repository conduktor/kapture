#!/usr/bin/env node
/**
 * Kapture dev seed script.
 *
 * Usage:
 *   node tools/seed.mjs                       # 100 messages, then exit
 *   node tools/seed.mjs --loop                # infinite loop, 5 msg/s per topic
 *   node tools/seed.mjs --loop --rate 50      # infinite loop, 50 msg/s per topic
 *   node tools/seed.mjs --count 1000          # 1000 messages, then exit
 *
 * Targets the local Redpanda from docker-compose.yml on localhost:19092.
 */

import { Kafka, logLevel } from "kafkajs";

const args = process.argv.slice(2);
const has = (flag) => args.includes(flag);
const arg = (flag, fallback) => {
  const i = args.indexOf(flag);
  return i >= 0 && args[i + 1] !== undefined ? args[i + 1] : fallback;
};

const BROKER = arg("--broker", "localhost:19092");
const LOOP = has("--loop");
const RATE = Number(arg("--rate", "5"));
const COUNT = Number(arg("--count", "100"));

const TOPICS = ["orders.raw", "orders.enriched", "users.events"];
const TENANTS = ["acme", "globex", "initech", "umbrella"];
const CURRENCIES = ["EUR", "USD", "GBP", "JPY"];
const TIERS = ["bronze", "silver", "gold", "platinum"];
const EVENTS = ["session.started", "page.viewed", "search.performed", "cart.updated"];

const kafka = new Kafka({
  clientId: "kapture-seed",
  brokers: [BROKER],
  logLevel: logLevel.NOTHING,
  connectionTimeout: 5_000,
  retry: { retries: 5, initialRetryTime: 200 },
});

const producer = kafka.producer({
  allowAutoTopicCreation: true,
  transactionTimeout: 30_000,
});
const admin = kafka.admin();

const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];
const userId = () => `u-${Math.floor(Math.random() * 200).toString().padStart(3, "0")}`;
const traceId = () => Math.random().toString(16).slice(2, 10);

function buildOrderRaw() {
  const id = userId();
  return {
    topic: "orders.raw",
    key: id,
    value: JSON.stringify({
      orderId: `ord-${traceId()}`,
      userId: id,
      amount: Math.floor(Math.random() * 5000) + 50,
      currency: pick(CURRENCIES),
      items: Math.floor(Math.random() * 5) + 1,
      createdAt: new Date().toISOString(),
    }),
    headers: {
      tenant: pick(TENANTS),
      traceid: traceId(),
      "content-type": "application/json",
    },
  };
}

function buildOrderEnriched() {
  const id = userId();
  return {
    topic: "orders.enriched",
    key: id,
    value: JSON.stringify({
      orderId: `ord-${traceId()}`,
      userId: id,
      amount: Math.floor(Math.random() * 5000) + 50,
      currency: pick(CURRENCIES),
      userTier: pick(TIERS),
      riskScore: Number((Math.random() * 0.5).toFixed(3)),
      enrichedAt: new Date().toISOString(),
    }),
    headers: {
      tenant: pick(TENANTS),
      traceid: traceId(),
      source: "enricher-v2",
    },
  };
}

function buildUserEvent() {
  const id = userId();
  return {
    topic: "users.events",
    key: id,
    value: JSON.stringify({
      userId: id,
      event: pick(EVENTS),
      ip: `10.0.${Math.floor(Math.random() * 256)}.${Math.floor(Math.random() * 256)}`,
      ts: Date.now(),
    }),
    headers: {},
  };
}

const builders = [buildOrderRaw, buildOrderEnriched, buildUserEvent];

async function ensureTopics() {
  await admin.connect();
  const existing = new Set(await admin.listTopics());
  const missing = TOPICS.filter((t) => !existing.has(t));
  if (missing.length > 0) {
    console.log(`creating topics: ${missing.join(", ")}`);
    await admin.createTopics({
      topics: missing.map((topic) => ({ topic, numPartitions: 8, replicationFactor: 1 })),
    });
  } else {
    console.log("topics already present");
  }
  await admin.disconnect();
}

async function produceBatch(n) {
  const grouped = new Map();
  for (let i = 0; i < n; i++) {
    const built = pick(builders)();
    const list = grouped.get(built.topic) ?? [];
    list.push({ key: built.key, value: built.value, headers: built.headers });
    grouped.set(built.topic, list);
  }
  for (const [topic, messages] of grouped) {
    await producer.send({ topic, messages });
  }
  return n;
}

async function main() {
  console.log(`broker: ${BROKER}`);
  await ensureTopics();
  await producer.connect();
  console.log("producer connected");

  if (LOOP) {
    console.log(`looping at ~${RATE} msg/s per topic. Ctrl-C to stop.`);
    let total = 0;
    const tickMs = 1000;
    const perTick = Math.max(1, Math.floor((RATE * 3) / (1000 / tickMs)));
    while (true) {
      const sent = await produceBatch(perTick);
      total += sent;
      if (total % 50 < perTick) {
        console.log(`sent ${total}`);
      }
      await new Promise((r) => setTimeout(r, tickMs));
    }
  } else {
    const sent = await produceBatch(COUNT);
    console.log(`sent ${sent} messages, exiting`);
    await producer.disconnect();
  }
}

main().catch((err) => {
  console.error("seed failed:", err);
  process.exitCode = 1;
});
