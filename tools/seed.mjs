#!/usr/bin/env node
/**
 * Kapture dev seed script.
 *
 * Produces a mix of payload encodings so the Inspector can be exercised
 * against realistic schema-registry traffic:
 *
 *   - orders.raw         JSON (no Schema Registry)
 *   - orders.enriched    JSON (no Schema Registry)
 *   - users.events       JSON (no Schema Registry)
 *   - orders.avro        Avro via Confluent Schema Registry
 *   - orders.jsonschema  JSON Schema via Confluent Schema Registry
 *
 * Usage:
 *   node tools/seed.mjs                       # 200 messages, then exit
 *   node tools/seed.mjs --loop                # infinite loop, 5 msg/s/topic
 *   node tools/seed.mjs --loop --rate 50      # infinite loop, 50 msg/s/topic
 *   node tools/seed.mjs --count 1000          # 1000 messages, then exit
 *
 * Targets the docker-compose stack: Kafka API on localhost:19092,
 * Schema Registry on http://localhost:18081.
 */

import { Kafka, logLevel } from "kafkajs";
import { SchemaRegistry, SchemaType } from "@kafkajs/confluent-schema-registry";

const args = process.argv.slice(2);
const has = (flag) => args.includes(flag);
const arg = (flag, fallback) => {
  const i = args.indexOf(flag);
  return i >= 0 && args[i + 1] !== undefined ? args[i + 1] : fallback;
};

const BROKER = arg("--broker", "localhost:19092");
const REGISTRY = arg("--registry", "http://localhost:18081");
const LOOP = has("--loop");
const RATE = Number(arg("--rate", "5"));
const COUNT = Number(arg("--count", "200"));

const TOPICS_RAW = ["orders.raw", "orders.enriched", "users.events"];
const TOPIC_AVRO = "orders.avro";
const TOPIC_JSONSCHEMA = "orders.jsonschema";
const ALL_TOPICS = [...TOPICS_RAW, TOPIC_AVRO, TOPIC_JSONSCHEMA];

const TENANTS = ["acme", "globex", "initech", "umbrella"];
const CURRENCIES = ["EUR", "USD", "GBP", "JPY"];
const TIERS = ["bronze", "silver", "gold", "platinum"];
const EVENTS = ["session.started", "page.viewed", "search.performed", "cart.updated"];

const ORDER_AVRO_SCHEMA = {
  type: "record",
  namespace: "io.kapture.orders",
  name: "Order",
  fields: [
    { name: "orderId", type: "string" },
    { name: "userId", type: "string" },
    { name: "amount", type: "int" },
    { name: "currency", type: "string" },
    { name: "userTier", type: "string" },
    { name: "riskScore", type: "double" },
    { name: "items", type: { type: "array", items: "string" } },
  ],
};

const ORDER_JSONSCHEMA = {
  $schema: "http://json-schema.org/draft-07/schema#",
  $id: "io.kapture.orders.OrderJson",
  type: "object",
  required: ["orderId", "userId", "amount", "currency"],
  properties: {
    orderId: { type: "string" },
    userId: { type: "string" },
    amount: { type: "integer", minimum: 0 },
    currency: { type: "string" },
    userTier: { type: "string" },
    riskScore: { type: "number" },
    items: { type: "array", items: { type: "string" } },
  },
};

const kafka = new Kafka({
  clientId: "kapture-seed",
  brokers: [BROKER],
  logLevel: logLevel.NOTHING,
  connectionTimeout: 5_000,
  retry: { retries: 5, initialRetryTime: 200 },
});

const registry = new SchemaRegistry({ host: REGISTRY });
const producer = kafka.producer({
  allowAutoTopicCreation: true,
  transactionTimeout: 30_000,
});
const admin = kafka.admin();

const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];
const userId = () =>
  `u-${Math.floor(Math.random() * 200)
    .toString()
    .padStart(3, "0")}`;
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

function buildAvroPayload() {
  const id = userId();
  return {
    orderId: `ord-${traceId()}`,
    userId: id,
    amount: Math.floor(Math.random() * 5000) + 50,
    currency: pick(CURRENCIES),
    userTier: pick(TIERS),
    riskScore: Number((Math.random() * 0.5).toFixed(3)),
    items: Array.from({ length: 1 + Math.floor(Math.random() * 4) }, () => `sku-${traceId()}`),
  };
}

function buildJsonSchemaPayload() {
  const id = userId();
  return {
    orderId: `ord-${traceId()}`,
    userId: id,
    amount: Math.floor(Math.random() * 5000) + 50,
    currency: pick(CURRENCIES),
    userTier: pick(TIERS),
    riskScore: Number((Math.random() * 0.5).toFixed(3)),
    items: Array.from({ length: 1 + Math.floor(Math.random() * 4) }, () => `sku-${traceId()}`),
  };
}

const builders = [buildOrderRaw, buildOrderEnriched, buildUserEvent];

async function ensureTopics() {
  await admin.connect();
  const existing = new Set(await admin.listTopics());
  const missing = ALL_TOPICS.filter((t) => !existing.has(t));
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

async function registerSchemas() {
  const avro = await registry.register(
    {
      type: SchemaType.AVRO,
      schema: JSON.stringify(ORDER_AVRO_SCHEMA),
    },
    { subject: `${TOPIC_AVRO}-value` },
  );
  const jsonSchema = await registry.register(
    {
      type: SchemaType.JSON,
      schema: JSON.stringify(ORDER_JSONSCHEMA),
    },
    { subject: `${TOPIC_JSONSCHEMA}-value` },
  );
  console.log(`registered Avro schema id=${avro.id}`);
  console.log(`registered JSON Schema id=${jsonSchema.id}`);
  return { avroId: avro.id, jsonSchemaId: jsonSchema.id };
}

async function produceBatch(n, schemaIds) {
  const grouped = new Map();
  const append = (topic, msg) => {
    const list = grouped.get(topic) ?? [];
    list.push(msg);
    grouped.set(topic, list);
  };

  for (let i = 0; i < n; i++) {
    const roll = Math.random();
    if (roll < 0.6) {
      // 60% raw JSON across the three legacy topics
      const built = pick(builders)();
      append(built.topic, { key: built.key, value: built.value, headers: built.headers });
    } else if (roll < 0.85) {
      // 25% Avro
      const id = userId();
      const encoded = await registry.encode(schemaIds.avroId, buildAvroPayload());
      append(TOPIC_AVRO, {
        key: id,
        value: encoded,
        headers: { tenant: pick(TENANTS), traceid: traceId() },
      });
    } else {
      // 15% JSON Schema
      const id = userId();
      const encoded = await registry.encode(schemaIds.jsonSchemaId, buildJsonSchemaPayload());
      append(TOPIC_JSONSCHEMA, {
        key: id,
        value: encoded,
        headers: { tenant: pick(TENANTS), traceid: traceId() },
      });
    }
  }
  for (const [topic, messages] of grouped) {
    await producer.send({ topic, messages });
  }
  return n;
}

async function main() {
  console.log(`broker:   ${BROKER}`);
  console.log(`registry: ${REGISTRY}`);
  await ensureTopics();
  const schemaIds = await registerSchemas();
  await producer.connect();
  console.log("producer connected");

  if (LOOP) {
    console.log(`looping at ~${RATE} msg/s. Ctrl-C to stop.`);
    let total = 0;
    const tickMs = 1000;
    const perTick = Math.max(1, Math.floor(RATE * (tickMs / 1000)));
    while (true) {
      const sent = await produceBatch(perTick, schemaIds);
      total += sent;
      if (total % 50 < perTick) {
        console.log(`sent ${total}`);
      }
      await new Promise((r) => setTimeout(r, tickMs));
    }
  } else {
    const sent = await produceBatch(COUNT, schemaIds);
    console.log(`sent ${sent} messages, exiting`);
    await producer.disconnect();
  }
}

main().catch((err) => {
  console.error("seed failed:", err);
  process.exitCode = 1;
});
