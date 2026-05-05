import type { KafkaMessage } from "./types";

export const MOCK_MESSAGES: KafkaMessage[] = [
  {
    id: "m-1",
    timestamp: "12:01:03.412",
    topic: "orders.raw",
    partition: 3,
    offset: 42031,
    key: "u-42",
    schemaName: "OrderCreated",
    schemaId: 17,
    sizeBytes: 312,
    headers: [
      { key: "tenant", value: "acme" },
      { key: "traceid", value: "9f1ad8" },
    ],
    payload: {
      kind: "object",
      fields: [
        { name: "amount", value: { kind: "primitive", type: "number", value: "1450" } },
        { name: "currency", value: { kind: "primitive", type: "string", value: "EUR" } },
        { name: "userId", value: { kind: "primitive", type: "string", value: "u-42" } },
      ],
    },
    rawHex: "00 00 00 00 11 0e 6f 72 64 65 72 73 2e 72 61 77 03 a8 0b 45 55 52 75 2d 34 32",
  },
  {
    id: "m-2",
    timestamp: "12:01:04.087",
    topic: "orders.enriched",
    partition: 3,
    offset: 18914,
    key: "u-42",
    schemaName: "OrderEnriched",
    schemaId: 23,
    sizeBytes: 488,
    headers: [
      { key: "tenant", value: "acme" },
      { key: "traceid", value: "9f1ad8" },
      { key: "source", value: "enricher-v2" },
    ],
    payload: {
      kind: "object",
      fields: [
        { name: "amount", value: { kind: "primitive", type: "number", value: "1450" } },
        { name: "currency", value: { kind: "primitive", type: "string", value: "EUR" } },
        { name: "userTier", value: { kind: "primitive", type: "string", value: "gold" } },
        { name: "riskScore", value: { kind: "primitive", type: "number", value: "0.12" } },
      ],
    },
    rawHex: "00 00 00 00 17 0e 67 6f 6c 64 0c 30 2e 31 32 03 a8 0b 45 55 52",
  },
  {
    id: "m-3",
    timestamp: "12:01:04.612",
    topic: "users.events",
    partition: 7,
    offset: 992014,
    key: "u-99",
    schemaName: null,
    schemaId: null,
    sizeBytes: 102,
    headers: [],
    payload: {
      kind: "object",
      fields: [
        { name: "event", value: { kind: "primitive", type: "string", value: "session.started" } },
        { name: "ip", value: { kind: "primitive", type: "string", value: "10.0.0.42" } },
      ],
    },
    rawHex: "7b 22 65 76 65 6e 74 22 3a 22 73 65 73 73 69 6f 6e 2e 73 74 61 72 74 65 64 22",
  },
];
