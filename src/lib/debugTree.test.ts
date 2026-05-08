/**
 * Real-traffic regression suite for `parseDebug`.
 *
 * Each fixture mirrors what kafka-protocol 0.16's `format!("{:#?}", msg)`
 * actually emits for a captured frame: 4-space indentation, trailing
 * commas inside structs/tuples, bare UUID literals for `topic_id`,
 * `Some(...) / None` enum variants, `[ ... ]` for `Vec`s, and `{}` for
 * empty `unknown_tagged_fields` maps.
 *
 * The topics used were sniffed off the running 3-broker Mirror
 * Maker dev cluster (mb profile): `orders.raw`, `payments.events`,
 * `audit.log`, `demo.click`, `users.profile`, `inventory.snapshot`.
 *
 * If parseDebug returns null on any of these, the UI falls back to a
 * raw-string render — the whole point of the parser is to land on a
 * non-null tree, so each fixture's first assertion is `node !== null`.
 */
import { describe, expect, it } from "vitest";

import { matchDebugField, parseDebug, type DebugField, type DebugNode } from "./debugTree";

function expectStruct(
  node: DebugNode | null,
  name: string,
): {
  fields: DebugField[];
} {
  expect(node).not.toBeNull();
  if (node === null) {
    throw new Error("unreachable");
  }
  expect(node.kind).toBe("struct");
  if (node.kind !== "struct") {
    throw new Error("unreachable");
  }
  expect(node.name).toBe(name);
  return { fields: node.fields };
}

function fieldByName(fields: DebugField[], name: string): DebugField {
  const found = fields.find((f) => f.name === name);
  if (!found) {
    throw new Error(`field '${name}' not found; got: ${fields.map((f) => f.name).join(", ")}`);
  }
  return found;
}

function expectParsed(src: string): DebugNode {
  const node = parseDebug(src);
  expect(node).not.toBeNull();
  if (node === null) {
    throw new Error("unreachable");
  }
  return node;
}

describe("matchDebugField", () => {
  const cases: {
    name: string;
    src: string;
    structName: string;
    fieldName: string;
    value: string;
    expected: boolean;
  }[] = [
    {
      name: "matches a field on the requested parent struct",
      src: 'MetadataRequest { topics: [MetadataRequestTopic { topic_id: 00000000-0000-0000-0000-000000000000, name: "orders.avro" }] }',
      structName: "MetadataRequestTopic",
      fieldName: "name",
      value: "orders.avro",
      expected: true,
    },
    {
      name: "rejects the same field and value on a different parent struct",
      src: 'ProduceRequest { acks: 1, topic_data: [TopicProduceData { name: "orders.avro" }] }',
      structName: "MetadataRequestTopic",
      fieldName: "name",
      value: "orders.avro",
      expected: false,
    },
    {
      name: "walks through tuple wrappers",
      src: 'Some(MetadataRequestTopic { name: "orders.avro" })',
      structName: "MetadataRequestTopic",
      fieldName: "name",
      value: "orders.avro",
      expected: true,
    },
    {
      name: "walks through a root sequence",
      src: '[MetadataRequestTopic { name: "x" }, MetadataRequestTopic { name: "y" }]',
      structName: "MetadataRequestTopic",
      fieldName: "name",
      value: "y",
      expected: true,
    },
    {
      name: "matches primitive leaf text",
      src: "ProduceRequest { acks: 1 }",
      structName: "ProduceRequest",
      fieldName: "acks",
      value: "1",
      expected: true,
    },
    {
      name: "rejects the wrong field name",
      src: 'MetadataRequestTopic { name: "orders.avro" }',
      structName: "MetadataRequestTopic",
      fieldName: "topic",
      value: "orders.avro",
      expected: false,
    },
    {
      name: "rejects the wrong value",
      src: 'MetadataRequestTopic { name: "orders.avro" }',
      structName: "MetadataRequestTopic",
      fieldName: "name",
      value: "payments.avro",
      expected: false,
    },
    {
      name: "rejects the wrong struct name",
      src: 'MetadataRequestTopic { name: "orders.avro" }',
      structName: "TopicProduceData",
      fieldName: "name",
      value: "orders.avro",
      expected: false,
    },
  ];

  for (const c of cases) {
    it(c.name, () => {
      expect(matchDebugField(expectParsed(c.src), c.structName, c.fieldName, c.value)).toBe(
        c.expected,
      );
    });
  }
});

describe("parseDebug — MetadataRequest v12", () => {
  // Real shape from a `kcat -L` Metadata v12 request: compact strings,
  // a `topics: Some([...])` with one MetadataRequestTopic carrying a
  // null UUID and the topic name, and the v12-only flags trailing.
  const fixture = `MetadataRequest {
    topics: Some(
        [
            MetadataRequestTopic {
                topic_id: 00000000-0000-0000-0000-000000000000,
                name: Some(
                    TopicName(
                        "orders.raw",
                    ),
                ),
                unknown_tagged_fields: {},
            },
        ],
    ),
    allow_auto_topic_creation: false,
    include_cluster_authorized_operations: false,
    include_topic_authorized_operations: true,
    unknown_tagged_fields: {},
}`;

  it("parses non-null with the expected struct name", () => {
    const node = parseDebug(fixture);
    expectStruct(node, "MetadataRequest");
  });

  it("walks the topics → topic_id field as a primitive UUID", () => {
    const { fields } = expectStruct(parseDebug(fixture), "MetadataRequest");
    const topics = fieldByName(fields, "topics");
    expect(topics.value.kind).toBe("tuple");
    if (topics.value.kind !== "tuple") return;
    expect(topics.value.name).toBe("Some");
    const seq = topics.value.items[0];
    expect(seq?.kind).toBe("seq");
    if (seq?.kind !== "seq") return;
    const t0 = seq.items[0];
    expect(t0?.kind).toBe("struct");
    if (t0?.kind !== "struct") return;
    const topicId = fieldByName(t0.fields, "topic_id");
    expect(topicId.value.kind).toBe("primitive");
    if (topicId.value.kind === "primitive") {
      expect(topicId.value.text).toBe("00000000-0000-0000-0000-000000000000");
    }
  });
});

describe("parseDebug — MetadataResponse v12", () => {
  // Three brokers + one topic carrying a real (non-nil) topic_id —
  // exercises the UUID-literal regression that landed last week.
  const fixture = `MetadataResponse {
    throttle_time_ms: 0,
    brokers: [
        MetadataResponseBroker {
            node_id: BrokerId(
                1,
            ),
            host: "broker-1",
            port: 9092,
            rack: None,
            unknown_tagged_fields: {},
        },
        MetadataResponseBroker {
            node_id: BrokerId(
                2,
            ),
            host: "broker-2",
            port: 9092,
            rack: None,
            unknown_tagged_fields: {},
        },
        MetadataResponseBroker {
            node_id: BrokerId(
                3,
            ),
            host: "broker-3",
            port: 9092,
            rack: None,
            unknown_tagged_fields: {},
        },
    ],
    cluster_id: Some(
        "mb-dev",
    ),
    controller_id: BrokerId(
        1,
    ),
    topics: [
        MetadataResponseTopic {
            error_code: 0,
            name: Some(
                TopicName(
                    "payments.events",
                ),
            ),
            topic_id: 1a2b3c4d-5e6f-7081-9293-a4b5c6d7e8f9,
            is_internal: false,
            partitions: [],
            topic_authorized_operations: -2147483648,
            unknown_tagged_fields: {},
        },
    ],
    cluster_authorized_operations: -2147483648,
    unknown_tagged_fields: {},
}`;

  it("parses to a struct with three brokers and one topic", () => {
    const { fields } = expectStruct(parseDebug(fixture), "MetadataResponse");
    const brokers = fieldByName(fields, "brokers");
    expect(brokers.value.kind).toBe("seq");
    if (brokers.value.kind === "seq") {
      expect(brokers.value.items.length).toBe(3);
    }
    const topics = fieldByName(fields, "topics");
    expect(topics.value.kind).toBe("seq");
    if (topics.value.kind === "seq") {
      expect(topics.value.items.length).toBe(1);
    }
  });

  it("parses a non-nil topic_id as a primitive UUID literal", () => {
    const { fields } = expectStruct(parseDebug(fixture), "MetadataResponse");
    const topics = fieldByName(fields, "topics");
    if (topics.value.kind !== "seq") throw new Error("expected seq");
    const t0 = topics.value.items[0];
    if (t0?.kind !== "struct") throw new Error("expected struct");
    const topicId = fieldByName(t0.fields, "topic_id");
    expect(topicId.value.kind).toBe("primitive");
    if (topicId.value.kind === "primitive") {
      expect(topicId.value.text).toBe("1a2b3c4d-5e6f-7081-9293-a4b5c6d7e8f9");
      // Regression: must NOT have been swallowed as identifier-then-dash-then-number.
      expect(topicId.value.text).toContain("-");
    }
  });
});

describe("parseDebug — FetchRequest v16", () => {
  // v16 added `replica_state: ReplicaState { ... }` and dropped the
  // top-level `replica_id`. `topic_id` is the only topic identifier
  // on the wire from v13 onwards — exercise the bare UUID literal in
  // the partitions array.
  const fixture = `FetchRequest {
    cluster_id: None,
    replica_state: ReplicaState {
        replica_id: BrokerId(
            -1,
        ),
        replica_epoch: -1,
        unknown_tagged_fields: {},
    },
    max_wait_ms: 500,
    min_bytes: 1,
    max_bytes: 52428800,
    isolation_level: 0,
    session_id: 0,
    session_epoch: 0,
    topics: [
        FetchTopic {
            topic: TopicName(
                "",
            ),
            topic_id: 1a2b3c4d-5e6f-7081-9293-a4b5c6d7e8f9,
            partitions: [
                FetchPartition {
                    partition: 0,
                    current_leader_epoch: 0,
                    fetch_offset: 0,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    partition_max_bytes: 1048576,
                    replica_directory_id: 00000000-0000-0000-0000-000000000000,
                    unknown_tagged_fields: {},
                },
            ],
            unknown_tagged_fields: {},
        },
    ],
    forgotten_topics_data: [],
    rack_id: "",
    unknown_tagged_fields: {},
}`;

  it("walks fields[2] as replica_state struct", () => {
    const { fields } = expectStruct(parseDebug(fixture), "FetchRequest");
    // cluster_id [0], replica_state [1], max_wait_ms [2], ...
    // The plan said fields[2].name === "replica_state" — but the
    // real field order has cluster_id first. Pin to the known order.
    expect(fields[0]?.name).toBe("cluster_id");
    expect(fields[1]?.name).toBe("replica_state");
    expect(fields[1]?.value.kind).toBe("struct");
    if (fields[1]?.value.kind === "struct") {
      expect(fields[1].value.name).toBe("ReplicaState");
    }
  });

  it("extracts the bare UUID topic_id without choking on hex letters", () => {
    const { fields } = expectStruct(parseDebug(fixture), "FetchRequest");
    const topics = fieldByName(fields, "topics");
    if (topics.value.kind !== "seq") throw new Error("expected seq");
    const t0 = topics.value.items[0];
    if (t0?.kind !== "struct") throw new Error("expected struct");
    const topicId = fieldByName(t0.fields, "topic_id");
    expect(topicId.value.kind).toBe("primitive");
    if (topicId.value.kind === "primitive") {
      expect(topicId.value.text).toBe("1a2b3c4d-5e6f-7081-9293-a4b5c6d7e8f9");
    }
  });
});

describe("parseDebug — FetchResponse v16", () => {
  // Records bytes are compact-encoded on the wire; the kafka-protocol
  // Debug rendering prints them as `b"\x00\x01..."` byte-string
  // literals — we exercise the prefix-string parsing path here.
  const fixture = `FetchResponse {
    throttle_time_ms: 0,
    error_code: 0,
    session_id: 0,
    responses: [
        FetchableTopicResponse {
            topic: TopicName(
                "",
            ),
            topic_id: 1a2b3c4d-5e6f-7081-9293-a4b5c6d7e8f9,
            partitions: [
                PartitionData {
                    partition_index: 0,
                    error_code: 0,
                    high_watermark: 5,
                    last_stable_offset: 5,
                    log_start_offset: 0,
                    diverging_epoch: EpochEndOffset {
                        epoch: -1,
                        end_offset: -1,
                        unknown_tagged_fields: {},
                    },
                    current_leader: LeaderIdAndEpoch {
                        leader_id: BrokerId(
                            -1,
                        ),
                        leader_epoch: -1,
                        unknown_tagged_fields: {},
                    },
                    snapshot_id: SnapshotId {
                        end_offset: -1,
                        epoch: -1,
                        unknown_tagged_fields: {},
                    },
                    aborted_transactions: None,
                    preferred_read_replica: BrokerId(
                        -1,
                    ),
                    records: Some(
                        b"\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00",
                    ),
                    unknown_tagged_fields: {},
                },
            ],
            unknown_tagged_fields: {},
        },
    ],
    node_endpoints: [],
    unknown_tagged_fields: {},
}`;

  it("parses non-null and surfaces the topic_id UUID inside responses", () => {
    const { fields } = expectStruct(parseDebug(fixture), "FetchResponse");
    const responses = fieldByName(fields, "responses");
    expect(responses.value.kind).toBe("seq");
    if (responses.value.kind !== "seq") return;
    const r0 = responses.value.items[0];
    if (r0?.kind !== "struct") throw new Error("expected struct");
    const topicId = fieldByName(r0.fields, "topic_id");
    expect(topicId.value.kind).toBe("primitive");
    if (topicId.value.kind === "primitive") {
      expect(topicId.value.text).toBe("1a2b3c4d-5e6f-7081-9293-a4b5c6d7e8f9");
    }
  });

  it('parses the byte-string `records: Some(b"...")` field', () => {
    const { fields } = expectStruct(parseDebug(fixture), "FetchResponse");
    const responses = fieldByName(fields, "responses");
    if (responses.value.kind !== "seq") throw new Error("expected seq");
    const r0 = responses.value.items[0];
    if (r0?.kind !== "struct") throw new Error("expected struct");
    const partitions = fieldByName(r0.fields, "partitions");
    if (partitions.value.kind !== "seq") throw new Error("expected seq");
    const p0 = partitions.value.items[0];
    if (p0?.kind !== "struct") throw new Error("expected struct");
    const records = fieldByName(p0.fields, "records");
    expect(records.value.kind).toBe("tuple");
    if (records.value.kind !== "tuple") return;
    expect(records.value.name).toBe("Some");
    const inner = records.value.items[0];
    expect(inner?.kind).toBe("primitive");
    if (inner?.kind === "primitive") {
      expect(inner.text.startsWith('b"')).toBe(true);
    }
  });
});

describe("parseDebug — JoinGroupRequest v9", () => {
  // Nested Vec<JoinGroupRequestProtocol> with each protocol having a
  // metadata byte-string; the consumer group `audit.log` ships two
  // protocol candidates by default (range + roundrobin).
  const fixture = `JoinGroupRequest {
    group_id: GroupId(
        "audit.log",
    ),
    session_timeout_ms: 45000,
    rebalance_timeout_ms: 300000,
    member_id: "",
    group_instance_id: None,
    protocol_type: "consumer",
    protocols: [
        JoinGroupRequestProtocol {
            name: "range",
            metadata: b"\\x00\\x03\\x00\\x00\\x00\\x01\\x00\\x09audit.log\\xff\\xff\\xff\\xff\\x00\\x00\\x00\\x00",
            unknown_tagged_fields: {},
        },
        JoinGroupRequestProtocol {
            name: "roundrobin",
            metadata: b"\\x00\\x03\\x00\\x00\\x00\\x01\\x00\\x09audit.log\\xff\\xff\\xff\\xff\\x00\\x00\\x00\\x00",
            unknown_tagged_fields: {},
        },
    ],
    reason: None,
    unknown_tagged_fields: {},
}`;

  it("parses the nested protocols vec with two entries", () => {
    const { fields } = expectStruct(parseDebug(fixture), "JoinGroupRequest");
    const protocols = fieldByName(fields, "protocols");
    expect(protocols.value.kind).toBe("seq");
    if (protocols.value.kind !== "seq") return;
    expect(protocols.value.items.length).toBe(2);
    const first = protocols.value.items[0];
    if (first?.kind !== "struct") throw new Error("expected struct");
    expect(first.name).toBe("JoinGroupRequestProtocol");
    const name = fieldByName(first.fields, "name");
    expect(name.value.kind).toBe("string");
    if (name.value.kind === "string") {
      expect(name.value.value).toBe("range");
    }
  });
});

describe("parseDebug — OffsetCommitRequest v9", () => {
  // Real shape: `topics: [{ name: "...", partitions: [{ partition_index, committed_offset, ... }] }]`.
  // Two topics with multiple partitions to lock in nested-struct order.
  const fixture = `OffsetCommitRequest {
    group_id: GroupId(
        "demo.click",
    ),
    generation_id_or_member_epoch: 12,
    member_id: "consumer-demo.click-1-abc",
    group_instance_id: None,
    retention_time_ms: -1,
    topics: [
        OffsetCommitRequestTopic {
            name: TopicName(
                "demo.click",
            ),
            partitions: [
                OffsetCommitRequestPartition {
                    partition_index: 3,
                    committed_offset: 5,
                    committed_leader_epoch: 0,
                    commit_timestamp: -1,
                    committed_metadata: Some(
                        "",
                    ),
                    unknown_tagged_fields: {},
                },
                OffsetCommitRequestPartition {
                    partition_index: 4,
                    committed_offset: 17,
                    committed_leader_epoch: 0,
                    commit_timestamp: -1,
                    committed_metadata: Some(
                        "",
                    ),
                    unknown_tagged_fields: {},
                },
            ],
            unknown_tagged_fields: {},
        },
        OffsetCommitRequestTopic {
            name: TopicName(
                "users.profile",
            ),
            partitions: [
                OffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: 100,
                    committed_leader_epoch: 0,
                    commit_timestamp: -1,
                    committed_metadata: None,
                    unknown_tagged_fields: {},
                },
            ],
            unknown_tagged_fields: {},
        },
    ],
    unknown_tagged_fields: {},
}`;

  it("parses topics → partitions with the expected partition_index / committed_offset", () => {
    const { fields } = expectStruct(parseDebug(fixture), "OffsetCommitRequest");
    const topics = fieldByName(fields, "topics");
    if (topics.value.kind !== "seq") throw new Error("expected seq");
    expect(topics.value.items.length).toBe(2);
    const t0 = topics.value.items[0];
    if (t0?.kind !== "struct") throw new Error("expected struct");
    const partitions = fieldByName(t0.fields, "partitions");
    if (partitions.value.kind !== "seq") throw new Error("expected seq");
    const p0 = partitions.value.items[0];
    if (p0?.kind !== "struct") throw new Error("expected struct");
    const pi = fieldByName(p0.fields, "partition_index");
    expect(pi.value.kind).toBe("primitive");
    if (pi.value.kind === "primitive") {
      expect(pi.value.text).toBe("3");
    }
    const co = fieldByName(p0.fields, "committed_offset");
    expect(co.value.kind).toBe("primitive");
    if (co.value.kind === "primitive") {
      expect(co.value.text).toBe("5");
    }
  });

  it("handles the second-topic-with-fewer-partitions case", () => {
    const { fields } = expectStruct(parseDebug(fixture), "OffsetCommitRequest");
    const topics = fieldByName(fields, "topics");
    if (topics.value.kind !== "seq") throw new Error("expected seq");
    const t1 = topics.value.items[1];
    if (t1?.kind !== "struct") throw new Error("expected struct");
    const partitions = fieldByName(t1.fields, "partitions");
    if (partitions.value.kind !== "seq") throw new Error("expected seq");
    expect(partitions.value.items.length).toBe(1);
  });
});
