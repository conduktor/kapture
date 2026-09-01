# Real-client anti-pattern harness

This is stage and development tooling, not a code path inside Kapture. It runs the
real Apache Kafka Java client in deliberately bad or fixed configurations so the
wire difference is genuine.

The scenarios target Kapture's proxy at `127.0.0.1:9092`. Point that proxy at the
local Apache Kafka broker on `localhost:29092`, then build the harness once:

```sh
pnpm stack:up:kafka
pnpm demo:client:build
pnpm demo:client:setup
```

`setup` connects directly to the local Kafka listener and creates the two
one-partition demo topics. Run it before starting or clearing the Kapture
capture, so topic administration does not add noise to the protocol story.

## Producer lifecycle

The bad client constructs, connects, sends through, and closes one
`KafkaProducer` per application record. Twelve records therefore cause twelve
full client lifecycles:

```sh
pnpm demo:client -- producer-lifecycle bad
```

Expected finding: **Producer-instance leak**. In Protocol, filter on the demo
client and watch `ApiVersions`, `Metadata`, `InitProducerId`, and `Produce`
repeat.

Clear the Kapture capture before running the fixed half:

```sh
pnpm demo:client -- producer-lifecycle fixed
```

The fixed client reuses one producer and sends all records through it. Expected:
one negotiated lifecycle, a real Produce batch, and no Producer-instance-leak
finding.

## Offset commit per record

The harness creates a consumer group, produces thirty records after assignment,
and processes only those records. The bad mode calls `commitSync()` after each
record:

```sh
pnpm demo:client -- offset-commit bad
```

Expected finding: **Overcommit**, with thirty `OffsetCommit` requests visible on
the wire.

Clear the capture, then run the fixed mode:

```sh
pnpm demo:client -- offset-commit fixed
```

The fixed client processes the same thirty records and commits once at the end.

## Options

```text
--broker <host:port>  Kafka bootstrap (default: 127.0.0.1:9092)
--topic <name>        Override the scenario topic
--count <n>           Override the record count
--group <id>          Override the offset-commit consumer group
```

The default counts intentionally clear Kapture's default detector thresholds.
Lower values remain useful for exploring the Protocol tab but may not create an
Expert finding.

## JVM tap instead of proxy

Build the JVM agent, start a tap session in Kapture, point the client directly at
the broker, and attach the agent at JVM startup:

```sh
mvn -q -DskipTests -f agents/jvm-tap/pom.xml package
java -javaagent:agents/jvm-tap/target/kapture-jvm-agent.jar \
  -jar demos/client-antipatterns/target/kapture-client-antipatterns.jar \
  producer-lifecycle bad --broker localhost:29092
```

The same harness therefore demonstrates both Kapture capture modes without any
SDK instrumentation in its application code.
