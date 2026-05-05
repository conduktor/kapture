//! End-to-end smoke test for the Schema Registry path.
//!
//! Requires `docker compose up -d` and `node tools/seed.mjs`.
//! Connects with rdkafka, reads a few messages from the SR-backed topics,
//! parses the Confluent envelope, fetches the schema, and Avro-decodes the
//! payload. Independent from the Tauri lib so it serves as an integration
//! check on the dependency stack.
//!
//! Usage:
//!   `cargo run --manifest-path src-tauri/Cargo.toml --example sr_smoke`

use std::time::Duration;

use apache_avro::{from_avro_datum, types::Value as AvroValue, Schema};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use serde::Deserialize;
use tokio::time::timeout;

const BROKER: &str = "localhost:19092";
const REGISTRY: &str = "http://localhost:18081";
const TOPICS: [&str; 2] = ["orders.avro", "orders.jsonschema"];
const TARGET: usize = 4;

#[derive(Debug, Deserialize)]
struct RegistryResponse {
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
    subject: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("sr_smoke: broker={BROKER} registry={REGISTRY}");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .set(
            "group.id",
            format!("kapture-sr-smoke-{}", uuid::Uuid::new_v4().simple()),
        )
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "10000")
        .set("client.id", "kapture-sr-smoke")
        .create()?;
    consumer.subscribe(&TOPICS)?;

    let http = reqwest::Client::new();

    let mut received = 0usize;
    while received < TARGET {
        match timeout(Duration::from_secs(10), consumer.recv()).await {
            Ok(Ok(msg)) => {
                let topic = msg.topic();
                let partition = msg.partition();
                let offset = msg.offset();
                let bytes = msg.payload().unwrap_or(&[]);
                if bytes.len() < 5 || bytes[0] != 0x00 {
                    println!(
                        "sr_smoke: {topic}/{partition}@{offset} no Confluent envelope ({} bytes)",
                        bytes.len()
                    );
                    received += 1;
                    continue;
                }
                let id = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
                let payload = &bytes[5..];

                let url = format!("{REGISTRY}/schemas/ids/{id}");
                let response: RegistryResponse = http.get(&url).send().await?.json().await?;
                let kind = response.schema_type.as_deref().unwrap_or("AVRO");

                println!(
                    "sr_smoke: {topic}/{partition}@{offset} schema_id={id} kind={kind} subject={:?}",
                    response.subject
                );

                match kind {
                    "AVRO" => {
                        let schema = Schema::parse_str(&response.schema)?;
                        let mut cursor = std::io::Cursor::new(payload);
                        let decoded = from_avro_datum(&schema, &mut cursor, None)?;
                        println!("  decoded: {}", brief_avro(&decoded));
                    }
                    "JSON" => {
                        let value: serde_json::Value = serde_json::from_slice(payload)?;
                        println!("  decoded: {value}");
                    }
                    other => println!("  decoder for {other} not implemented in smoke"),
                }
                received += 1;
            }
            Ok(Err(err)) => eprintln!("sr_smoke: consumer error: {err}"),
            Err(_) => {
                eprintln!("sr_smoke: no message in 10s — is the seeder running?");
                std::process::exit(2);
            }
        }
    }

    println!("sr_smoke: received {received} messages, OK");
    Ok(())
}

fn brief_avro(value: &AvroValue) -> String {
    match value {
        AvroValue::Record(fields) => {
            let mut parts = Vec::new();
            for (name, val) in fields {
                parts.push(format!("{name}={}", brief_avro(val)));
            }
            format!("{{{}}}", parts.join(", "))
        }
        AvroValue::String(s) | AvroValue::Enum(_, s) => format!("\"{s}\""),
        AvroValue::Int(n) => n.to_string(),
        AvroValue::Long(n) => n.to_string(),
        AvroValue::Float(n) => n.to_string(),
        AvroValue::Double(n) => n.to_string(),
        AvroValue::Boolean(b) => b.to_string(),
        AvroValue::Array(items) => {
            let inner: Vec<String> = items.iter().map(brief_avro).collect();
            format!("[{}]", inner.join(", "))
        }
        AvroValue::Null => "null".to_owned(),
        other => format!("{other:?}"),
    }
}
