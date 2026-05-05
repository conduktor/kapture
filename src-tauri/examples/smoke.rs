//! Smoke test: connect to local Redpanda, consume 5 messages, print them, exit.
//!
//! Requires `docker compose up -d` and `node tools/seed.mjs` to have been run first.
//!
//! Usage:
//!   cargo run --manifest-path src-tauri/Cargo.toml --example smoke
//!
//! Override broker/topic via env: `KAPTURE_BROKER`, `KAPTURE_TOPICS` (comma-separated).

use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use tokio::time::timeout;

const DEFAULT_BROKER: &str = "localhost:19092";
const DEFAULT_TOPICS: &str = "orders.raw,orders.enriched,users.events";
const TARGET_COUNT: usize = 5;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = std::env::var("KAPTURE_BROKER").unwrap_or_else(|_| DEFAULT_BROKER.to_owned());
    let topics_env = std::env::var("KAPTURE_TOPICS").unwrap_or_else(|_| DEFAULT_TOPICS.to_owned());
    let topics: Vec<&str> = topics_env.split(',').map(str::trim).collect();

    println!("smoke: connecting to {broker}, subscribing to {topics:?}");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .set(
            "group.id",
            format!("kapture-smoke-{}", uuid::Uuid::new_v4().simple()),
        )
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "10000")
        .set("client.id", "kapture-smoke")
        .create()?;

    consumer.subscribe(&topics)?;

    let mut received = 0usize;
    while received < TARGET_COUNT {
        match timeout(Duration::from_secs(10), consumer.recv()).await {
            Ok(Ok(msg)) => {
                let key = msg
                    .key()
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .unwrap_or("<binary-key>");
                let payload = msg
                    .payload()
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .unwrap_or("<binary-payload>");
                let topic = msg.topic();
                let partition = msg.partition();
                let offset = msg.offset();
                println!(
                    "smoke: {topic}/{partition}@{offset} key={key} payload={}",
                    truncate(payload, 120)
                );
                received += 1;
            }
            Ok(Err(err)) => {
                eprintln!("smoke: consumer error: {err}");
            }
            Err(_) => {
                eprintln!("smoke: no message in 10s, is the seeder running?");
                std::process::exit(2);
            }
        }
    }

    println!("smoke: received {received} messages, OK");
    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_owned()
    } else {
        format!("{}…", &s[..max])
    }
}
