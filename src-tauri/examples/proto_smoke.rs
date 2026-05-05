//! End-to-end smoke test for the Rust-side proto-hook integration.
//!
//! Builds against the patched librdkafka (vendor/librdkafka). Creates a
//! high-level `StreamConsumer`, installs the proto hook via the new
//! `rd_kafka_set_proto_hook_cb` setter, consumes a few messages, and
//! prints every protocol frame seen by the broker thread.
//!
//! Requires `pnpm stack:up` and `pnpm seed`.
//!
//! Usage:
//!   `cargo run --manifest-path src-tauri/Cargo.toml --example proto_smoke`

#![allow(unsafe_code)]

use std::ffi::c_void;
use std::os::raw::{c_double, c_int};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rdkafka::client::Client;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use tokio::time::timeout;

const BROKER: &str = "localhost:19092";
const TOPIC: &str = "orders.raw";

const PROTO_DIR_SEND: c_int = 0;

type CHookFn = unsafe extern "C" fn(
    rk: *mut c_void,
    dir: c_int,
    api_key: c_int,
    api_version: c_int,
    corr_id: i32,
    broker_id: i32,
    payload_size: usize,
    rtt_ms: c_double,
    opaque: *mut c_void,
);

#[link(name = "rdkafka")]
extern "C" {
    fn rd_kafka_set_proto_hook_cb(rk: *mut c_void, cb: Option<CHookFn>, opaque: *mut c_void);
}

struct Counters {
    sends: AtomicUsize,
    recvs: AtomicUsize,
}

unsafe extern "C" fn proto_hook(
    _rk: *mut c_void,
    dir: c_int,
    api_key: c_int,
    api_version: c_int,
    corr_id: i32,
    broker_id: i32,
    payload_size: usize,
    rtt_ms: c_double,
    opaque: *mut c_void,
) {
    if opaque.is_null() {
        return;
    }
    let counters = unsafe { &*opaque.cast::<Counters>() };
    let name = api_name(api_key);
    if dir == PROTO_DIR_SEND {
        counters.sends.fetch_add(1, Ordering::Relaxed);
        println!(
            "→ SEND api={name:<16} v{api_version} corr_id=0x{corr_id:08x} broker={broker_id} size={payload_size}"
        );
    } else {
        counters.recvs.fetch_add(1, Ordering::Relaxed);
        println!(
            "← RECV api={name:<16} v{api_version} corr_id=0x{corr_id:08x} broker={broker_id} size={payload_size} rtt={rtt_ms:.1}ms"
        );
    }
}

const fn api_name(api_key: i32) -> &'static str {
    match api_key {
        0 => "Produce",
        1 => "Fetch",
        2 => "ListOffsets",
        3 => "Metadata",
        8 => "OffsetCommit",
        9 => "OffsetFetch",
        10 => "FindCoordinator",
        11 => "JoinGroup",
        12 => "Heartbeat",
        13 => "LeaveGroup",
        14 => "SyncGroup",
        18 => "ApiVersions",
        _ => "Other",
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let counters = Arc::new(Counters {
        sends: AtomicUsize::new(0),
        recvs: AtomicUsize::new(0),
    });

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .set(
            "group.id",
            format!("kapture-proto-smoke-{}", uuid::Uuid::new_v4().simple()),
        )
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "10000")
        .set("client.id", "kapture-proto-smoke")
        .create()?;

    // Install the hook directly on the live `rd_kafka_t`.
    let client: &Client<_> = consumer.client();
    let rk_ptr: *mut c_void = client.native_ptr().cast();
    let counters_raw = Arc::as_ptr(&counters).cast::<c_void>().cast_mut();
    unsafe {
        rd_kafka_set_proto_hook_cb(rk_ptr, Some(proto_hook), counters_raw);
    }

    consumer.subscribe(&[TOPIC])?;

    println!("proto_smoke: connected to {BROKER}, subscribed to {TOPIC}");

    let mut messages = 0usize;
    while messages < 5 {
        match timeout(Duration::from_secs(10), consumer.recv()).await {
            Ok(Ok(msg)) => {
                println!("  msg {}/{}@{}", msg.topic(), msg.partition(), msg.offset());
                messages += 1;
            }
            Ok(Err(err)) => eprintln!("proto_smoke: consumer error: {err}"),
            Err(_) => {
                eprintln!("proto_smoke: no message in 10s — is the seeder running?");
                std::process::exit(2);
            }
        }
    }

    // Detach before the consumer is destroyed.
    unsafe {
        rd_kafka_set_proto_hook_cb(rk_ptr, None, std::ptr::null_mut());
    }
    drop(consumer);

    let sends = counters.sends.load(Ordering::Relaxed);
    let recvs = counters.recvs.load(Ordering::Relaxed);
    println!("\n=== summary ===");
    println!("SEND frames observed: {sends}");
    println!("RECV frames observed: {recvs}");
    println!("messages consumed:    {messages}");
    if sends == 0 || recvs == 0 {
        eprintln!("FAIL: hook did not fire (need both SEND and RECV)");
        std::process::exit(2);
    }
    Ok(())
}
