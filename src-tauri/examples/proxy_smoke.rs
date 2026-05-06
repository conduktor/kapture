//! Programmatic smoke for the proxy mode.
//!
//! Brings up a `ProxyHandle` instance (no Tauri), forwards a local
//! port to an upstream Kafka broker, and prints `ProtoCorrelator`
//! frame summaries every second so an external `kcat` client can
//! drive it for an end-to-end multi-broker rewrite check.
//!
//! Usage:
//! ```text
//! pnpm stack:up:mb            # 3-broker KRaft cluster on 39092/93/94
//! cargo run --manifest-path src-tauri/Cargo.toml \
//!     --example proxy_smoke -- --upstream localhost:39092 --listen 9092
//! # In another shell:
//! kcat -b localhost:9092 -L
//! echo "k:v" | kcat -b localhost:9092 -P -t mb-test -K:
//! kcat -b localhost:9092 -C -t mb-test -e -o beginning
//! ```
//!
//! Ctrl-C exits cleanly (drains listeners). The poll loop also has an
//! upper bound (`--seconds`) so CI runs terminate without a TTY.

use std::sync::Arc;
use std::time::Duration;

use kapture_lib::example_api::{ProtoCorrelator, ProtoDirection, ProxyConfig, ProxyHandle};
use tokio::time::{interval, MissedTickBehavior};
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
struct Args {
    upstream: String,
    listen_port: u16,
    seconds: u64,
}

fn parse_args() -> Result<Args, String> {
    let mut upstream = "localhost:39092".to_owned();
    let mut listen_port: u16 = 9092;
    let mut seconds: u64 = 60;

    let mut iter = std::env::args().skip(1);
    while let Some(a) = iter.next() {
        match a.as_str() {
            "--upstream" => {
                upstream = iter.next().ok_or("--upstream needs a value")?;
            }
            "--listen" => {
                let v = iter.next().ok_or("--listen needs a value")?;
                listen_port = v.parse().map_err(|e| format!("--listen: {e}"))?;
            }
            "--seconds" => {
                let v = iter.next().ok_or("--seconds needs a value")?;
                seconds = v.parse().map_err(|e| format!("--seconds: {e}"))?;
            }
            "-h" | "--help" => {
                println!("usage: proxy_smoke [--upstream HOST:PORT] [--listen PORT] [--seconds N]");
                std::process::exit(0);
            }
            other => return Err(format!("unknown arg: {other}")),
        }
    }
    Ok(Args {
        upstream,
        listen_port,
        seconds,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .try_init();

    let args = parse_args().map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    println!(
        "proxy_smoke: upstream={} listen=127.0.0.1:{} budget={}s",
        args.upstream, args.listen_port, args.seconds,
    );

    let correlator = Arc::new(ProtoCorrelator::new());
    let cfg = ProxyConfig::new(args.upstream.clone(), args.listen_port);
    let handle = ProxyHandle::start(cfg, Arc::clone(&correlator)).await?;
    println!(
        "proxy_smoke: bootstrap listener bound at {}",
        handle.local_addr()
    );

    let mut already_printed: usize = 0;
    let mut tick = interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(args.seconds);

    loop {
        tokio::select! {
            () = async {
                let _ = tokio::signal::ctrl_c().await;
            } => {
                println!("proxy_smoke: ctrl-c received, stopping");
                break;
            }
            () = tokio::time::sleep_until(deadline) => {
                println!("proxy_smoke: time budget reached, stopping");
                break;
            }
            _ = tick.tick() => {
                let summaries = correlator.summaries(500);
                if summaries.len() > already_printed {
                    for s in summaries.iter().skip(already_printed) {
                        let dir = match s.direction {
                            ProtoDirection::Send => "->",
                            ProtoDirection::Recv => "<-",
                        };
                        println!(
                            "FRAME {dir} api={:<16} v{:<3} corr=0x{:08x} conn={} size={} rtt={:.1}ms",
                            s.api_name,
                            s.api_version,
                            s.corr_id,
                            s.broker_id,
                            s.size,
                            s.rtt_ms,
                        );
                    }
                    already_printed = summaries.len();
                }
            }
        }
    }

    handle.stop().await;
    let final_summaries = correlator.summaries(500);
    println!(
        "proxy_smoke: stopped. total frames observed: {}",
        final_summaries.len()
    );
    Ok(())
}
