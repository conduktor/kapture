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

use std::sync::atomic::{AtomicUsize, Ordering};

use kapture_lib::example_api::{
    CapturedMessage, ProtoCorrelator, ProtoDirection, ProxyConfig, ProxyHandle, RecordSink,
    UpstreamSaslConfig, UpstreamSaslMechanism,
};
use tokio::time::{interval, MissedTickBehavior};
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
struct Args {
    upstream: String,
    listen_port: u16,
    seconds: u64,
    sasl_mechanism: String,
    sasl_username: Option<String>,
    sasl_password: Option<String>,
}

fn parse_args() -> Result<Args, String> {
    let mut upstream = "localhost:39092".to_owned();
    let mut listen_port: u16 = 9092;
    let mut seconds: u64 = 60;
    let mut sasl_mechanism = "PLAIN".to_owned();
    let mut sasl_username: Option<String> = None;
    let mut sasl_password: Option<String> = None;

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
            "--sasl-mechanism" => {
                sasl_mechanism = iter.next().ok_or("--sasl-mechanism needs a value")?;
            }
            "--sasl-username" => {
                sasl_username = Some(iter.next().ok_or("--sasl-username needs a value")?);
            }
            "--sasl-password" => {
                sasl_password = Some(iter.next().ok_or("--sasl-password needs a value")?);
            }
            "-h" | "--help" => {
                println!("usage: proxy_smoke [--upstream HOST:PORT] [--listen PORT] [--seconds N] [--sasl-mechanism PLAIN|SCRAM-SHA-256|SCRAM-SHA-512] [--sasl-username U --sasl-password P]");
                std::process::exit(0);
            }
            other => return Err(format!("unknown arg: {other}")),
        }
    }
    Ok(Args {
        upstream,
        listen_port,
        seconds,
        sasl_mechanism,
        sasl_username,
        sasl_password,
    })
}

fn build_proxy_config(args: &Args) -> Result<ProxyConfig, Box<dyn std::error::Error>> {
    let mut cfg = ProxyConfig::new(args.upstream.clone(), args.listen_port);
    if let (Some(u), Some(p)) = (args.sasl_username.as_ref(), args.sasl_password.as_ref()) {
        let mechanism = match args.sasl_mechanism.to_uppercase().as_str() {
            "PLAIN" => UpstreamSaslMechanism::Plain,
            "SCRAM-SHA-256" => UpstreamSaslMechanism::ScramSha256,
            "SCRAM-SHA-512" => UpstreamSaslMechanism::ScramSha512,
            other => {
                return Err(format!(
                    "unsupported --sasl-mechanism `{other}` (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)"
                )
                .into());
            }
        };
        cfg = cfg.with_sasl(UpstreamSaslConfig {
            mechanism,
            username: u.clone(),
            password: p.clone(),
        });
    }
    Ok(cfg)
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
    let sasl_enabled = args.sasl_username.is_some() && args.sasl_password.is_some();
    println!(
        "proxy_smoke: upstream={} listen=127.0.0.1:{} budget={}s sasl={}",
        args.upstream,
        args.listen_port,
        args.seconds,
        if sasl_enabled {
            args.sasl_mechanism.as_str()
        } else {
            "off"
        },
    );

    let correlator = Arc::new(ProtoCorrelator::new());
    let cfg = build_proxy_config(&args)?;
    let captured_count = Arc::new(AtomicUsize::new(0));
    let captured_count_for_sink = Arc::clone(&captured_count);
    let sink: RecordSink = Arc::new(move |msg: CapturedMessage| {
        captured_count_for_sink.fetch_add(1, Ordering::Relaxed);
        println!(
            "RECORD topic={} partition={} offset={} key={:?} size={}",
            msg.topic, msg.partition, msg.offset, msg.key, msg.size_bytes,
        );
    });
    let handle = ProxyHandle::start(cfg, Arc::clone(&correlator), sink).await?;
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
                            s.connection_id,
                            s.size,
                            s.rtt_ms,
                        );
                    }
                    already_printed = summaries.len();
                }
            }
        }
    }

    let topic_id_map = handle.topic_id_map();
    handle.stop().await;
    let final_summaries = correlator.summaries(500);
    let topic_snapshot = topic_id_map.snapshot();
    println!(
        "proxy_smoke: stopped. total frames observed: {} | captured {} messages | topic_id_map size: {}",
        final_summaries.len(),
        captured_count.load(Ordering::Relaxed),
        topic_snapshot.len(),
    );
    for (id, name) in &topic_snapshot {
        println!("  topic_id {id} -> {name}");
    }
    Ok(())
}
