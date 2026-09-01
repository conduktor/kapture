//! Headless JVM-tap listener for performance and integration runs.
//!
//! Starts the same bounded UDS/reassembly/correlator path as the desktop app,
//! without renderer or terminal-per-frame overhead.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use kapture_lib::example_api::{JvmTapConfig, JvmTapHandle, ProtoCorrelator, ProtoDirection};
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
struct Args {
    socket: PathBuf,
    seconds: u64,
}

fn parse_args() -> Result<Args, String> {
    let mut socket = PathBuf::from("/tmp/kapture-perf-tap.sock");
    let mut seconds = 60;
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--socket" => {
                socket = PathBuf::from(arguments.next().ok_or("--socket needs a value")?);
            }
            "--seconds" => {
                seconds = arguments
                    .next()
                    .ok_or("--seconds needs a value")?
                    .parse()
                    .map_err(|error| format!("--seconds: {error}"))?;
            }
            "--help" | "-h" => {
                println!("usage: jvm_tap_smoke [--socket PATH] [--seconds N]");
                std::process::exit(0);
            }
            unknown => return Err(format!("unknown argument: {unknown}")),
        }
    }
    if seconds == 0 {
        return Err("--seconds must be positive".to_owned());
    }
    Ok(Args { socket, seconds })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with_target(false)
        .try_init();
    let arguments = parse_args().map_err(|error| -> Box<dyn std::error::Error> { error.into() })?;
    let correlator = Arc::new(ProtoCorrelator::new());
    let tap = JvmTapHandle::start(
        JvmTapConfig::new(arguments.socket.clone()),
        Arc::clone(&correlator),
    )
    .await?;
    println!(
        "jvm_tap_smoke: listening at {}",
        tap.socket_path().display()
    );

    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result?;
            println!("jvm_tap_smoke: ctrl-c received, stopping");
        }
        () = tokio::time::sleep(Duration::from_secs(arguments.seconds)) => {
            println!("jvm_tap_smoke: time budget reached, stopping");
        }
    }

    tap.stop().await;
    let summaries = correlator.summaries(100_000);
    let sends = summaries
        .iter()
        .filter(|summary| matches!(summary.direction, ProtoDirection::Send))
        .count();
    let receives = summaries.len() - sends;
    let analyzed_frames = correlator.analyzed_frames();
    let analyzer_pending = correlator.analyzer_pending();
    let analyzer_drops = correlator.analyzer_drops();
    let agent_drops = correlator.agent_drops();
    let record_extraction_drops = correlator.record_extraction_drops();
    println!(
        "jvm_tap_smoke: stopped; analyzed_frames={analyzed_frames} analyzer_pending={analyzer_pending} retained_frames={} retained_sends={} retained_receives={} analyzer_drops={analyzer_drops} agent_drops={agent_drops} record_extraction_drops={record_extraction_drops}",
        summaries.len(),
        sends,
        receives,
    );
    if analyzed_frames == 0 {
        return Err("no JVM tap frames were analyzed".into());
    }
    if analyzer_pending != 0
        || analyzer_drops != 0
        || agent_drops != 0
        || record_extraction_drops != 0
    {
        return Err("JVM tap capture health is incomplete".into());
    }
    Ok(())
}
