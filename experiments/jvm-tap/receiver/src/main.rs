//! Kapture JVM-tap receiver.
//!
//! Listens on a Unix Domain Socket, accepts one connection (the JVM agent),
//! reads length-prefixed frames and prints them. Prints a summary on Ctrl-C
//! or after 30s of idle.
//!
//! Frame layout (little-endian):
//!   u8   direction  (0 = outgoing/write, 1 = incoming/read)
//!   u64  nanos
//!   u32  connection_id
//!   u32  payload_len
//!   ...  payload bytes

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::io::AsyncReadExt;
use tokio::net::UnixListener;
use tokio::signal;
use tokio::time::timeout;

const SOCKET_PATH: &str = "/tmp/kapture-tap.sock";
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Default)]
struct Stats {
    frames: u64,
    bytes: u64,
    conns: HashSet<u32>,
    started: Option<Instant>,
}

impl Stats {
    fn record(&mut self, conn: u32, payload_len: u32) {
        if self.started.is_none() {
            self.started = Some(Instant::now());
        }
        self.frames += 1;
        self.bytes += payload_len as u64;
        self.conns.insert(conn);
    }

    fn summary(&self) {
        let dur = self.started.map(|s| s.elapsed()).unwrap_or_default();
        eprintln!();
        eprintln!("=== kapture-tap-receiver summary ===");
        eprintln!("  duration         : {:.2?}", dur);
        eprintln!("  frames           : {}", self.frames);
        eprintln!("  bytes            : {}", self.bytes);
        eprintln!("  distinct conn_id : {}", self.conns.len());
        if !self.conns.is_empty() {
            let mut ids: Vec<_> = self.conns.iter().copied().collect();
            ids.sort_unstable();
            eprintln!("  conn_ids         : {:?}", ids);
        }
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let path = PathBuf::from(
        std::env::var("KAPTURE_TAP_SOCKET").unwrap_or_else(|_| SOCKET_PATH.to_string()),
    );

    // Stale socket file? Remove.
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path)?;
    eprintln!("[receiver] listening on {}", path.display());

    let mut stats = Stats::default();

    let accept = async {
        let (stream, _) = listener.accept().await?;
        eprintln!("[receiver] agent connected");
        Ok::<_, std::io::Error>(stream)
    };

    let stream = tokio::select! {
        s = accept => s?,
        _ = signal::ctrl_c() => {
            eprintln!("[receiver] ctrl-c before connect");
            stats.summary();
            let _ = std::fs::remove_file(&path);
            return Ok(());
        }
    };

    let mut stream = stream;

    loop {
        let read = read_frame(&mut stream);
        tokio::pin!(read);

        let result = tokio::select! {
            r = timeout(IDLE_TIMEOUT, &mut read) => r,
            _ = signal::ctrl_c() => {
                eprintln!("[receiver] ctrl-c");
                break;
            }
        };

        match result {
            Err(_elapsed) => {
                eprintln!("[receiver] idle {}s — stopping", IDLE_TIMEOUT.as_secs());
                break;
            }
            Ok(Ok(Some(frame))) => {
                stats.record(frame.conn_id, frame.payload.len() as u32);
                print_frame(&frame);
            }
            Ok(Ok(None)) => {
                eprintln!("[receiver] agent disconnected");
                break;
            }
            Ok(Err(e)) => {
                eprintln!("[receiver] read error: {e}");
                break;
            }
        }
    }

    stats.summary();
    let _ = std::fs::remove_file(&path);
    Ok(())
}

struct Frame {
    direction: u8,
    nanos: u64,
    conn_id: u32,
    payload: Vec<u8>,
}

async fn read_frame<R: AsyncReadExt + Unpin>(r: &mut R) -> std::io::Result<Option<Frame>> {
    let mut header = [0u8; 1 + 8 + 4 + 4];
    match r.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let direction = header[0];
    let nanos = u64::from_le_bytes(header[1..9].try_into().unwrap());
    let conn_id = u32::from_le_bytes(header[9..13].try_into().unwrap());
    let payload_len = u32::from_le_bytes(header[13..17].try_into().unwrap()) as usize;

    // Sanity cap: 8 MiB. Beyond that, the stream is almost certainly desynced.
    if payload_len > 8 * 1024 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("payload_len {payload_len} exceeds 8 MiB cap"),
        ));
    }

    let mut payload = vec![0u8; payload_len];
    r.read_exact(&mut payload).await?;
    Ok(Some(Frame { direction, nanos, conn_id, payload }))
}

fn print_frame(f: &Frame) {
    let dir = match f.direction {
        0 => 'W',
        1 => 'R',
        _ => '?',
    };
    let hex_preview: String = f
        .payload
        .iter()
        .take(80)
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ");
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    println!(
        "[conn={} dir={} t={} jnanos={}] {} bytes: {}{}",
        f.conn_id,
        dir,
        now_ms,
        f.nanos,
        f.payload.len(),
        hex_preview,
        if f.payload.len() > 80 { " ..." } else { "" }
    );
}
