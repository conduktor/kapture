//! End-to-end test for the JVM tap mode.
//!
//! Walks the full pipeline: a Java Kafka producer (under
//! `tests/fixtures/jvm-test-client/`) attaches the Kapture JVM agent
//! (`agents/jvm-tap/target/kapture-jvm-agent.jar`), produces 10
//! messages to a real SSL-listener Apache Kafka broker, and the test
//! asserts that the JVM-tap listener captured the matching Kafka
//! request/response frames through `ProtoCorrelator`.
//!
//! Gating
//! ------
//! Two env vars must be set, otherwise the test prints a notice and
//! returns early (so `cargo test` on a vanilla checkout is green):
//!   * `KAPTURE_JVM_TAP_E2E=1`
//!   * `KAPTURE_KAFKA_SSL_BOOTSTRAP=localhost:39093`
//!
//! Prerequisites the test does NOT install for you (CI provisions
//! these in a previous step; a clear panic message tells you what's
//! missing if you run locally):
//!   * SSL Kafka broker reachable on `KAPTURE_KAFKA_SSL_BOOTSTRAP`
//!     (`docker compose --profile ssl up -d`).
//!   * JKS truststore at `tests/fixtures/certs/client.truststore.jks`
//!     (`tests/fixtures/certs/gen-certs.sh` writes it).
//!   * Two Maven artifacts pre-built:
//!     `agents/jvm-tap/target/kapture-jvm-agent.jar`
//!     `src-tauri/tests/fixtures/jvm-test-client/target/jvm-tap-app.jar`
//!     (each `mvn -q -DskipTests package`).
//!
//! Why not auto-build? `mvn package` on the dev box is 30-60s and
//! requires JDK 21+. CI does it once in a dedicated step; running it
//! per test would make `cargo test` painful.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use kapture_lib::example_api::{JvmTapConfig, JvmTapHandle, ProtoCorrelator};

fn env_or_skip(test_name: &str, key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping {test_name}: ${key} not set");
            None
        }
    }
}

/// Path to the repo root, computed from `CARGO_MANIFEST_DIR`. Used to
/// resolve absolute paths to the fixture JARs and certs regardless of
/// where the test is invoked from.
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("CARGO_MANIFEST_DIR has a parent (repo root)")
        .to_path_buf()
}

fn require_file(path: &Path, hint: &str) {
    assert!(
        path.exists(),
        "expected {} to exist for the JVM tap e2e test.\nhint: {hint}",
        path.display()
    );
}

fn ensure_topic_exists(bootstrap: &str) {
    // We've seen `auto-create-topics-enable=true` clusters still
    // return `Topic tap-test not present in metadata after 60000 ms`
    // when the producer's first Produce races the controller's
    // initial metadata propagation on a freshly-restarted broker.
    // Pre-creating the topic via `kafka-topics.sh` inside the broker
    // container makes the test deterministic. The container name
    // matches `docker-compose.yml`'s `kapture-kafka-ssl`.
    let _ = bootstrap; // bootstrap is for diagnostics in panics below
    let output = Command::new("docker")
        .args([
            "exec",
            "kapture-kafka-ssl",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "kafka-ssl:9092",
            "--create",
            "--topic",
            "tap-test",
            "--partitions",
            "1",
            "--replication-factor",
            "1",
            "--if-not-exists",
        ])
        .output()
        .expect("invoke docker exec kafka-topics.sh");
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "failed to pre-create tap-test topic: status={}\nstderr: {stderr}",
            output.status
        );
    }
}

#[tokio::test]
async fn jvm_tap_captures_produce_and_fetch_frames() {
    let Some(_flag) = env_or_skip(
        "jvm_tap_captures_produce_and_fetch_frames",
        "KAPTURE_JVM_TAP_E2E",
    ) else {
        return;
    };
    let Some(bootstrap) = env_or_skip(
        "jvm_tap_captures_produce_and_fetch_frames",
        "KAPTURE_KAFKA_SSL_BOOTSTRAP",
    ) else {
        return;
    };

    let root = repo_root();
    let agent_jar = root.join("agents/jvm-tap/target/kapture-jvm-agent.jar");
    let client_jar = root.join("src-tauri/tests/fixtures/jvm-test-client/target/jvm-tap-app.jar");
    let truststore = root.join("src-tauri/tests/fixtures/certs/client.truststore.jks");
    require_file(
        &agent_jar,
        "build with: (cd agents/jvm-tap && mvn -q -DskipTests package)",
    );
    require_file(
        &client_jar,
        "build with: (cd src-tauri/tests/fixtures/jvm-test-client && mvn -q -DskipTests package)",
    );
    require_file(
        &truststore,
        "generate with: src-tauri/tests/fixtures/certs/gen-certs.sh",
    );
    ensure_topic_exists(&bootstrap);

    // Use a per-test UDS path so two parallel runs don't fight over
    // /tmp/kapture-tap.sock.
    let tmp = tempfile::tempdir().expect("temp dir");
    let socket_path = tmp.path().join("kapture-tap.sock");

    let correlator = Arc::new(ProtoCorrelator::new());
    let tap = JvmTapHandle::start(
        JvmTapConfig::new(socket_path.clone()),
        Arc::clone(&correlator),
    )
    .await
    .expect("jvm-tap listener starts");

    // ------- producer pass -------
    let producer_status = Command::new("java")
        .args([
            &format!("-javaagent:{}", agent_jar.display()),
            "-Dio.kapture.tap.shaded.bytebuddy.experimental=true",
            &format!("-Dkapture.tap.socket={}", socket_path.display()),
            "--add-opens",
            "java.base/java.nio=ALL-UNNAMED",
            &format!("-Dtruststore={}", truststore.display()),
            &format!("-Dbootstrap={bootstrap}"),
            "-jar",
        ])
        .arg(&client_jar)
        .arg("producer")
        .status()
        .expect("spawn java producer");
    assert!(
        producer_status.success(),
        "producer exited with {producer_status}; is the SSL broker reachable on {bootstrap}?"
    );

    // ------- consumer pass -------
    let consumer_status = Command::new("java")
        .args([
            &format!("-javaagent:{}", agent_jar.display()),
            "-Dio.kapture.tap.shaded.bytebuddy.experimental=true",
            &format!("-Dkapture.tap.socket={}", socket_path.display()),
            "--add-opens",
            "java.base/java.nio=ALL-UNNAMED",
            &format!("-Dtruststore={}", truststore.display()),
            &format!("-Dbootstrap={bootstrap}"),
            "-jar",
        ])
        .arg(&client_jar)
        .arg("consumer")
        .status()
        .expect("spawn java consumer");
    assert!(
        consumer_status.success(),
        "consumer exited with {consumer_status}"
    );

    // Give the tap publisher's shutdown drain a moment to flush.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ------- assert -------
    let summaries = correlator.summaries(10_000);
    let api_names: std::collections::HashSet<String> =
        summaries.iter().map(|s| s.api_name.clone()).collect();

    // The Kafka client always handshakes with ApiVersions and resolves
    // brokers via Metadata before doing anything else. If those are
    // missing the tap didn't capture the bootstrap connection at all.
    assert!(
        api_names.contains("ApiVersionsRequest"),
        "expected at least one ApiVersionsRequest, got: {api_names:?}"
    );
    assert!(
        api_names.contains("MetadataRequest"),
        "expected at least one MetadataRequest, got: {api_names:?}"
    );

    // Producer side: at least one ProduceRequest (the batch carries
    // all 10 records).
    assert!(
        api_names.contains("ProduceRequest"),
        "expected at least one ProduceRequest, got: {api_names:?}"
    );

    // Consumer side: at least one FetchRequest, plus the group RPCs
    // the consumer uses to join `tap-test`.
    assert!(
        api_names.contains("FetchRequest"),
        "expected at least one FetchRequest, got: {api_names:?}"
    );

    // Per-side request/response pairing: every captured request
    // should have a non-empty payload (the agent ships the full
    // wire frame, so `size > 4` is the floor — 4 bytes is the
    // length prefix alone).
    for s in &summaries {
        assert!(s.size > 4, "captured frame too short: {s:?}");
    }

    tap.stop().await;
}
