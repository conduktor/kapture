#!/usr/bin/env bash
# Baseline test for the JVM-tap experiment: prove that a real Java
# Kafka client can produce + consume 10 messages over TLS against a
# real Apache Kafka broker. No agent attached yet — this is the
# pre-condition that any later eBPF-vs-JVM-bytecode comparison rests
# on. If this fails, nothing downstream matters.
#
# Steps:
#   1. Generate self-signed certs (idempotent).
#   2. Bring up `ssl` docker-compose profile, wait for healthy.
#   3. Run the producer (sends 10 records).
#   4. Run the consumer (reads, expects 10).
#   5. PASS/FAIL on the consumer exit code.
#
# Leaves the broker running so the JVM-agent side can attach next.
# Pass --teardown to bring it down at the end.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
CERTS_DIR="$HERE/certs"
APP_DIR="$HERE/java-app"
JAR="$APP_DIR/target/jvm-tap-app.jar"

TEARDOWN=0
for arg in "$@"; do
  case "$arg" in
    --teardown) TEARDOWN=1 ;;
    *) echo "Unknown arg: $arg (only --teardown is recognized)"; exit 2 ;;
  esac
done

log() { echo "[baseline] $*"; }
fail() { echo "[baseline] FAIL: $*" >&2; exit 1; }

# --- Step 1: certs ----------------------------------------------------------
log "Generating certs (idempotent) ..."
bash "$CERTS_DIR/gen-certs.sh"

# --- Step 2: docker compose -------------------------------------------------
log "Starting ssl profile ..."
(cd "$REPO_ROOT" && docker compose --profile ssl up -d kafka-ssl)

log "Waiting for kafka-ssl healthcheck ..."
deadline=$(( $(date +%s) + 120 ))
while :; do
  status="$(docker inspect -f '{{.State.Health.Status}}' kapture-kafka-ssl 2>/dev/null || echo missing)"
  case "$status" in
    healthy) log "Broker healthy."; break ;;
    unhealthy) fail "Broker reported unhealthy. Check: docker logs kapture-kafka-ssl" ;;
    missing)   fail "Container kapture-kafka-ssl not found." ;;
  esac
  if [[ $(date +%s) -gt $deadline ]]; then
    fail "Timed out waiting for broker to become healthy after 120s."
  fi
  sleep 2
done

# --- Step 3 & 4: producer + consumer ----------------------------------------
if [[ ! -f "$JAR" ]]; then
  fail "$JAR not found. Build first: (cd $APP_DIR && mvn -q -DskipTests package)"
fi

log "Running producer ..."
( cd "$APP_DIR" && java -jar "$JAR" producer ) || fail "Producer failed."

log "Running consumer ..."
set +e
( cd "$APP_DIR" && java -jar "$JAR" consumer )
consumer_rc=$?
set -e

# --- Step 5: verdict --------------------------------------------------------
if [[ "$TEARDOWN" == "1" ]]; then
  log "Tearing down ssl profile ..."
  (cd "$REPO_ROOT" && docker compose --profile ssl down)
fi

if [[ $consumer_rc -eq 0 ]]; then
  echo "[baseline] PASS — 10 messages flowed end-to-end over SSL."
  exit 0
else
  echo "[baseline] FAIL — consumer exit code $consumer_rc (expected 0)." >&2
  exit 1
fi
