#!/usr/bin/env bash
# Generate self-signed JKS keystore + truststore for the SSL Kafka broker.
# Idempotent: skips generation when all output files exist.
#
# Outputs (in this script's directory):
#   broker.keystore.jks      — broker's private key + cert (CN=localhost, SAN=localhost,kafka-ssl)
#   broker.truststore.jks    — trust store the broker uses (here it just trusts itself)
#   client.truststore.jks    — trust store the Java client uses to validate the broker cert
#
# Password for all stores: kapture (dev only, NEVER reuse).
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

PASS="kapture"
DAYS=3650
CN="localhost"
SAN="dns:localhost,dns:kafka-ssl,ip:127.0.0.1"

# Files we expect to exist when generation is complete.
ARTIFACTS=(
  broker.keystore.jks
  broker.truststore.jks
  client.truststore.jks
  broker_keystore_creds
  broker_key_creds
)

all_present=1
for f in "${ARTIFACTS[@]}"; do
  if [[ ! -f "$f" ]]; then
    all_present=0
    break
  fi
done

if [[ "$all_present" == "1" ]]; then
  echo "[gen-certs] All cert artifacts already present in $DIR, skipping."
  exit 0
fi

echo "[gen-certs] Generating self-signed certs in $DIR ..."

# Clean any half-generated state to keep things deterministic.
rm -f broker.keystore.jks broker.truststore.jks client.truststore.jks broker.cer ca.cer ca.key ca.crt ca.srl broker.csr broker.signed.cer

# 1) Broker keystore with a self-signed key pair. SAN ensures the cert
#    is valid for both `localhost` (client connects via localhost:39093)
#    and `kafka-ssl` (in-network DNS).
keytool -genkeypair \
  -alias kafka-ssl \
  -keyalg RSA -keysize 2048 \
  -validity "$DAYS" \
  -dname "CN=$CN, OU=Kapture, O=Kapture, L=Paris, S=IDF, C=FR" \
  -ext "san=$SAN" \
  -keystore broker.keystore.jks \
  -storepass "$PASS" -keypass "$PASS" \
  -storetype JKS \
  -noprompt

# 2) Export the broker's public cert.
keytool -exportcert \
  -alias kafka-ssl \
  -file broker.cer \
  -keystore broker.keystore.jks \
  -storepass "$PASS"

# 3) Broker truststore — only trusts itself (inter-broker would need this
#    if we enabled mTLS; we don't, but keep symmetry for completeness).
keytool -importcert \
  -alias kafka-ssl \
  -file broker.cer \
  -keystore broker.truststore.jks \
  -storepass "$PASS" \
  -storetype JKS \
  -noprompt

# 4) Client truststore — trusts the broker cert.
keytool -importcert \
  -alias kafka-ssl \
  -file broker.cer \
  -keystore client.truststore.jks \
  -storepass "$PASS" \
  -storetype JKS \
  -noprompt

rm -f broker.cer

# 5) Password files for the apache/kafka image entrypoint. It reads the
#    password via `cat "$KAFKA_SSL_*_CREDENTIALS"` so the file must NOT
#    have a trailing newline.
printf 'kapture' > broker_keystore_creds
printf 'kapture' > broker_key_creds

echo "[gen-certs] Done. Files:"
ls -1 *.jks
