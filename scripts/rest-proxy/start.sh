#!/usr/bin/env bash
# Start a single-node Kafka broker with the REST proxy demo listeners.
# Defaults match the Strimzi deployment so test.sh can target either backend
# with the same env vars:
#   PLAINTEXT://localhost:9092                (binary Kafka protocol)
#   CONTROLLER://localhost:9093               (KRaft controller)
#   HTTP://0.0.0.0:${HTTP_PORT:-9090}         (REST proxy, plain)
#   HTTPS://0.0.0.0:${HTTPS_PORT:-8443}       (REST proxy, TLS)
#
# Overridable env vars (and their defaults):
#   KAFKA_PORT=9092  CONTROLLER_PORT=9093  HTTP_PORT=9090  HTTPS_PORT=8443
#
# Set HTTPS_PORT= (empty) to disable the HTTPS listener entirely.
#
# Does the setup once, idempotently:
#   1. Ensures the project jars are built.
#   2. Generates a self-signed keystore at tmp/server.keystore.jks if missing.
#   3. Writes tmp/rest-proxy-demo.properties (listeners line is rewritten
#      every run to track port-env-var changes; other edits are preserved).
#   4. Uses a persistent log directory at tmp/kafka-logs so topics and data
#      survive a clean (Ctrl-C) restart. Only formats it on first run.
#   5. Launches the broker in the foreground (Ctrl-C to stop cleanly).
#
# Reset the state (drop all topics / offsets) with:
#   rm -rf tmp/kafka-logs
# If the broker was killed hard and the next boot hits locked segments on
# Windows, the same reset command clears it.
#
# Usage:
#   bash scripts/rest-proxy/start.sh
#
# Run from the repo root.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

KAFKA_PORT="${KAFKA_PORT:-9092}"
CONTROLLER_PORT="${CONTROLLER_PORT:-9093}"
HTTP_PORT="${HTTP_PORT:-9090}"
HTTPS_PORT="${HTTPS_PORT:-8443}"

# Native-style path for things we write into server.properties. On Git Bash,
# `pwd` gives /d/... which Java on Windows can't resolve; cygpath -m converts
# to D:/... (mixed form — forward slashes, drive letter). On Linux/macOS
# cygpath isn't present and the POSIX path is already what Java expects.
if command -v cygpath >/dev/null 2>&1; then
  ROOT_NATIVE="$(cygpath -m "$ROOT")"
else
  ROOT_NATIVE="$ROOT"
fi

# The Windows Git Bash env-var workaround — kafka-run-class.sh's default
# CLASSPATH-filter regex contains | characters that otherwise get mangled.
export regex='(-(test|test-sources|src|scaladoc|javadoc)\.jar|jar\.asc|connect-file.*\.jar)$'

STATE_DIR="tmp"
KEYSTORE="$STATE_DIR/server.keystore.jks"
PROPERTIES="$STATE_DIR/rest-proxy-demo.properties"
# Persistent log dir — topics and data survive a clean restart.
# If you ever need a reset (e.g. the broker was killed hard and the next
# boot trips on locked segments): rm -rf "$LOG_DIR"
LOG_DIR="$STATE_DIR/kafka-logs"

mkdir -p "$STATE_DIR" "$LOG_DIR"

# ─── 1. Build ───────────────────────────────────────────────────────────
if [ ! -f "core/build/libs/kafka_2.13-"*-SNAPSHOT.jar 2>/dev/null ] \
   && ! ls core/build/libs/kafka_2.13-*-SNAPSHOT.jar >/dev/null 2>&1; then
  echo "[start.sh] Building project jars (first run) ..."
  ./gradlew jar --console=plain
fi

# ─── 2. Keystore ────────────────────────────────────────────────────────
if [ ! -f "$KEYSTORE" ]; then
  echo "[start.sh] Generating self-signed keystore at $KEYSTORE ..."
  keytool -genkeypair -alias rest-proxy -keyalg RSA -keysize 2048 \
          -validity 365 -keystore "$KEYSTORE" \
          -storepass changeit -keypass changeit \
          -dname "CN=localhost, O=demo, L=demo, ST=demo, C=US" \
          >/dev/null
fi

# ─── 3. Properties ──────────────────────────────────────────────────────
# Listener entries are built from the port env vars; HTTPS is only included
# when HTTPS_PORT is non-empty so you can run HTTP-only when targeting a
# Strimzi-style setup.
LISTENERS="PLAINTEXT://localhost:${KAFKA_PORT},CONTROLLER://localhost:${CONTROLLER_PORT},HTTP://0.0.0.0:${HTTP_PORT}"
if [ -n "${HTTPS_PORT}" ]; then
  LISTENERS="${LISTENERS},HTTPS://0.0.0.0:${HTTPS_PORT}"
fi

# Only write a fresh file when one doesn't exist, so user edits
# (e.g. http.rest.swagger-ui.enabled=true) survive a restart. log.dirs
# and listeners are per-run, so patch those lines in place on every run.
if [ ! -f "$PROPERTIES" ]; then
  cat > "$PROPERTIES" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:${CONTROLLER_PORT}

listeners=${LISTENERS}
advertised.listeners=PLAINTEXT://localhost:${KAFKA_PORT}
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

log.dirs=$ROOT_NATIVE/$LOG_DIR
num.partitions=3

http.rest.basic.credentials=alice:s3cret
http.rest.executor.threads=8

ssl.keystore.location=$ROOT_NATIVE/$KEYSTORE
ssl.keystore.password=changeit
ssl.key.password=changeit

offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
EOF
  echo "[start.sh] Wrote $PROPERTIES (log.dirs=$LOG_DIR)"
else
  # Patch log.dirs, listeners, advertised.listeners, and the controller
  # quorum voter so port-env-var overrides take effect on existing files.
  esc_logdir=$(printf '%s\n' "$ROOT_NATIVE/$LOG_DIR" | sed 's/[\/&]/\\&/g')
  esc_listeners=$(printf '%s\n' "$LISTENERS" | sed 's/[\/&]/\\&/g')
  if grep -q '^log.dirs=' "$PROPERTIES"; then
    sed -i.bak "s/^log.dirs=.*/log.dirs=$esc_logdir/" "$PROPERTIES" && rm -f "$PROPERTIES.bak"
  else
    echo "log.dirs=$ROOT_NATIVE/$LOG_DIR" >> "$PROPERTIES"
  fi
  sed -i.bak "s/^listeners=.*/listeners=$esc_listeners/" "$PROPERTIES" && rm -f "$PROPERTIES.bak"
  sed -i.bak "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://localhost:${KAFKA_PORT}|" "$PROPERTIES" && rm -f "$PROPERTIES.bak"
  sed -i.bak "s|^controller.quorum.voters=.*|controller.quorum.voters=1@localhost:${CONTROLLER_PORT}|" "$PROPERTIES" && rm -f "$PROPERTIES.bak"
  echo "[start.sh] Kept existing $PROPERTIES (log.dirs=$LOG_DIR, listeners updated)"
fi

# ─── 4. Format (first run only) ─────────────────────────────────────────
# An already-formatted log dir has meta.properties at its root; kafka-storage
# format refuses to touch a dir that has one. Skip the format step entirely
# when we find that marker so data from the previous run survives.
if [ -f "$LOG_DIR/meta.properties" ]; then
  echo "[start.sh] $LOG_DIR already formatted; keeping existing data."
else
  echo "[start.sh] Formatting $LOG_DIR ..."
  CLUSTER_ID=$(bash ./bin/kafka-run-class.sh kafka.tools.StorageTool random-uuid 2>/dev/null | tail -1)
  if [ -z "$CLUSTER_ID" ] || [ "${#CLUSTER_ID}" -lt 16 ]; then
    echo "[start.sh] ERROR: could not get a cluster id (got '$CLUSTER_ID')."
    echo "          Most likely the classpath filter regex isn't exported in this shell."
    echo "          Run:  export regex='(-(test|test-sources|src|scaladoc|javadoc)\\.jar|jar\\.asc|connect-file.*\\.jar)$'"
    exit 1
  fi
  echo "[start.sh] Cluster ID: $CLUSTER_ID"

  bash ./bin/kafka-run-class.sh kafka.tools.StorageTool format \
      -t "$CLUSTER_ID" -c "$PROPERTIES"
fi

# ─── 5. Start broker ────────────────────────────────────────────────────
# Launch Kafka in the background, poll the REST port until it accepts, then
# print a highly-visible READY banner. The broker's own log output is quiet
# under the tools log4j config (mostly WARN/ERROR only), so without this poll
# you can't tell from the console whether the broker is still booting, stuck,
# or ready — we've been getting bitten by exactly that.
echo ""
echo "[start.sh] Launching broker in the background ..."
KAFKA_HEAP_OPTS="-Xmx1G" bash ./bin/kafka-run-class.sh kafka.Kafka "$PROPERTIES" &
KAFKA_PID=$!

# Clean Ctrl-C handling — forward the signal to the Kafka JVM.
trap 'echo ""; echo "[start.sh] Stopping broker (pid $KAFKA_PID) ..."; kill "$KAFKA_PID" 2>/dev/null || true; wait "$KAFKA_PID" 2>/dev/null || true; exit 0' INT TERM

deadline=$(( $(date +%s) + 120 ))
while : ; do
  if ! kill -0 "$KAFKA_PID" 2>/dev/null; then
    echo ""
    echo "[start.sh] Broker JVM exited before coming up. Scroll up for the error."
    exit 1
  fi
  # Probe any URL the REST server responds to. We don't use /openapi.yaml
  # any more because it's only served when http.rest.swagger-ui.enabled=true.
  # curl -f fails on 4xx, so we drop it and just accept ANY 3-digit HTTP
  # response as proof that Jetty is bound and Jersey is dispatching —
  # the produce endpoint replies 401 (no creds) or 415 (no Content-Type),
  # both of which count as "up."
  http_code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 \
    "http://localhost:${HTTP_PORT}/v1/topics/_probe" 2>/dev/null || echo 000)
  if [ -n "${HTTPS_PORT}" ]; then
    https_code=$(curl -s -k -o /dev/null -w '%{http_code}' --max-time 1 \
      "https://localhost:${HTTPS_PORT}/v1/topics/_probe" 2>/dev/null || echo 000)
  else
    https_code="skip"
  fi
  if [ "$http_code" != "000" ] && [ "$https_code" != "000" ]; then
    break
  fi
  if [ "$(date +%s)" -gt "$deadline" ]; then
    echo ""
    echo "[start.sh] Gave up waiting for listeners after 120 s."
    kill "$KAFKA_PID" 2>/dev/null || true
    exit 1
  fi
  sleep 2
done

echo ""
echo "════════════════════════════════════════════════════════════════"
echo " BROKER READY"
echo "   HTTP  → http://localhost:${HTTP_PORT}"
if [ -n "${HTTPS_PORT}" ]; then
  echo "   HTTPS → https://localhost:${HTTPS_PORT}   (self-signed cert, use curl -k)"
fi
echo "   Kafka → localhost:${KAFKA_PORT}           (binary protocol)"
echo ""
echo " In another terminal, run:"
echo "   bash scripts/rest-proxy/test.sh"
echo ""
echo " Ctrl-C here to shut the broker down cleanly."
echo "════════════════════════════════════════════════════════════════"

# Stay attached to the JVM so Ctrl-C stops it and so any further Kafka log
# output still reaches this terminal.
wait "$KAFKA_PID"
