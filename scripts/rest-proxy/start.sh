#!/usr/bin/env bash
# Start a single-node Kafka broker with the REST proxy demo listeners:
#   PLAINTEXT://localhost:9092   (binary Kafka protocol)
#   CONTROLLER://localhost:9093  (KRaft controller)
#   HTTP://0.0.0.0:8080          (REST proxy, plain)
#   HTTPS://0.0.0.0:8443         (REST proxy, TLS via broker's ssl.keystore.*)
#
# Does the setup once, idempotently:
#   1. Ensures the project jars are built.
#   2. Generates a self-signed keystore at tmp/server.keystore.jks if missing.
#   3. Writes tmp/rest-proxy-demo.properties if missing.
#   4. Creates a fresh log directory on each run (avoids Windows file-lock
#      recovery crashes on stale segments from a previous kill).
#   5. Formats that log directory.
#   6. Launches the broker in the foreground (Ctrl-C to stop cleanly).
#
# Usage:
#   bash scripts/rest-proxy/start.sh
#
# Run from the repo root.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

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
# Fresh log dir per run so a previous crash can't corrupt this one. Unix
# seconds resolution is enough; if you start twice in the same second you
# just get two identically-named dirs and the second run formats the first.
LOG_DIR="$STATE_DIR/kafka-logs-$(date +%s)"

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
cat > "$PROPERTIES" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093

listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093,HTTP://0.0.0.0:8080,HTTPS://0.0.0.0:8443
advertised.listeners=PLAINTEXT://localhost:9092
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

# ─── 4. Format ──────────────────────────────────────────────────────────
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
  if curl -sf -o /dev/null --max-time 1 "http://localhost:8080/openapi.yaml" 2>/dev/null \
     && curl -sf -k -o /dev/null --max-time 1 "https://localhost:8443/openapi.yaml" 2>/dev/null; then
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
echo "   HTTP  → http://localhost:8080"
echo "   HTTPS → https://localhost:8443   (self-signed cert, use curl -k)"
echo "   Kafka → localhost:9092           (binary protocol)"
echo ""
echo " In another terminal, run:"
echo "   bash scripts/rest-proxy/test.sh"
echo ""
echo " Ctrl-C here to shut the broker down cleanly."
echo "════════════════════════════════════════════════════════════════"

# Stay attached to the JVM so Ctrl-C stops it and so any further Kafka log
# output still reaches this terminal.
wait "$KAFKA_PID"
