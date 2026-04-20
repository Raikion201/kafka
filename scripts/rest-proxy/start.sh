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

log.dirs=$ROOT/$LOG_DIR
num.partitions=3

http.rest.basic.credentials=alice:s3cret
http.rest.executor.threads=8

ssl.keystore.location=$ROOT/$KEYSTORE
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
echo ""
echo "[start.sh] Starting broker. Ctrl-C to stop."
echo "[start.sh] Once you see the 'KafkaServer started' line, open another terminal and run:"
echo "            bash scripts/rest-proxy/test.sh"
echo ""
KAFKA_HEAP_OPTS="-Xmx1G" \
  exec bash ./bin/kafka-run-class.sh kafka.Kafka "$PROPERTIES"
