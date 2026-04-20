#!/usr/bin/env bash
# End-to-end smoke test of the REST proxy demo.
#   1. Waits up to 60 s for HTTP :8080 and HTTPS :8443 to answer.
#   2. Creates topic `rest-demo` (idempotent — ignores "already exists").
#   3. Produces one record over HTTP, one over HTTPS.
#   4. Runs the negative-path cases (401, 404).
#   5. Consumes the topic and prints every record.
#
# Run this while start.sh is running in another terminal.
#
# Usage:
#   bash scripts/rest-proxy/test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export regex='(-(test|test-sources|src|scaladoc|javadoc)\.jar|jar\.asc|connect-file.*\.jar)$'

TOPIC="${TOPIC:-rest-demo}"
USER="${REST_USER:-alice}"
PASS="${REST_PASS:-s3cret}"
HTTP_PORT="${HTTP_PORT:-8080}"
HTTPS_PORT="${HTTPS_PORT:-8443}"

hr() { printf -- '─%.0s' $(seq 1 60); echo; }

# ─── 1. Wait for the REST listeners ─────────────────────────────────────
echo "[1/5] Waiting for listeners to come up ..."
deadline=$(( $(date +%s) + 60 ))
while : ; do
  http_ok=0 ; https_ok=0
  curl -sf -o /dev/null --max-time 1 "http://localhost:${HTTP_PORT}/openapi.yaml"       && http_ok=1  || true
  curl -sf -k -o /dev/null --max-time 1 "https://localhost:${HTTPS_PORT}/openapi.yaml"  && https_ok=1 || true
  if [ "$http_ok" = 1 ] && [ "$https_ok" = 1 ]; then
    echo "    HTTP :${HTTP_PORT}  OK"
    echo "    HTTPS :${HTTPS_PORT} OK"
    break
  fi
  if [ "$(date +%s)" -gt "$deadline" ]; then
    echo "    Gave up after 60 s. Is the broker running?"
    echo "    Start it with:  bash scripts/rest-proxy/start.sh"
    exit 1
  fi
  sleep 2
done

# ─── 2. Create the topic ────────────────────────────────────────────────
hr ; echo "[2/5] Create topic '$TOPIC' (3 partitions) ..."
bash ./bin/kafka-run-class.sh org.apache.kafka.tools.TopicCommand \
    --bootstrap-server localhost:9092 \
    --create --topic "$TOPIC" --partitions 3 --replication-factor 1 \
  2>&1 | grep -v "already exists" || true

echo ""
echo "    Topics:"
bash ./bin/kafka-run-class.sh org.apache.kafka.tools.TopicCommand \
    --bootstrap-server localhost:9092 --list \
  | sed 's/^/      /'

# ─── 3. Produce ─────────────────────────────────────────────────────────
hr ; echo "[3/5] Produce two records ..."

echo "    HTTP :"
curl -s -u "${USER}:${PASS}" \
     -H 'Content-Type: application/json' \
     -d '{"key":"http-key","value":"from-http"}' \
     "http://localhost:${HTTP_PORT}/v1/topics/${TOPIC}"
echo ""

echo "    HTTPS:"
curl -s -k -u "${USER}:${PASS}" \
     -H 'Content-Type: application/json' \
     -d '{"key":"https-key","value":"from-tls"}' \
     "https://localhost:${HTTPS_PORT}/v1/topics/${TOPIC}"
echo ""

# ─── 4. Negative paths ──────────────────────────────────────────────────
hr ; echo "[4/5] Negative cases ..."
printf "    no auth header          "
curl -s -o /dev/null -w "HTTP %{http_code}\n" \
     "http://localhost:${HTTP_PORT}/v1/topics/${TOPIC}" -d '{}'

printf "    wrong password          "
curl -s -o /dev/null -w "HTTP %{http_code}\n" -u "${USER}:wrong" \
     "http://localhost:${HTTP_PORT}/v1/topics/${TOPIC}" -d '{}'

printf "    unknown topic           "
curl -s -o /dev/null -w "HTTP %{http_code}\n" -u "${USER}:${PASS}" \
     "http://localhost:${HTTP_PORT}/v1/topics/no-such-topic" -d '{}'

# ─── 5. Read back ───────────────────────────────────────────────────────
hr ; echo "[5/5] Consume '$TOPIC' from the beginning (5s timeout) ..."
bash ./bin/kafka-run-class.sh org.apache.kafka.tools.consumer.ConsoleConsumer \
    --bootstrap-server localhost:9092 \
    --topic "$TOPIC" --from-beginning --timeout-ms 5000 \
    --property print.partition=true \
    --property print.key=true \
    --property key.separator='|' \
  2>&1 \
  | grep -v -E '^\[|deprecated|consumer rebalance|TimeoutException|Processed' \
  | sed 's/^/    /'

hr
echo "Done. Expect:"
echo "  Partition:N|http-key|from-http"
echo "  Partition:M|https-key|from-tls"
echo "  (Different N and M because BuiltInPartitioner hashes the key.)"
