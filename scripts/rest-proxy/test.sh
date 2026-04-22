#!/usr/bin/env bash
# End-to-end smoke test of the REST proxy demo. Works against either the
# standalone start.sh broker OR a Strimzi deployment (both default to 9090).
#   1. Waits up to 60 s for the HTTP (and optional HTTPS) listener to answer.
#   2. Creates topic `rest-demo` (idempotent — ignores "already exists").
#   3. Produces one record over HTTP (+ HTTPS when enabled).
#   4. Runs the negative-path cases (401, 404).
#   5. Consumes the topic and prints every record.
#
# Run this while start.sh is running in another terminal, or against a
# live Strimzi cluster whose REST proxy is exposed on HTTP_PORT.
#
# Overridable env vars (and their defaults):
#   HTTP_PORT=9090                    # REST proxy plain listener port
#   HTTPS_PORT=8443                   # set HTTPS_PORT= (empty) to skip HTTPS
#   KAFKA_BOOTSTRAP=localhost:9092    # used by create/consume via kafka tools
#   KUBECTL_POD=                      # if set, run kafka CLI via `kubectl exec`
#   KUBECTL_NS=kafka                  # namespace for KUBECTL_POD
#   TOPIC=rest-demo  REST_USER=alice  REST_PASS=s3cret
#
# Usage:
#   bash scripts/rest-proxy/test.sh                       # local broker or port-forward
#   KUBECTL_POD=my-cluster-broker-0 bash ... /test.sh     # Strimzi without port-forward
#   HTTPS_PORT= bash scripts/rest-proxy/test.sh           # HTTP only
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export regex='(-(test|test-sources|src|scaladoc|javadoc)\.jar|jar\.asc|connect-file.*\.jar)$'

TOPIC="${TOPIC:-rest-demo}"
USER="${REST_USER:-alice}"
PASS="${REST_PASS:-s3cret}"
HTTP_PORT="${HTTP_PORT:-9095}"
HTTPS_PORT="${HTTPS_PORT-8443}"
KUBECTL_POD="${KUBECTL_POD:-}"
KUBECTL_NS="${KUBECTL_NS:-kafka}"

# If routing through a k8s pod, the bootstrap is always localhost from inside
# the broker; otherwise use whatever the caller set (or default to localhost).
if [ -n "$KUBECTL_POD" ]; then
  KAFKA_BOOTSTRAP="localhost:9092"
else
  KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
fi

# kafka_topics / kafka_consumer — wrappers that run either the locally-built
# Gradle classes or the broker pod's shipped /opt/kafka/bin/ scripts.
if [ -n "$KUBECTL_POD" ]; then
  # MSYS_NO_PATHCONV=1 stops Git Bash from rewriting /opt/... into a Windows path.
  kafka_topics()   { MSYS_NO_PATHCONV=1 kubectl exec -n "$KUBECTL_NS" "$KUBECTL_POD" -- /opt/kafka/bin/kafka-topics.sh "$@"; }
  kafka_consumer() { MSYS_NO_PATHCONV=1 kubectl exec -n "$KUBECTL_NS" "$KUBECTL_POD" -- /opt/kafka/bin/kafka-console-consumer.sh "$@"; }
else
  kafka_topics()   { bash ./bin/kafka-run-class.sh org.apache.kafka.tools.TopicCommand "$@"; }
  kafka_consumer() { bash ./bin/kafka-run-class.sh org.apache.kafka.tools.consumer.ConsoleConsumer "$@"; }
fi

hr() { printf -- '─%.0s' $(seq 1 60); echo; }

# ─── 1. Wait for the REST listeners ─────────────────────────────────────
# /openapi.yaml is only served when http.rest.swagger-ui.enabled=true, so we
# probe /v1/topics/_probe instead — it answers 401/415 once Jetty is up.
# HTTPS is best-effort: we wait for HTTP first, then give HTTPS a short
# grace window. If it never responds (e.g. Strimzi exposes HTTP only), we
# clear HTTPS_PORT and downstream steps skip HTTPS automatically.
echo "[1/5] Waiting for listeners to come up ..."
deadline=$(( $(date +%s) + 60 ))
while : ; do
  # `|| true` keeps curl's exit status from tripping `set -e`; -w '%{http_code}'
  # already prints "000" on connection failure, so no replacement echo needed.
  http_code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 \
    "http://localhost:${HTTP_PORT}/v1/topics/_probe" 2>/dev/null || true)
  if [ -n "$http_code" ] && [ "$http_code" != "000" ]; then
    echo "    HTTP :${HTTP_PORT}  OK"
    break
  fi
  if [ "$(date +%s)" -gt "$deadline" ]; then
    echo "    Gave up after 60 s. Is the broker running?"
    echo "    Start it with:  bash scripts/rest-proxy/start.sh"
    exit 1
  fi
  sleep 2
done

if [ -n "${HTTPS_PORT}" ]; then
  https_deadline=$(( $(date +%s) + 10 ))
  while : ; do
    https_code=$(curl -s -k -o /dev/null -w '%{http_code}' --max-time 1 \
      "https://localhost:${HTTPS_PORT}/v1/topics/_probe" 2>/dev/null || true)
    if [ -n "$https_code" ] && [ "$https_code" != "000" ]; then
      echo "    HTTPS :${HTTPS_PORT} OK"
      break
    fi
    if [ "$(date +%s)" -gt "$https_deadline" ]; then
      echo "    HTTPS :${HTTPS_PORT} not reachable — skipping HTTPS steps"
      HTTPS_PORT=""
      break
    fi
    sleep 1
  done
fi

# ─── 2. Create the topic ────────────────────────────────────────────────
if [ -n "$KUBECTL_POD" ]; then
  hr ; echo "[2/5] Create topic '$TOPIC' (3 partitions) via kubectl exec ${KUBECTL_NS}/${KUBECTL_POD} ..."
else
  hr ; echo "[2/5] Create topic '$TOPIC' (3 partitions) on ${KAFKA_BOOTSTRAP} ..."
fi
kafka_topics \
    --bootstrap-server "${KAFKA_BOOTSTRAP}" \
    --create --topic "$TOPIC" --partitions 3 --replication-factor 1 \
  2>&1 | grep -v "already exists" || true

echo ""
echo "    Topics:"
kafka_topics --bootstrap-server "${KAFKA_BOOTSTRAP}" --list \
  | sed 's/^/      /'

# ─── 3. Produce ─────────────────────────────────────────────────────────
hr ; echo "[3/5] Produce records ..."

echo "    HTTP :"
curl -s -u "${USER}:${PASS}" \
     -H 'Content-Type: application/json' \
     -d '{"key":"http-key","value":"from-http"}' \
     "http://localhost:${HTTP_PORT}/v1/topics/${TOPIC}"
echo ""

if [ -n "${HTTPS_PORT}" ]; then
  echo "    HTTPS:"
  curl -s -k -u "${USER}:${PASS}" \
       -H 'Content-Type: application/json' \
       -d '{"key":"https-key","value":"from-tls"}' \
       "https://localhost:${HTTPS_PORT}/v1/topics/${TOPIC}"
  echo ""
else
  echo "    HTTPS: skipped (HTTPS_PORT unset)"
fi

# ─── 4. Negative paths ──────────────────────────────────────────────────
# Every request sends application/json so we get the REAL status from our
# handler, not a 415 from Jersey's content-type check running earlier.
hr ; echo "[4/5] Negative cases ..."
printf "    no auth header          "
curl -s -o /dev/null -w "HTTP %{http_code}\n" \
     -H 'Content-Type: application/json' \
     "http://localhost:${HTTP_PORT}/v1/topics/${TOPIC}" -d '{}'

printf "    wrong password          "
curl -s -o /dev/null -w "HTTP %{http_code}\n" -u "${USER}:wrong" \
     -H 'Content-Type: application/json' \
     "http://localhost:${HTTP_PORT}/v1/topics/${TOPIC}" -d '{}'

printf "    unknown topic           "
curl -s -o /dev/null -w "HTTP %{http_code}\n" -u "${USER}:${PASS}" \
     -H 'Content-Type: application/json' \
     "http://localhost:${HTTP_PORT}/v1/topics/no-such-topic" -d '{}'

# ─── 5. Read back ───────────────────────────────────────────────────────
hr ; echo "[5/5] Consume '$TOPIC' from the beginning (5s timeout) ..."
kafka_consumer \
    --bootstrap-server "${KAFKA_BOOTSTRAP}" \
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
if [ -n "${HTTPS_PORT}" ]; then
  echo "  Partition:M|https-key|from-tls"
  echo "  (Different N and M because BuiltInPartitioner hashes the key.)"
fi
