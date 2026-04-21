# Embedded HTTP REST Proxy

A thin HTTP front-end for the Kafka broker. When enabled, the broker accepts
`POST /v1/topics/{name}` requests and runs them through the broker's own
server-side produce path — the same `AuthHelper` authorization check and the
same `ReplicaManager.appendRecords` call that binary Kafka clients hit.

It is **embedded in the broker JVM**, not a sidecar. There is no separate
service to run.

---

## Enabling

Add one `HTTP://` (and/or `HTTPS://`) entry to the standard `listeners=` line
in `server.properties`:

```properties
listeners=PLAINTEXT://localhost:9092,HTTP://0.0.0.0:8080,HTTPS://0.0.0.0:8443
```

That's it. If `listeners=` has no HTTP/HTTPS entry, the REST server code never
runs. The standard Kafka listener parser never sees the HTTP entries — they're
split off by `KafkaConfig.httpListeners` before it runs, so the existing
`SocketServer` path is undisturbed.

## Configuration keys

| Key                              | Default | Type       | Purpose                                                               |
|----------------------------------|---------|------------|-----------------------------------------------------------------------|
| `http.rest.executor.threads`     | `8`     | INT (≥ 4)  | Worker threads in the Jetty thread pool                               |
| `http.rest.basic.credentials`    | (empty) | PASSWORD   | Comma-separated `user:pass` pairs for HTTP Basic Auth. Empty ⇒ no auth |

`PASSWORD`-typed values are redacted in broker logs and in
`kafka-configs.sh --describe`.

### SSL for HTTPS

HTTPS reuses **every** SSL knob the broker already has — no new keys. The
listener goes through Kafka's `SslFactory` pipeline (same one `SSL://` Kafka
listeners use), which means:

- `ssl.keystore.*`, `ssl.truststore.*`, `ssl.cipher.suites`, etc. all apply
- `ssl.engine.factory.class` plugin is honoured — your HSM-backed or
  audit-wrapped `SslEngineFactory` works on HTTPS too
- **Per-listener overrides** — `listener.name.https.ssl.keystore.location=/path/to/rest.jks`
  wins over the global `ssl.keystore.location`
- **Live cert rotation** — `kafka-configs.sh --alter --entity-type brokers
  --add-config listener.name.https.ssl.keystore.location=...` swaps the cert
  without a restart, validated by `CertificateEntries.ensureCompatible` +
  `SslEngineValidator.validate` before it takes effect

---

## Endpoint

```
POST   /v1/topics/{name}           Produce a single record
GET    /openapi.yaml               OpenAPI 3 spec
GET    /swagger                    Swagger UI (for in-browser testing)
```

### `POST /v1/topics/{name}`

Produce a single record to `{name}`. Topic must already exist.

**Request body**

```json
{ "key": "optional-string", "value": "optional-string" }
```

- `key` null ⇒ partition chosen randomly
- `key` set ⇒ partition = `BuiltInPartitioner.partitionForKey(keyBytes, numPartitions)`
  (murmur2 — same as Kafka's default producer partitioner, so a record
  produced via REST with key `k` lands on the same partition as the same key
  produced via `KafkaProducer`)

**Success (200)**

```json
{ "partition": 2, "offset": 47 }
```

**Error mapping**

| Status | When                                                                 |
|-------:|----------------------------------------------------------------------|
| 400    | `INVALID_TOPIC_EXCEPTION`, `INVALID_REQUIRED_ACKS`, `CORRUPT_MESSAGE` |
| 401    | Missing / malformed `Authorization` header, or wrong credentials     |
| 403    | `TOPIC_AUTHORIZATION_FAILED`, `CLUSTER_AUTHORIZATION_FAILED`, `DELEGATION_TOKEN_AUTHORIZATION_FAILED` |
| 404    | Topic doesn't exist, `UNKNOWN_TOPIC_OR_PARTITION`, `UNKNOWN_TOPIC_ID` |
| 413    | `MESSAGE_TOO_LARGE`                                                  |
| 500    | Fallback for any other Kafka `Errors.*`                              |
| 503    | `NOT_LEADER_OR_FOLLOWER`, `NOT_ENOUGH_REPLICAS(_AFTER_APPEND)`, `KAFKA_STORAGE_ERROR`, topic metadata gap |
| 504    | `REQUEST_TIMED_OUT` (broker-side append timeout, or client waited > 35 s) |

---

## Quickest path from nothing to a running demo

Three helper scripts in `scripts/rest-proxy/` automate the whole bootstrap.

**Terminal A — start the broker:**

```bash
bash scripts/rest-proxy/start.sh
```

The script is idempotent: it builds jars if needed, generates a self-signed
keystore on first run, writes `tmp/rest-proxy-demo.properties`, formats a
fresh log dir, launches Kafka, polls until both listeners accept, and prints
a big `BROKER READY` banner with the URLs. Ctrl-C for clean shutdown.

**Terminal B — smoke-test it:**

```bash
bash scripts/rest-proxy/test.sh
```

Waits for the listeners, creates `rest-demo`, produces one record over HTTP
and one over HTTPS, exercises 401 / 401 / 404 negative paths, and consumes
the topic back.

**If you ever lose the start.sh terminal:**

```bash
bash scripts/rest-proxy/stop.sh
```

Force-kills any lingering Kafka JVM.

### What the scripts are doing under the hood

If you want to understand each step (or run on a machine where the scripts
don't work — e.g. pure Linux without `cygpath`), here's the manual flow:

```bash
# 1. Keystore (one-time)
keytool -genkeypair -alias rest-proxy -keyalg RSA -keysize 2048 \
    -validity 365 -keystore tmp/server.keystore.jks \
    -storepass changeit -keypass changeit \
    -dname "CN=localhost, O=demo, L=demo, ST=demo, C=US"

# 2. server.properties with all three listeners
cat > tmp/rest-demo.properties <<'EOF'
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093

listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093,HTTP://0.0.0.0:8080,HTTPS://0.0.0.0:8443
advertised.listeners=PLAINTEXT://localhost:9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

log.dirs=tmp/kafka-logs
num.partitions=3

http.rest.basic.credentials=alice:s3cret

ssl.keystore.location=tmp/server.keystore.jks
ssl.keystore.password=changeit
ssl.key.password=changeit
EOF

# 3. Format + start
./bin/kafka-storage.sh format -t "$(./bin/kafka-storage.sh random-uuid)" \
    -c tmp/rest-demo.properties
./bin/kafka-server-start.sh tmp/rest-demo.properties &

# 4. Create topic (binary protocol)
./bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic rest-demo --partitions 3 --replication-factor 1

# 5. Produce via HTTP
curl -u alice:s3cret -H 'Content-Type: application/json' \
     -d '{"key":"k1","value":"from-http"}' \
     http://localhost:8080/v1/topics/rest-demo
# → {"partition":0,"offset":0}

# 6. Produce via HTTPS (-k because the cert is self-signed)
curl -k -u alice:s3cret -H 'Content-Type: application/json' \
     -d '{"key":"k2","value":"from-tls"}' \
     https://localhost:8443/v1/topics/rest-demo
# → {"partition":2,"offset":0}

# 7. Read both back
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic rest-demo --from-beginning --timeout-ms 3000 \
    --property print.key=true --property key.separator='|'
# → k1|from-http
# → k2|from-tls
```

### Swagger UI in a browser

Open `http://localhost:8080/swagger` (or `https://localhost:8443/swagger`
and accept the self-signed cert). Click **Authorize**, enter
`alice` / `s3cret`, expand the `POST` row, fill in a topic name and body,
hit **Execute**. You get the same 200 response with partition/offset as
the `curl` version.

---

## Architecture

```
listeners=PLAINTEXT://...,HTTP://...,HTTPS://...
                 │
                 ▼
         KafkaConfig.scala
         ┌──────┴───────┐
    listeners:      httpListeners:
    [PLAINTEXT]     [HttpEndpoint(HTTP,  8080, isTls=false),
         │           HttpEndpoint(HTTPS, 8443, isTls=true )]
         ▼              │
    SocketServer        ▼
    (port 9092)    HttpRestServer (Jetty 12 + Jersey 3.1)
                        │
     ┌──────────────────┤ TLS only
     ▼                  │
  KafkaSslContextFactory│   ← extends Jetty's SslContextFactory.Server,
     │                  │     overrides newSSLEngine to delegate to …
     ▼                  │
  SslFactory            │   ← Kafka's own SSL pipeline:
  (reused from core)    │     • DefaultSslEngineFactory (pluggable via
                        │       ssl.engine.factory.class)
                        │     • listener.name.<name>.ssl.* overrides
                        │     • reconfigurable via DynamicBrokerConfig
                        │
                        ▼
                   BasicAuthFilter          → 401 on bad / missing creds
                        │
                        ▼
                   ProduceResource          JSON in/out, @Suspended async
                        │
                        ▼
                   AuthHelper.authorize     ← reuses broker ACL path
                        │
                        ▼
                   MetadataCache            ← reuses broker metadata
                        │
                        ▼
                   ReplicaManager.appendRecords  ← reuses broker append path
                        │
                        ▼
                   HTTP 200 { partition, offset }
```

The only genuinely new surface is HTTP routing and JSON. Everything below
`ProduceResource` is the broker's existing plumbing.

## Files

| File                                                         | Role                                                                  |
|--------------------------------------------------------------|-----------------------------------------------------------------------|
| `server/.../config/HttpServerConfigs.java`                   | Config keys + `ConfigDef` (merged into `AbstractKafkaConfig.CONFIG_DEF`) |
| `core/.../server/KafkaConfig.scala`                          | `httpListeners` / `httpExecutorThreads` / `httpBasicCredentials` accessors and the listener-split override |
| `core/.../server/BrokerServer.scala`                         | Lifecycle — constructs and shuts down `HttpRestServer`                |
| `core/.../server/http/HttpRestServer.scala`                  | Jetty server lifecycle: thread pool, connectors, per-HTTPS-listener `SslFactory` registration |
| `core/.../server/http/KafkaSslContextFactory.scala`          | Jetty ↔ Kafka SSL bridge; implements `ListenerReconfigurable` for live cert rotation |
| `core/.../server/http/HttpRouter.scala`                      | Jersey `ResourceConfig` factory — the canonical routes table lives in its Scaladoc |
| `core/.../server/http/BasicAuthFilter.scala`                 | `@PreMatching` filter; constant-time compare; allow-lists `/swagger` and `/openapi.yaml` |
| `core/.../server/http/ProduceResource.scala`                 | `@POST /v1/topics/{name}` — auth + partition + append + error mapping; `@Suspended` async so it doesn't pin a Jetty worker during the append |
| `core/.../server/http/OpenApiResource.scala`                 | Serves `openapi.yaml` and `swagger-ui.html` from classpath             |
| `core/.../server/http/ProduceBody.java`                      | JSON request DTO (Java record)                                        |
| `core/.../server/http/ProduceResponseBody.java`              | JSON response DTO (Java record)                                       |
| `core/.../resources/kafka/server/http/openapi.yaml`          | Hand-written OpenAPI 3 spec                                           |
| `core/.../resources/kafka/server/http/swagger-ui.html`       | Loads Swagger UI from a CDN, points it at `/openapi.yaml`             |
| `scripts/rest-proxy/{start,test,stop}.sh`                    | One-command bootstrap + smoke test                                    |

## Testing

Run the REST-proxy test suite:

```bash
./gradlew :core:test \
  --tests "kafka.server.HttpRestProxyIntegrationTest" \
  --tests "kafka.server.HttpsRestProxyIntegrationTest" \
  --tests "kafka.server.http.BasicAuthFilterTest" \
  --tests "kafka.server.http.HttpConfigTest" \
  --tests "kafka.server.http.KafkaSslContextFactoryTest"
```

### Coverage

| Test class                           | Cases | What it proves                                                              |
|--------------------------------------|------:|-----------------------------------------------------------------------------|
| `HttpRestProxyIntegrationTest`       |     4 | End-to-end over plain HTTP: produce → consume back, 401 / 401 / 404         |
| `HttpsRestProxyIntegrationTest`      |     2 | HTTPS handshake + produce + consume back; per-listener SSL override served  |
| `BasicAuthFilterTest`                |    10 | Every auth outcome: missing, non-Basic, malformed base64, no colon, unknown user, wrong password, success, password with colon, `/swagger` and `/openapi.yaml` allow-list |
| `HttpConfigTest`                     |    10 | `listeners=` splitting, IPv6 bracket form, `PASSWORD`-typed credentials, malformed entries, default thread count, `atLeast(4)` floor |
| `KafkaSslContextFactoryTest`         |     5 | The bridge in isolation: `newSSLEngine` delegates to `SslFactory`, fresh engines per call, reconfigurable configs exposed, `reconfigure` flows through |
| **Total**                            |  **31** |                                                                             |

