# Embedded HTTP REST Proxy

A thin HTTP front-end for the Kafka broker. When enabled, the broker accepts
`POST /v1/topics/{name}` requests and forwards them through the broker's
server-side produce path — the same `AuthHelper` authorization check and the
same `ReplicaManager.appendRecords` call that binary Kafka clients hit.

It is **embedded in the broker JVM**, not a sidecar. There is no separate
service to run.

## Enabling

Add one HTTP (or HTTPS) entry to the standard `listeners=` line in
`server.properties`:

```properties
listeners=PLAINTEXT://localhost:9092,HTTP://0.0.0.0:8080
```

That's it — if `listeners=` contains no `HTTP://` or `HTTPS://` entry, the
REST server code never runs.

## Configuration

| Key                              | Default | Purpose                                                               |
|----------------------------------|---------|-----------------------------------------------------------------------|
| `http.rest.executor.threads`     | `8`     | Worker threads in the Jetty thread pool                               |
| `http.rest.basic.credentials`    | (empty) | Comma-separated `user:pass` pairs for HTTP Basic Auth. Empty = no auth |

HTTPS listeners inherit the broker's existing `ssl.keystore.*` and
`ssl.truststore.*` configs — no new SSL config keys. If you've already
configured an `SSL://` Kafka listener, HTTPS works out of the box.

## Endpoint

### `POST /v1/topics/{name}`

Produce a single record to `{name}`.

**Request body**

```json
{ "key": "optional-string", "value": "optional-string" }
```

If `key` is `null`, the record goes to a randomly chosen partition. Otherwise
the partition is `Math.floorMod(hashCode(key), numPartitions)`.

**Response (200)**

```json
{ "partition": 2, "offset": 47 }
```

**Error responses**

| Status | When                                                              |
|--------|-------------------------------------------------------------------|
| 401    | Missing / malformed `Authorization` header or wrong credentials   |
| 403    | ACL denies `WRITE` on the topic (only if an authorizer is configured) |
| 404    | Topic does not exist                                              |
| 500    | Append failed — error code in the response body                   |

## Examples

### HTTP (no TLS)

```bash
# Start the broker with the listener + basic auth
cat >> server.properties <<EOF
listeners=PLAINTEXT://localhost:9092,HTTP://0.0.0.0:8080
http.rest.basic.credentials=alice:s3cret
EOF

# Create the topic via the normal admin path
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic rest-demo --partitions 3

# Produce over HTTP
curl -u alice:s3cret \
     -H 'Content-Type: application/json' \
     -d '{"key":"k1","value":"hello"}' \
     http://localhost:8080/v1/topics/rest-demo
# → {"partition":2,"offset":0}

# Verify the record landed on the topic
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic rest-demo --from-beginning
# → hello
```

### HTTPS

HTTPS reuses the broker's existing `ssl.keystore.*` / `ssl.truststore.*`
configuration — no separate SSL keys for the REST server.

#### Full-fat demo matching the design doc

This brings up a single broker with **PLAINTEXT + HTTP + HTTPS listeners
all at once**, the arrangement the design doc calls for:
`listeners=PLAINTEXT://...,HTTP://0.0.0.0:8080,HTTPS://0.0.0.0:8443`.

```bash
# 1) Generate a self-signed keystore for HTTPS
keytool -genkeypair -alias rest-proxy -keyalg RSA -keysize 2048 \
    -validity 365 -keystore /tmp/server.keystore.jks \
    -storepass changeit -keypass changeit \
    -dname "CN=localhost, O=demo, L=demo, ST=demo, C=US"

# 2) Write a server.properties with all three listeners
cat > /tmp/rest-demo.properties <<'EOF'
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093

# Kafka's standard listeners= config. HTTP:// and HTTPS:// entries are
# split out by KafkaConfig.httpListeners before the standard listener
# parser sees them.
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093,HTTP://0.0.0.0:8080,HTTPS://0.0.0.0:8443
advertised.listeners=PLAINTEXT://localhost:9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

log.dirs=/tmp/kafka-logs
num.partitions=3

http.rest.basic.credentials=alice:s3cret

# Broker-wide SSL configs — reused by HTTPS://0.0.0.0:8443
ssl.keystore.location=/tmp/server.keystore.jks
ssl.keystore.password=changeit
ssl.key.password=changeit
EOF

# 3) Format storage and start the broker
./bin/kafka-storage.sh format -t "$(./bin/kafka-storage.sh random-uuid)" \
    -c /tmp/rest-demo.properties
./bin/kafka-server-start.sh /tmp/rest-demo.properties &

# 4) Create a topic (over the binary PLAINTEXT listener)
./bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic rest-demo --partitions 3

# 5) Produce over HTTP
curl -u alice:s3cret \
     -H 'Content-Type: application/json' \
     -d '{"key":"k1","value":"from-http"}' \
     http://localhost:8080/v1/topics/rest-demo
# → {"partition":0,"offset":0}

# 6) Produce over HTTPS (-k because the cert is self-signed)
curl -k -u alice:s3cret \
     -H 'Content-Type: application/json' \
     -d '{"key":"k2","value":"from-tls"}' \
     https://localhost:8443/v1/topics/rest-demo
# → {"partition":2,"offset":0}

# 7) Verify both records reached the topic
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic rest-demo --from-beginning --timeout-ms 3000
# → from-http
# → from-tls
```

## Architecture

```
listeners=PLAINTEXT://...,HTTP://0.0.0.0:8080
                 │
                 ▼
         KafkaConfig.scala
         ┌──────┴───────┐
    listeners:      httpListeners:
    [PLAINTEXT]     [(0.0.0.0, 8080, false)]
         │              │
         ▼              ▼
    SocketServer   HttpRestServer (Jetty 12 + Jersey 3.1)
    (port 9092)    port 8080
                        │
                        ▼
                   BasicAuthFilter          → 401 on bad creds
                        │
                        ▼
                   ProduceResource          JSON in/out
                        │
                        ▼
                   AuthHelper.authorize     ← reuses broker ACL
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

## Files

| File                                 | Role                                                        |
|--------------------------------------|-------------------------------------------------------------|
| `HttpServerConfigs.java` (server/)   | Config keys + `ConfigDef`                                   |
| `KafkaConfig.scala` (core/)          | `httpListeners` extractor + accessors                       |
| `HttpRestServer.java`                | Jetty lifecycle: thread pool, connectors, SSL, start/stop   |
| `HttpSslUtils.java`                  | Builds `SslContextFactory.Server` from broker `ssl.*`       |
| `HttpRouter.java`                    | Jersey `ResourceConfig` — registers JSON, auth, resources   |
| `BasicAuthFilter.java`               | Pre-matching filter, attaches `KafkaPrincipal` on success   |
| `ProduceResource.scala`              | `@POST /v1/topics/{name}` — the B2 produce path             |
| `ProduceBody.java`                   | JSON request DTO                                            |
| `ProduceResponseBody.java`           | JSON response DTO                                           |

## Testing

```bash
./gradlew :core:test --tests "kafka.server.HttpRestProxyIntegrationTest"
```

The integration test boots a real broker in-JVM, POSTs a record over HTTP,
and reads it back with a normal `KafkaConsumer`. If it passes, every hop of
the chain is alive.

## What this does **not** do (yet)

- Batch produce (one record per request)
- Consumer endpoints
- Admin endpoints (topic CRUD, ACLs)
- Quota enforcement — HTTP produces bypass `quotaManagers.produce`
- Throttling beyond Jetty's thread pool ceiling

These are all clean follow-ups; the scaffold here is meant to be small and
reviewable.
