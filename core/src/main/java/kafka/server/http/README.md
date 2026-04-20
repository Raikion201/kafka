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
