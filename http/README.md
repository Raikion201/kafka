# Embedded HTTP REST Proxy

A thin HTTP front-end for the Kafka broker. When enabled, the broker accepts
`POST /v1/topics/{name}` requests and runs them through the broker's own
server-side produce path — the same authorization check and the same
`ReplicaManager.appendRecords` call that binary Kafka clients hit.

It is **embedded in the broker JVM**, not a sidecar. There is no separate
service to run.

The proxy lives in its own Gradle module (`:http`) and is plugged into the
broker through a narrow SPI exposed by `:http-api`. The broker only depends
on the SPI at compile time; the implementation is discovered at runtime via
`java.util.ServiceLoader`. If the `:http` jar is not on the runtime
classpath, the broker boots without the REST proxy.

---

## Enabling

Add one `HTTP://` (and/or `HTTPS://`) entry to the standard `listeners=` line
in `server.properties`:

```properties
listeners=PLAINTEXT://localhost:9092,HTTP://0.0.0.0:8080,HTTPS://0.0.0.0:8443
```

That's it. If `listeners=` has no HTTP/HTTPS entry, the REST server code
never runs. The standard Kafka listener parser never sees the HTTP entries
— they're split off by `KafkaConfig.httpListeners` before it runs, so the
existing `SocketServer` path is undisturbed.

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

### Swagger UI in a browser

Open `http://localhost:8080/swagger` (or `https://localhost:8443/swagger`
and accept the self-signed cert). Click **Authorize**, enter
`alice` / `s3cret`, expand the `POST` row, fill in a topic name and body,
hit **Execute**. You get the same 200 response with partition/offset as
the `curl` version.

---

## Module layout

```
http/
├── api/                           :http-api  (Java, narrow SPI seen by core)
│   └── src/main/java/org/apache/kafka/server/http/api/
│       ├── BrokerHttpServer.java            startup() / shutdown()
│       ├── BrokerHttpServerFactory.java     ServiceLoader SPI
│       ├── BrokerHttpServerContext.java     immutable bag passed to the factory
│       ├── HttpEndpoint.java                host, port, isTls
│       ├── RecordAppender.java              narrow over ReplicaManager.appendRecords
│       └── AuthorizationHelper.java         narrow over AuthHelper.authorize
│
└── src/                           :http      (Java, Jetty/Jersey impl)
    ├── main/java/org/apache/kafka/server/http/
    │   ├── HttpRestServer.java              implements BrokerHttpServer
    │   ├── HttpRestServerFactory.java       implements BrokerHttpServerFactory
    │   ├── HttpRouter.java                  Jersey ResourceConfig builder
    │   ├── ProduceResource.java             @POST /v1/topics/{name}
    │   ├── ProduceBody.java                 JSON request DTO (record)
    │   ├── ProduceResponseBody.java         JSON response DTO (record)
    │   ├── BasicAuthFilter.java             @PreMatching filter
    │   ├── OpenApiResource.java             /openapi.yaml + /swagger
    │   └── KafkaSslContextFactory.java      Jetty ↔ SslFactory bridge
    └── main/resources/
        ├── META-INF/services/org.apache.kafka.server.http.api.BrokerHttpServerFactory
        ├── org/apache/kafka/server/http/openapi.yaml
        └── org/apache/kafka/server/http/swagger-ui.html
```

**Why the split?** `:http-api` is small, depends only on clients +
server-common + metadata, and contains zero Jetty/Jersey. It's what `core`
compiles against. `:http` is where the web framework lives and is loaded at
runtime via ServiceLoader — swap it out, replace it, or strip it from a
distribution without recompiling the broker.

## Architecture

```
listeners=PLAINTEXT://...,HTTP://...,HTTPS://...
                 │
                 ▼
         KafkaConfig.scala                         ┌─────────────────────────┐
         ┌──────┴───────┐                          │     :core (Scala)       │
    listeners:      httpListeners:                 └─────────────────────────┘
    [PLAINTEXT]     [HttpEndpoint(HTTP,  8080),
         │           HttpEndpoint(HTTPS, 8443)]
         ▼              │
    SocketServer        ▼
    (port 9092)    BrokerHttpServers.load
                        │  (ServiceLoader)
                        ▼
                   BrokerHttpServerFactory ◄─────── META-INF/services
                        │
                        ▼
                   BrokerHttpServer                ┌─────────────────────────┐
                        ↑                          │       :http (Java)      │
     ┌──────────────────┤ TLS only                 └─────────────────────────┘
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
                   AuthorizationHelper      ← SPI adapter over AuthHelper
                        │
                        ▼
                   MetadataCache            ← reuses broker metadata
                        │
                        ▼
                   RecordAppender           ← SPI adapter over ReplicaManager
                        │
                        ▼
                   HTTP 200 { partition, offset }
```

The `RecordAppender` / `AuthorizationHelper` / `ReconfigurableRegistry`
adapters are built inline in `BrokerServer.scala` — one lambda / SAM each,
forwarding straight to `ReplicaManager.appendRecords`, `AuthHelper.authorize`,
and `config.{add,remove}Reconfigurable`. The ServiceLoader lookup itself
lives in `:http-api` (`BrokerHttpServers.load`), so `:core` never does
any HTTP-specific wiring beyond constructing those three adapters.
Nothing in `:http` imports any Scala/core type.

## Wiring files (core)

| File                                                         | Role                                                                  |
|--------------------------------------------------------------|-----------------------------------------------------------------------|
| `server/.../config/HttpServerConfigs.java`                   | Config keys + `ConfigDef` (merged into `AbstractKafkaConfig.CONFIG_DEF`) |
| `core/.../server/KafkaConfig.scala`                          | `httpListeners` / `httpExecutorThreads` / `httpBasicCredentials` accessors and the listener-split override |
| `core/.../server/BrokerServer.scala`                         | Lifecycle — builds adapters inline, calls `BrokerHttpServers.load`, starts/stops the server |
| `scripts/rest-proxy/{start,test,stop}.sh`                    | One-command bootstrap + smoke test                                    |

## Testing

Run the REST-proxy test suite:

```bash
./gradlew :http:test :http:http-api:test :core:test \
  --tests "kafka.server.HttpRestProxyIntegrationTest" \
  --tests "kafka.server.HttpsRestProxyIntegrationTest" \
  --tests "org.apache.kafka.server.http.BasicAuthFilterTest" \
  --tests "org.apache.kafka.server.http.HttpConfigTest" \
  --tests "org.apache.kafka.server.http.KafkaSslContextFactoryTest"
```

### Coverage

| Test class                           | Module    | Cases | What it proves                                                              |
|--------------------------------------|-----------|------:|-----------------------------------------------------------------------------|
| `HttpRestProxyIntegrationTest`       | core      |     4 | End-to-end over plain HTTP: produce → consume back, 401 / 401 / 404         |
| `HttpsRestProxyIntegrationTest`      | core      |     2 | HTTPS handshake + produce + consume back; per-listener SSL override served  |
| `BasicAuthFilterTest`                | :http     |    10 | Every auth outcome: missing, non-Basic, malformed base64, no colon, unknown user, wrong password, success, password with colon, `/swagger` and `/openapi.yaml` allow-list |
| `HttpConfigTest`                     | :http     |    10 | `listeners=` splitting, IPv6 bracket form, `PASSWORD`-typed credentials, malformed entries, default thread count, `atLeast(4)` floor |
| `KafkaSslContextFactoryTest`         | :http     |     5 | The bridge in isolation: `newSSLEngine` delegates to `SslFactory`, fresh engines per call, reconfigurable configs exposed, `reconfigure` flows through |
| **Total**                            |           |  **31** |                                                                             |

Integration tests stay in `:core` because they extend `IntegrationTestHarness`
(a core-test-only fixture) and drive the whole broker. They depend on
`:http` via `testImplementation`, so the impl jar is on the broker's runtime
classpath when the test runs.
