# connect-manifest-codegen

Generates Kafka Connect source connector classes from
[Airbyte declarative manifests](https://docs.airbyte.com/connector-development/config-based/understanding-the-yaml-file).

Drop a `manifest.yaml` file into `src/main/manifests/` and the build produces three
ready-to-deploy Java files per manifest — no hand-written connector code required.

---

## Overview

Airbyte's declarative connector format describes how to call an HTTP API in YAML.
This module reads those YAML files and uses [JavaPoet](https://github.com/square/javapoet)
to emit idiomatic Kafka Connect source connectors.

```
manifest.yaml
     │
     ▼
  ManifestParser          (YAML → ManifestSpec object tree)
     │
     ├─► ConfigGenerator  → XxxConnectorConfig.java
     ├─► ConnectorGenerator → XxxSourceConnector.java
     └─► TaskGenerator    → XxxSourceTask.java
```

The three generated classes form a self-contained connector plugin that can be placed on
the Kafka Connect plugin path without any runtime dependency on this module.

---

## Generated classes

| Class | Role |
|---|---|
| `XxxConnectorConfig` | Extends `AbstractConfig`; one typed getter per `spec.connection_specification` property |
| `XxxSourceConnector` | Extends `SourceConnector`; delegates task config to `XxxConnectorConfig` |
| `XxxSourceTask` | Extends `SourceTask`; implements `poll()` — HTTP fetch, auth, pagination, record extraction |

The connector class prefix `Xxx` is derived from the manifest filename
(e.g. `gmail.yaml` → `Gmail`).

---

## Module structure

```
connect-manifest-codegen/
├── src/main/java/…/codegen/
│   ├── ManifestCodegenCli.java          CLI entry point
│   ├── parser/
│   │   └── ManifestParser.java          Jackson YAML → ManifestSpec
│   ├── model/
│   │   ├── ManifestSpec.java            Top-level spec; holds streams + spec block
│   │   ├── StreamSpec.java              Single stream definition
│   │   ├── RetrieverSpec.java           SimpleRetriever (requester + paginator + selector)
│   │   ├── RequesterSpec.java           HttpRequester (url_base, path, method, auth, params)
│   │   ├── AuthenticatorSpec.java       Auth block; all supported types
│   │   ├── PaginatorSpec.java           Paginator block; strategy + token/size options
│   │   └── RecordSelectorSpec.java      DpathExtractor field_path
│   └── generator/
│       ├── ConfigGenerator.java         Generates XxxConnectorConfig
│       ├── ConnectorGenerator.java      Generates XxxSourceConnector
│       └── TaskGenerator.java           Generates XxxSourceTask (the core generator)
├── src/main/manifests/                  Place your *.yaml files here for auto-generation
└── src/test/resources/manifests/        Test manifests covering all supported features
```

---

## Design approach

### Parse phase

`ManifestParser` uses Jackson's YAML module to deserialize the manifest into a typed Java
object tree (`ManifestSpec` and friends). All model classes are annotated with
`@JsonIgnoreProperties(ignoreUnknown = true)` so unknown fields are silently skipped.

The parser resolves `$ref` anchors defined in a top-level `definitions` block, so streams
that reference shared components are expanded before the generator sees them.

### Code generation phase

All three generators follow the same pattern: they receive a `ManifestSpec` and a target
package name and return a `JavaFile` (JavaPoet) that is written to disk.

`TaskGenerator` is the most complex. Its `generate()` method drives a builder pipeline:

```
addClassFields()           → static HttpClient, ObjectMapper, volatile auth fields
buildStart()               → parse config, prime auth caches (OAuth, SessionToken)
buildStop()                → set running = false
buildVersion()             → returns APP_VERSION constant
buildPoll()                → calls pollStreamN() for each stream, aggregates records
  └─ pollStreamN()
       ├─ buildPaginationInit()   → declare cursor/page/offset variables
       ├─ buildUrlBlock()         → build URL string or use cursor as full URL
       ├─ buildRequestStatement() → HttpRequest with correct auth header
       ├─ buildFetchBlock()       → send + retry on 429/5xx (exponential backoff, 3 attempts)
       ├─ buildRecordLoop()       → navigate field_path, emit SourceRecord per element
       └─ buildPaginationStateUpdate() → advance cursor/page/offset or detect stop
addAuthHelperMethods()     → refreshAccessToken(), loginAndCacheSessionToken(), buildJwt()
```

### Config template resolution

Airbyte manifests use Jinja2 expressions to reference config values:
`{{ config['api_key'] }}` or `{{ config["api_key"] }}`. The generator matches these with
the regex `\{\{\s*config\[['"]([^'"]+)['"]\]\s*\}\}` and replaces them with calls to the
corresponding typed getter on `XxxConnectorConfig` (e.g. `config.getApiKey()`).

A second pattern `json_loads(config['key'])['subkey']` is used in some manifests (notably
JWT auth) to extract a nested value from a JSON string config field.

---

## Supported authentication types

| Manifest type | Generated behaviour |
|---|---|
| `NoAuth` | No `Authorization` header |
| `BearerAuthenticator` | `Authorization: Bearer <token>` from config |
| `ApiKeyAuthenticator` | Injects into header or query param (`inject_into`) |
| `BasicHttpAuthenticator` | `Authorization: Basic <base64>`, credentials cached in `start()` |
| `OAuthAuthenticator` (refresh_token) | POST to `token_refresh_endpoint` in `start()`, token cached with expiry, auto-refreshed on 401 |
| `OAuthAuthenticator` (client_credentials) | Same as above but POST body uses `grant_type=client_credentials`; `refresh_request_body` fields appended |
| `SessionTokenAuthenticator` | `login_requester` POST (optionally with BasicHttp inner auth) called in `start()`; token extracted via `session_token_path` and cached as `volatile String` |
| `JwtAuthenticator` | RS256 JWT built fresh per request using `java.security.Signature`; PEM key decoded from config (`json_loads` or nested config map pattern); claims from `jwt_payload` + `additional_jwt_payload` |

---

## Supported pagination types

| Manifest strategy | Generated behaviour |
|---|---|
| `NoPagination` (or absent) | Single HTTP request per stream per poll |
| `CursorPagination` (query param) | `do { … } while (nextCursor != null)` loop; cursor appended to URL as query param |
| `CursorPagination` (RequestPath) | Cursor is the full next URL (`page_token_option.type: RequestPath`); `url = nextCursor != null ? nextCursor : baseUrl + path` |
| `PageIncrement` | Page counter loop; `?page=N&per_page=M` appended each iteration |
| `OffsetIncrement` | Offset counter loop; `?offset=N&limit=M` appended each iteration |

All paginated loops have a built-in stop condition: an empty response page terminates the
loop to prevent infinite polling against APIs that return 200 with empty data at the end.

---

## HTTP transport

The generated task uses `java.net.http.HttpClient` (Java 11+) directly — no additional HTTP
library dependency is needed at runtime.

**Retry logic**: on HTTP 429 or 5xx the task retries up to 3 times with exponential backoff
(1 s, 2 s, 4 s). Any other non-2xx response throws a `ConnectException`.

---

## Kafka record format

Each JSON element extracted from the response is published as a `SourceRecord` with:

- **topic** — stream name (lowercased)
- **value** — raw JSON string of the element
- **value schema** — `Schema.STRING_SCHEMA`
- **source partition / offset** — `{}` / `{}` (stateless; re-reads from the beginning each poll)

---

## Build integration

The Gradle `generateConnectors` task runs the CLI automatically before `compileJava`:

```bash
# Generate connector sources from src/main/manifests/*.yaml
./gradlew :connect-manifest-codegen:generateConnectors

# Full build: generate → compile → test → package jar
./gradlew :connect-manifest-codegen:build
```

Generated sources land in `build/generated-sources/connectors/` and are compiled into the
module jar. The generated code is excluded from Checkstyle and SpotBugs.

### CLI usage

```bash
java -jar connect-manifest-codegen.jar <manifest.yaml|manifest-dir> <output-dir> [package]
```

| Argument | Description |
|---|---|
| `manifest.yaml` or dir | Single manifest file or directory of `*.yaml` files |
| `output-dir` | Directory where `.java` files are written |
| `package` | (optional) Java package; defaults to `org.apache.kafka.connect.manifest.generated` |

Exit codes: `0` success · `1` bad args · `2` parse error · `3` codegen error · `4` I/O error

---

## Adding a new manifest

1. Copy the Airbyte manifest YAML to `src/main/manifests/your_connector.yaml`.
2. Run `./gradlew :connect-manifest-codegen:generateConnectors`.
3. Three files appear in `build/generated-sources/connectors/`.
4. Run `./gradlew :connect-manifest-codegen:build` to verify they compile.

For testing, add the manifest to `src/test/resources/manifests/` and add a row to the
`allManifests()` provider in `CodegenIntegrationTest`.

---

## Test coverage

Integration tests in `CodegenIntegrationTest` compile each generated task class in-process
(via `javax.tools.JavaCompiler`) to verify the output is valid Java. Focused assertions then
check that the generated source contains the expected auth/pagination patterns.

| Manifest | Auth type | Pagination type |
|---|---|---|
| `defillama.yaml` | NoAuth | None |
| `xkcd.yaml` | NoAuth | None |
| `zapier.yaml` | ApiKeyAuthenticator | None |
| `gmail.yaml` | OAuthAuthenticator (refresh_token) | None |
| `pivotal_tracker.yaml` | BearerAuthenticator | CursorPagination |
| `sendowl.yaml` | BasicHttpAuthenticator | PageIncrement |
| `illumina_basespace.yaml` | OAuthAuthenticator | OffsetIncrement |
| `box.yaml` | OAuthAuthenticator (client_credentials) | None |
| `assemblyai.yaml` | BearerAuthenticator | CursorPagination (RequestPath) |
| `akeneo.yaml` | SessionTokenAuthenticator | None |
| `google_analytics_jwt.yaml` | JwtAuthenticator (RS256) | None |

---

## How the generator picks the right type

The model and the generator have a strict separation of responsibilities:

- **Model** — holds data and exposes boolean helpers (`isXxx()`). It never generates code.
- **Generator** — reads those helpers and emits the right JavaPoet statements. It never parses strings.

### End-to-end example

Given this YAML:
```yaml
authenticator:
  type: BearerAuthenticator
  api_token: "{{ config['api_key'] }}"
paginator:
  type: DefaultPaginator
  pagination_strategy:
    type: CursorPagination
    cursor_value: "{{ response['next_cursor'] }}"
```

**Step 1 — parser** deserializes it into the model:
```
AuthenticatorSpec { type = "BearerAuthenticator", apiToken = "{{ config['api_key'] }}" }
PaginatorSpec     { paginationStrategy.type = "CursorPagination" }
```

**Step 2 — generator** reads the model and branches:

```java
// auth decision — in buildRequestStatement()
if      (auth.isNoAuth())       { /* no header */ }
else if (auth.isBearer())       { emit: request.header("Authorization", "Bearer " + config.getApiKey()) }
else if (auth.isApiKey())       { /* inject into header or query param */ }
else if (auth.isBasicHttp())    { /* Authorization: Basic <cached base64> */ }
else if (auth.isOAuth())        { /* Authorization: Bearer <cached OAuth token> */ }
else if (auth.isSessionToken()) { /* Authorization: Bearer <cached session token> */ }
else if (auth.isJwt())          { /* Authorization: Bearer <freshly signed JWT> */ }

// pagination decision — in pollStreamN()
if      (paginator.hasNoPagination())                { /* single request */ }
else if (paginator.isCursor() && isRequestPath())    { /* url = nextCursor ?? baseUrl + path */ }
else if (paginator.isCursor())                       { /* append ?cursor=X each iteration */ }
else if (paginator.isPageIncrement())                { /* append ?page=N each iteration */ }
else if (paginator.isOffsetIncrement())              { /* append ?offset=N each iteration */ }
```

Each `isXxx()` method is a simple string comparison on the `type` field that Jackson read
from the YAML:
```java
public boolean isBearer() {
    return "BearerAuthenticator".equalsIgnoreCase(type);
}
```

So the `type` string in the YAML is the single source of truth that drives which code gets
emitted. The full flow:

```
manifest.yaml
     │  type: BearerAuthenticator
     ▼
AuthenticatorSpec.type = "BearerAuthenticator"
     │  auth.isBearer() == true
     ▼
TaskGenerator emits:
     request.header("Authorization", "Bearer " + config.getApiKey())
```

### The role of the model layer

Without the model, the generator would have to dig through raw `Map<String, Object>` from
the YAML parser — fragile, untyped, and hard to extend. The model provides:

- **Type safety** — compiler catches field name typos
- **Discoverability** — all supported fields and helpers are visible in one class
- **Isolation** — YAML structure changes only touch model classes, not generator logic
- **Consistent extension pattern** — adding any new type is always the same two steps:
  add a field + `isXxx()` to the model, add a branch to the generator

---

## Extending the generator

To support a new authenticator type:

1. Add fields and a `isXxx()` helper to `AuthenticatorSpec`.
2. Add a branch in `TaskGenerator.buildRequestStatement()` to emit the correct header.
3. If the auth requires a helper method (token fetch, signing), add it via `addAuthHelperMethods()`.

To support a new pagination strategy:

1. Add a `isXxx()` helper to `PaginatorSpec`.
2. Extend `buildPaginationInit()`, `buildUrlBlock()`, `buildPaginationStateUpdate()`, and
   `buildStop()` for the new strategy.

---

## Airbyte catalog coverage

Tested against the full Airbyte connector catalog (**522 source manifests**, May 2026).
Manifest directory is kept in exact 1-to-1 parity with the Airbyte on-disk repo
(501 top-level `manifest.yaml` + 21 nested under `<pkg>/manifest.yaml`).

### Live deployment status (standalone Kafka Connect, 2026-05-14)

559 connectors registered on `http://localhost:8083`.

| State | Count | Notes |
|---|---:|---|
| **RUNNING** (task green, polling into Kafka) | **47** | Active connectors with valid credentials |
| **Rate-limited / transient 5xx** | **23** | Auth OK, codegen OK — failures are API throttling/upstream errors |
| Auth failed (placeholder creds, no real key) | 272 | `register-all` placeholder configs without credentials |
| Other failed (codegen limitation, dynamic streams) | 217 | Missing features, stub tasks, etc. |
| **Total working (RUNNING + rate-limited)** | **70** | |

See [`working-connectors.md`](working-connectors.md) for the full per-connector breakdown.

### What works today

| Feature | Status |
|---|---|
| All 8 auth types listed above | Fully generated |
| All 5 pagination strategies | Fully generated |
| Single-level `SubstreamPartitionRouter` (parent→child) | Fully generated |
| `DatetimeBasedCursor` incremental sync | Fully generated |
| Static streams (`streams:` entries with `SimpleRetriever`) | Fully generated |
| `DynamicDeclarativeStream` — Google Sheets shape | Fully generated (OAuth + sheet discovery + batchGet) |
| `DynamicDeclarativeStream` — other shapes | Stub task (throws `ConnectException` on start with clear message) |
| `SelectiveAuthenticator` — OAuth branch selected | Fully generated |
| `$ref` anchor resolution across definitions | Fully generated (up to 8-pass deep resolution) |
| Config dot-notation `{{ config.key }}` | Fully generated |
| Mixed URL templates `https://…/{{ config['id'] }}/path` | Fully generated via `JinjaSnippets.interpolateTemplate` |

### What is not yet supported

| Gap | Affected manifests (approx.) | Notes |
|---|---|---|
| Multi-level `SubstreamPartitionRouter` (depth > 1) | ~239 | Router list with >1 element is silently skipped; child stream falls back to top-level fetch |
| `ListPartitionRouter` | ~120 | Static list-of-values fanout (e.g. iterate over regions) — not recognised |
| `AsyncRetriever` | ~15 | Async job-submit + poll pattern; entirely different fetch loop — no path |
| `HttpComponentsResolver` (generic dynamic streams) | ~20 | Non-Sheets DynamicDeclarativeStream shapes; stub task is emitted |
| Record transformations (`transformations:` / `AddFields`, `RemoveFields`) | most | Fields silently ignored; downstream receives un-transformed records |
| `CustomPartitionRouter` / `CustomPaginator` / `CustomRequester` | ~30 | Custom Python classes; no Java equivalent; silently skipped |
| `GroupingPartitionRouter` | ~8 | Groups records by field — ignored |
| Complex Jinja expressions (`| regex_search`, `format_datetime`, `| upper`, etc.) | scattered | `JinjaSnippets.allTemplatesRecognized()` returns false; expression emitted as empty string |
| Response schema validation / `schema_loader` | all | Ignored; records are emitted as raw JSON strings |
| `InlineSchemaLoader` / `JsonFileSchemaLoader` | all | Schemas not used for record structuring |

### Coverage estimate

| Tier | Count | Estimate |
|---|---|---|
| Fully runnable today (single-stream, known auth+pagination, no multi-router) | ~190 | ~35% |
| Partially broken (multi-level substream skipped silently) | ~240 | ~44% |
| Stub task only (AsyncRetriever / CustomRequester / unsupported dynamic) | ~114 | ~21% |

Implementing `ListPartitionRouter` and capping multi-level substream detection would raise
the fully-runnable tier to roughly **65%**. Adding `AsyncRetriever` support would push it
to ~**80%**.

### Hardest manifests (highest unsupported-feature score)

| Manifest | Primary blockers |
|---|---|
| `source-zoom.yaml` | AsyncRetriever × many streams |
| `source-clickup-api.yaml` | Multi-level SubstreamPartitionRouter + ListPartitionRouter |
| `source-pretix.yaml` | Multi-level substream (3 levels) + complex Jinja |
| `source-jira.yaml` | Multi-level substream + custom transformations |
| `source-amazon-ads.yaml` | AsyncRetriever + multi-router |
| `source-linkedin-ads.yaml` | DynamicDeclarativeStream (non-Sheets) + multi-router |
| `source-bing-ads.yaml` | CustomRequester + CustomPaginator |
| `source_google_ads.yaml` | AsyncRetriever + GroupingPartitionRouter |
| `source_asana.yaml` | Multi-level substream (4 levels) |
| `source-pinterest.yaml` | ListPartitionRouter + multi-level substream |

---

## Why coverage is low and the road to 95%

### Why coverage is only ~35% today

Coverage is low because two gaps alone block the majority of the catalog, and both fail
silently — the generated code compiles and runs but produces wrong data:

**Gap 1 — Multi-level substreams (blocks ~44% of manifests)**

The current codegen handles one parent→child relationship (e.g. `courses` → `lessons`).
The moment a manifest chains two routers — or has a child stream whose parent is itself a
child stream (e.g. `organizations` → `projects` → `tasks`) — the router list has `>1`
element and `TaskGenerator` drops the entire stream with a comment: `// multi-level
substream, out of scope, skip`. The connector still deploys and starts, but those streams
silently emit nothing. This category is **the single largest gap** in the catalog.

**Gap 2 — ListPartitionRouter (blocks ~22% of manifests)**

Many connectors fan a stream out over a static list of values — for example, iterating over
ad account types `["SPONSORED_PRODUCTS", "SPONSORED_BRANDS"]` or over region codes from
config. This router type has no model field (`isSubstream()` returns false) and no
`PartitionRouterSpec` support for it, so `getSubstreamRouter()` returns null and the stream
runs once with no partition variable injected. Again the code compiles; it just returns
incomplete data.

**Why silent failure is worse than a crash:** both gaps pass `CodegenIntegrationTest`
because the test only checks that the generated Java compiles. A test that actually runs
the task and counts records would catch them immediately.

---

### What is genuinely hard

Some gaps are large but mechanical to implement. Others are architecturally difficult:

**Easy (1–3 days each)**

| Gap | Why easy |
|---|---|
| `ListPartitionRouter` | Wrap the existing stream fetch loop in a `for (String partition : listValues)` loop. Model already parses the router; just add `isList()` check and a loop in `buildStreamPollMethod`. |
| `AddFields` / `RemoveFields` transformations | Post-process the JSON node before emitting `SourceRecord`. Model change + one extra block per record. |
| `GroupingPartitionRouter` | Emit a `groupBy` accumulator map after the fetch loop; flush each group as separate records. Purely additive. |
| More Jinja snippets (`| default(x)`, `now_utc()`) | Add patterns to `JinjaSnippets` registry + corresponding Java expressions. Localized to one file. |

**Medium (1–2 weeks each)**

| Gap | Why medium |
|---|---|
| Multi-level `SubstreamPartitionRouter` (depth 2) | The current `buildStreamPollMethod` generates a flat method. Depth-2 requires the child method to call another `fetchXxxPartitionKeys` for the grandparent. Model already parses multi-router lists; the generator needs a recursive `buildSubstreamChain()` that nests loops. The tricky part is naming (each depth gets its own `partitionKey_N` variable) and ordering (root parent must be fetched before child). |
| `HttpComponentsResolver` dynamic streams | Each unsupported connector needs its own `DynamicStreamTaskBody`-equivalent hardcoded to that connector's specific discovery endpoint and auth shape. Not general code; just more specific cases. LinkedIn Ads, Instagram, etc. each need a dedicated builder. |
| Complex Jinja (`format_datetime`, `| regex_search`) | Requires mapping Python datetime format strings to Java `DateTimeFormatter` patterns (e.g. `%Y-%m-%dT%H:%M:%SZ` → `uuuu-MM-dd'T'HH:mm:ss'Z'`). Doable but finicky; 20+ format codes need coverage. |

**Hard (2–4 weeks, architectural change)**

| Gap | Why hard |
|---|---|
| `AsyncRetriever` (depth: entire new fetch path) | Today every stream uses a synchronous request→parse→emit loop. `AsyncRetriever` is a 3-phase state machine: (1) POST to submit a job, receive `job_id`; (2) loop-poll a status endpoint until `job_status == "succeeded"` or timeout; (3) fetch results pages. The generated task needs to persist `job_id` in the Connect offset store across `poll()` calls, check status on the next `poll()`, and only fetch records when complete. This is a fundamentally different control flow that requires a new `buildAsyncPollMethod()` pipeline with its own start/status/fetch URL resolution from the manifest's `creation_requester`, `polling_requester`, and `download_requester`. |
| `CustomRequester` / `CustomPaginator` / `CustomPartitionRouter` | These are Python class references (`class_name: source_zoom.ZoomOAuth2Authenticator`). There is no general Java equivalent. The only realistic path is to hand-implement each one as a connector-specific class (same approach as `DynamicStreamTaskBody` for Sheets), meaning the gain is linear in effort — one connector at a time, not a general solution. |

---

### Roadmap to 95%

Estimated cumulative coverage after each phase:

```
Today                                                             ~35%
 │
 ├─ Phase 1: ListPartitionRouter + multi-level substream (depth 2)   → ~65%
 │   PartitionRouterSpec.isList() + values deserialization
 │   TaskGenerator: for-loop over list values per stream
 │   TaskGenerator: recursive buildSubstreamChain(depth ≤ 2)
 │   Tests: source-pinterest.yaml, source-clickup-api.yaml
 │
 ├─ Phase 2: Transformations + Jinja coverage                        → ~72%
 │   AddFields / RemoveFields post-processing in record loop
 │   JinjaSnippets: format_datetime, now_utc(), | default(x)
 │   Tests: source-jira.yaml, source-pretix.yaml
 │
 ├─ Phase 3: AsyncRetriever                                          → ~82%
 │   New model: AsyncRetrieverSpec (creation/polling/download nodes)
 │   New codegen path: buildAsyncPollMethod() state machine
 │   Offset store: persist job_id across poll() calls
 │   Tests: source-zoom.yaml, source-amazon-ads.yaml
 │
 ├─ Phase 4: More HttpComponentsResolver shapes                      → ~88%
 │   One DynamicStreamTaskBody-style builder per connector family:
 │   LinkedIn Ads, Instagram, Airtable (discovery URL + auth shape)
 │   JinjaSnippets.allTemplatesRecognized() gates which get full body
 │
 └─ Phase 5: Custom* family + depth-3+ substreams + edge cases       → ~95%
     Hand-implement Custom* connectors one by one
     Extend substream recursion to depth 3 (source-asana.yaml)
     GroupingPartitionRouter accumulator
     Remaining Jinja filter coverage (| regex_search, | int, etc.)
     Schema-aware record structuring (optional — may stay as raw JSON)
```

**Note on the ~5% ceiling**: the remaining 5% is connectors that depend on Python-native
capabilities with no reasonable Java analogue — arbitrary `class_name` Python modules,
`format_datetime` with locale-specific calendars, or connectors that call internal Airbyte
APIs unavailable outside the Airbyte runtime. These are not tractable through codegen alone.

### Recommended first PR

Start with `ListPartitionRouter` (Phase 1a). It is self-contained, affects ~120 manifests,
and the existing `PartitionRouterSpec` + `PartitionRouterListDeserializer` already parse the
router list — the only missing piece is the model `isList()` helper and a `for`-loop wrapper
in `buildStreamPollMethod`. A single day's work with the largest per-effort coverage gain.
