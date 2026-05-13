# Airbyte → Kafka Connect Codegen: Coverage Analysis

## What This Is

This module (`connect-manifest-codegen`) reads Airbyte declarative YAML manifests and
generates Kafka Connect source connectors as Java source code using JavaPoet. The goal
is to run Airbyte's 523 declarative source connectors on Kafka Connect without
hand-writing any Java.

Airbyte ships 598 source connectors total. The 75-connector gap is fully-custom
Python/Java connectors with no YAML manifest — completely out of scope for this
approach.

---

## Connector Coverage (as of 2026-05-13)

| Bucket | Count | Description |
|--------|------:|-------------|
| **Works** | **455** | Uses only features we implement; generates, compiles, and polls correctly |
| **Needs custom ports** | **59** | Manifest references Python `custom_components` class names that must be ported to Java |
| **Cannot be built** | **9** | Uses GraphQL, AsyncRetriever, or non-HTTP transport — requires a new codegen path |
| **Total declarative** | **523** | All Airbyte manifests in our corpus |

---

## Bucket 1: Works (455 connectors)

These connectors use only standard Airbyte CDK declarative features:

- `SimpleRetriever` with `HttpRequester`
- `CursorPaginator`, `PageIncrement`, `OffsetIncrement`, `CursorPagination`
- `DatetimeBasedCursor` (including ISO-8601 `step:` window slicing)
- `ListPartitionRouter`, `SubstreamPartitionRouter`
- All 11 transformation types: `AddFields`, `RemoveFields`, `RecordFilter`,
  `FlattenFields`, `DpathFlattenFields`, key/value transforms, config transforms
- `DefaultErrorHandler` / `CompositeErrorHandler` with backoff strategies
- POST/PUT methods with `request_body_json` / `request_body_data`
- `CustomTransformation` and `CustomRecordExtractor` via `CustomComponentRegistry`

---

## Bucket 2: Needs Custom Ports (59 connectors)

Manifests declare `type: CustomRequester`, `type: CustomAuthenticator`,
`type: CustomPartitionRouter`, `type: CustomTransformation`, or
`type: CustomRecordExtractor` referencing Python class names.

Each class name is a separate Python implementation. Most are connector-specific;
only 5 are shared across multiple connectors.

### Subgroups

| Subgroup | Connectors | Status |
|----------|----------:|--------|
| Custom record transforms (pure map → map) | ~18 | Several already implemented |
| Custom HTTP requesters (non-standard auth/pagination) | ~12 | Asana, Mixpanel, JinaAI, YouTube implemented; others stubbed |
| Custom authenticators | ~14 | Identity stubs (connector starts, auth may fail) |
| Custom partition routers | ~12 | Identity stubs (connector starts, partitioning broken) |
| Custom record extractors | ~3 | Several already implemented |

**Already implemented (register in `CustomComponentRegistry`):**
- `RemoveEmptyFields`, `TransformDatetimesToRFC3339`, `SanitizeNumericFields`,
  `TransformEmptyMetrics`, `CustomFieldTransformation`, `DateTimeTransformer`,
  `ListAddFields`, `ObjectDpathExtractor`, `NullCheckedDpathExtractor`
- `AsanaHttpRequester`, `FunnelsHttpRequester` (Mixpanel), `JinaAiHttpRequester`,
  `ContentOwnerRequester` (YouTube Analytics)

**Identity-stubbed (connector starts; transform/auth/routing silently skipped):**
- `AddFieldsFromEndpointTransformation`, `BingAdsCampaignsRecordTransformer`,
  `InstagramMediaChildrenTransformation`, `NotionPropertiesTransformation`,
  `CampaignsDetailedTransformation`

---

## Bucket 3: Cannot Be Built (9 connectors)

These connectors require features that are either architecturally blocked or have
implementation cost completely disproportionate to the number of connectors they unlock.

| Feature | Connectors | Gap |
|---------|----------:|-----|
| **AsyncRetriever** | ~4 | 3-phase poll (submit → poll status → download); needs state-machine `poll()` generator |
| **GraphQL** | ~3 | POST body queries, 200-always errors, `pageInfo` pagination; needs new HTTP model |
| **SOAP** | ~1 | HTTP POST + XML, but: per-vendor namespaces, WS-Security auth, XPath response parsing, SOAP fault error model — ~1 connector, ROI terrible; **not worth building** |
| **gRPC** | ~1 | HTTP/2 + protobuf binary encoding; gRPC Java SDK banned by rule 5; hand-crafting protobuf wire format is unrealistic; **blocked** |

### What "new codegen path" means

`TaskGenerator` routes each manifest stream to one of several generator methods based
on stream type, each emitting a structurally different Java `poll()` method:

```
classifyStream(stream)
  → has ListPartitionRouter      → buildListRouterPollMethod()    → for-loop over list IDs
  → has SubstreamPartitionRouter → buildSubstreamPollMethod()     → nested fetch logic
  → simple paginated             → buildPollMethod()              → while(hasNext) loop
```

Adding AsyncRetriever or GraphQL support means:

1. **New spec classes** in `ManifestParser` (e.g. `AsyncRetrieverSpec` with submit URL,
   status URL, completion condition)
2. **New classifier branch** in `TaskGenerator`'s stream router
3. **New generator method** producing structurally different Java output:
   - `buildAsyncPollMethod()` → state-machine `poll()` reading `phase` from offset store
   - `buildGraphQLPollMethod()` → POST body with `{"query":…}`, check `response.errors[]`,
     paginate via `pageInfo.hasNextPage`

This is building a parallel generator track, not modifying existing paths.

---

## Phases Implemented

| Phase | Feature | Connectors Unblocked |
|-------|---------|--------------------:|
| Phase 1 | `DefaultErrorHandler` / `CompositeErrorHandler` / backoff | many |
| Phase 2 | All 11 transformation types | many |
| Phase 3 | POST/PUT HTTP methods + `request_body_json` / `request_body_data` + paginator body inject | many |
| Phase 4 | `DatetimeBasedCursor` ISO-8601 `step:` window slicing | many |
| Phase 5 | `CustomTransformation` + `CustomRecordExtractor` via `CustomComponentRegistry` | ~15 |
| Bug B1 | Jinja not rendered in OAuth `token_refresh_endpoint` | 19 |
| Bug B2 | Wrong datetime format used to parse `start_datetime` config value | 11 |
| Bug B3 | Epoch-seconds (`%s`) cursor fed a human-readable date string | 3 |
| Bug B4 | `now_utc()` returns String; `.strftime()` / arithmetic fail | 11 |
| Bug B5 | Python-style `'sep'.join(list)` in Jinja templates | 4 |
| Bug B6 | `CustomComponentRegistry` missing 18 class registrations | 19 |
| Fix | `buildListRouterUrlBlock` dropping requester `request_parameters` (Pipedrive 401) | ~5 |

---

## Known Gaps in "Working" Connectors

Some connectors report `RUNNING` but silently produce incomplete data:

| Gap | Example | Description |
|-----|---------|-------------|
| `CustomPartitionRouter` streams silently dropped | Trello | Streams with `stream_partition.id` in path are filtered out by stream classifier (line ~266 in `TaskGenerator`). Only top-level streams (`boards`, `organizations`) are polled. Child streams (`cards`, `lists`, `checklists`) never run. |
| Google Ads stub | google-ads | All classes registered so connector starts, but `GoogleAdsHttpRequester.send()` throws `ConnectException("not yet implemented")`. First poll crashes. |
| Rate limiting not enforced | many | `HTTPAPIBudget` / `MovingWindowCallRatePolicy` manifest fields are parsed but not emitted. Connectors may hit 429s on high-volume APIs. |

---

## Architecture Overview

```
YAML manifest
    │
    ▼
ManifestParser.java        — parses YAML → typed Java spec objects
    │
    ▼
TaskGenerator.java         — routes each stream to the right generator method
    │                         emits JavaPoet MethodSpec (poll(), start(), stop())
    ├── buildPollMethod()
    ├── buildSubstreamPollMethod()
    ├── buildListRouterPollMethod()
    └── buildListCyclePollMethod()
    │
    ▼
JavaPoet                   — writes .java source files
    │
    ▼
javac (via Gradle)         — compiles to .class
    │
    ▼
Kafka Connect plugin JAR   — deployed to Strimzi on minikube
```

**Runtime helpers** (called by generated code, not generated themselves):
- `DatetimeWindowHelper` — ISO-8601 / epoch / custom format parsing + formatting
- `AirbyteJinjaFunctions` / `AirbyteJinjaFilters` / `AirbyteDateTime` — Jinja evaluation
- `CustomComponentRegistry` — dynamic dispatch to registered `Custom*` implementations
- `SharedHttpClient` — singleton `java.net.http.HttpClient` shared across tasks

---

## Running Coverage Report

```bash
./gradlew :connect-manifest-codegen:test --tests ManifestCoverageReport.gradeEndToEndCoverage
```

## Checking Live Connector Status

```bash
# Port-forward must be running first:
kubectl port-forward -n kafka svc/my-connect-cluster-connect-api 18083:8083

make -f connect-manifest-codegen/Makefile.connect status
```

## Regenerate and Deploy All Connectors

```bash
./gradlew :connect-manifest-codegen:generateConnectors :connect-manifest-codegen:jar
make -f connect-manifest-codegen/Makefile.connect deploy
```
