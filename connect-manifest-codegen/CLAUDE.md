# Working rules for `connect-manifest-codegen`

These rules apply to every change in this module. They exist because the codegen
re-implements Airbyte CDK semantics in Java — so behavioural drift, untested
edge cases, and big un-reviewable commits all hurt disproportionately.

## 1. Always re-read the Airbyte source before implementing

Before writing or modifying any component (a `Custom*` Java port, a parser
change, a generator branch, a runtime helper):

- Find the Python source in the Airbyte repo first. Read the class, its
  base class, and any helpers it calls. Note the exact method signatures and
  what each kwarg does.
- For declarative-manifest behaviour (paginators, cursors, extractors,
  Jinja interpolation, error handlers), read the matching CDK class in
  `airbyte-cdk/python/airbyte_cdk/sources/declarative/`.
- Quote the Python source (file path + line range) in the task description or
  PR body so a reviewer can diff Python vs. Java side-by-side.
- If you cannot find the Python source, stop and ask. Do not guess Airbyte
  semantics from the manifest YAML alone — the YAML is the input, not the
  spec.

This applies just as strongly to bug fixes: before changing behaviour, confirm
what the Python runtime actually does for the same input.

## 2. Split work into tasks; one task = one commit

- Use `TaskCreate` to break any non-trivial change into discrete tasks before
  starting. A task is a unit of work that compiles cleanly, passes lint, and
  passes its tests on its own.
- Mark exactly one task `in_progress` at a time.
- When a task is done: run `compileJava`, `checkstyleMain`, and the relevant
  test target — all green — then commit and push that task on its own.
- Commit message scope = the task scope. Do not bundle "while I was here"
  cleanups into a feature commit; that is a separate task.
- Do not move on to the next task until the previous task is committed and
  pushed. The branch state on `origin` should always be a sequence of clean,
  individually reviewable commits.

## 3. Tests must cover every case, including integration

For every component (Custom* impl, parser, generator branch, runtime helper):

- **Unit tests** for each public method. Table-driven where the input space is
  enumerable. Cover: happy path, nulls, empty inputs, malformed inputs, and
  any branch from the Python source.
- **Integration tests** that exercise the component through the real codegen
  pipeline (`ManifestParser` → `TaskGenerator` → JavaPoet → `compileJava`).
  A unit test on the spec object is not enough; the generated source must
  also compile and behave correctly.
- **Codegen integration test:** any new manifest feature must land with a
  fixture under `src/test/resources/manifests/` and an assertion in
  `CodegenIntegrationTest` that the generated connector compiles cleanly.
- **End-to-end verification by the agent.** After unit + integration tests
  pass, the agent itself runs the connector end to end:
  1. `./gradlew :connect-manifest-codegen:generateConnectors :connect-manifest-codegen:jar`
  2. Deploy the JAR to a local Strimzi or standalone Kafka Connect cluster.
  3. POST the connector config, observe the topic, and confirm real records
     land. Capture the output (record count, sample record) in the task notes.
  Do not mark a phase complete on unit tests alone.

## 4. Credentialed tests follow Airbyte's `acceptance-test-config.yaml` model

Real connectors need real credentials. We mirror Airbyte's pattern:

- Each connector under test gets a config file at
  `src/test/resources/acceptance-tests/<connector>.yaml` describing:
  - which streams to test
  - expected record schemas
  - cursor / state assertions for incremental streams
  - sample config (with secret placeholders, never literal secrets)
- Secrets resolve from environment variables at test time
  (`AIRBYTE_<CONNECTOR>_<FIELD>`). The YAML names the env var; the test
  harness reads it. Tests that need a missing secret are **skipped, not
  failed**, with a clear message — same as Airbyte's behaviour.
- The harness drives: `discover` (list streams + schemas), `read` (full
  refresh produces records), `read-incremental` (cursor advances + resume
  works), and `state-resume` (kill mid-stream, restart, no duplicates / no
  gaps). One harness, every credentialed connector reuses it.
- CI runs the no-secret subset on every PR. The full credentialed suite runs
  in a separate job that requires the secret bundle.
- When porting a Custom* component, copy the matching Airbyte
  `acceptance-test-config.yaml` entries (streams, expected schemas) so we
  test the same surface they do.

## 5. No live vendor SDKs

Do not pull in third-party vendor SDKs (e.g. `com.google.api-ads:google-ads-java`,
`com.google.apis:google-api-services-*`, AWS SDK, Salesforce SDK) to back a
Custom* implementation. The codegen emits a portable Kafka Connect plugin —
adding a vendor SDK pins us to that vendor's release cadence, breaks on major
protobuf/grpc upgrades, balloons the plugin JAR, and pulls transitive deps
that conflict with Connect runtime classes.

Implement Custom* requesters and decoders against the vendor's REST/HTTP API
directly using `java.net.http.HttpClient` (already used by every generated
task) and Jackson for JSON. If the vendor only ships gRPC, raise it before
implementing — do not silently add the SDK. Stubs that throw
`ConnectException("not yet implemented")` are acceptable until a REST path
exists; vendoring the SDK is not.

This applies to Phase 2 of the Google Ads work and to any future connector
that has an "official Java SDK" — we re-implement against the underlying
HTTP API, same way the Airbyte Python source does it.

## What this means in practice

A typical change loop:

1. Read the Python source in the Airbyte repo. Note file paths + line ranges.
2. `TaskCreate` for each subtask (interface, impl, unit tests, integration
   test, e2e check, acceptance-test config).
3. For each task: implement → `compileJava` → `checkstyleMain` → run tests →
   commit → push → mark complete → next task.
4. After the last code task, run end-to-end against a real Kafka Connect
   instance and capture the result in the final commit message or PR body.
5. If credentials are needed, add or update the `acceptance-tests/<x>.yaml`
   entry and verify the credentialed test passes locally with the env vars
   set.

If any rule above is in tension with a specific change, raise it explicitly
before deviating — do not silently skip. These rules cost time up front and
save much more time at review and in production.

## Coverage baseline (as of 2026-05-06, after Phase 3)

Of the 549 manifests under `src/test/resources/manifests/`:

| Class | Count | % | Meaning |
|---|---:|---:|---|
| Works correctly | 432 | 79% | Uses only features we implement |
| Works but wrong record shape | 0 | 0% | All transforms now implemented |
| Works until first transient error | 0 | 0% | Retry/backoff now implemented |
| Broken (uses unimplemented feature) | 104 | 19% | Fails at runtime or silently misses data |
| Stub (throws on `start()`) | 13 | 2% | `GenericDynamicStreamStub` |

Phases implemented so far:
- **Phase 1**: `DefaultErrorHandler` / `CompositeErrorHandler` / backoff strategies → wired
- **Phase 2**: All 11 transformation types (`AddFields`, `RemoveFields`, `RecordFilter`,
  key transforms, `FlattenFields`, `DpathFlattenFields`, config transforms) → wired
- **Phase 3**: POST/PUT HTTP methods + `request_body_json` / `request_body_data` +
  paginator `inject_into: body_json` / `body_data` → wired

Re-run with `./gradlew :connect-manifest-codegen:test --tests
ManifestCoverageReport.gradeEndToEndCoverage`; the report file
`ManifestCoverageReport.java` lives in `src/test/java/.../codegen/`.

### Top blockers (manifests using each feature)

| # | Missing feature | Effect |
|---:|---|---|
| 49 | `DatetimeBasedCursor` with `step:` window slicing | sync only fetches one window |
| 26 | `HTTPAPIBudget` rate limiting | 429-storm |
| 25 | `MovingWindowCallRatePolicy` | same |
| 23 | `CustomRecordExtractor` (registry exists, not wired) | nothing extracted |
| 16 | `CustomTransformation` | records unmodified |

### Path to ~93% (under the rule-5 constraints)

1. ~~Phase 1: DefaultErrorHandler + backoff~~ ✓ Done.
2. ~~Phase 2: Transformations pipeline~~ ✓ Done.
3. ~~Phase 3: POST/PUT + request bodies~~ ✓ Done.
4. `DatetimeBasedCursor.step` window slicing → unblocks ~49.
5. Wire `Custom*` registry into the generated task → unblocks ~40.

After 4–5 plus the `ManifestParser.validate()` $ref-string fix (frees 11
otherwise-stubbed manifests), the working bucket goes from 432 → ~510 / 549
≈ **~93%**. The remaining ~39 manifests need `AsyncRetriever`, vendor-SDK-backed
custom components, or `DynamicSchemaLoader` / `PropertiesFromEndpoint` /
`ConfigComponentsResolver` — out of scope per rule 5 and the no-async
directive. **The realistic ceiling under current constraints is ~93%, not 100%.**

### Connectors that will not work without lifting a constraint

`source_google_ads.yaml`, `source-bing-ads.yaml`, `source-amazon-ads.yaml`,
`source-amazon-seller-partner.yaml`, `source-hubspot.yaml`,
`source-pinterest.yaml`, `source-linkedin-ads.yaml`, `source-stripe.yaml`,
`source-google-search-console.yaml`, `source_mixpanel.yaml`,
`source-sendgrid.yaml` — each needs `AsyncRetriever`, vendor SDKs, or
non-Sheets dynamic streams. Do not promise these without raising the
constraint first.
