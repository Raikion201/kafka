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

### Credential / setup docs for individual connectors

When adding credentials for a connector (or debugging why a registered one
won't authenticate), the Airbyte docs page for that source is the canonical
reference: how to mint the token, which scopes are required, what the
`start_date` / region / subdomain fields expect, and known upstream quirks.

URL pattern: `https://docs.airbyte.com/integrations/sources/<connector-name>`

The `<connector-name>` matches the manifest filename stem — drop the
`source-` prefix from `source-<name>.yaml`. Examples:
- `source-statuspage.yaml` → https://docs.airbyte.com/integrations/sources/statuspage
- `source-google-analytics-data-api.yaml` → https://docs.airbyte.com/integrations/sources/google-analytics-data-api
- `source-amazon-ads.yaml` → https://docs.airbyte.com/integrations/sources/amazon-ads

Fetch it with WebFetch when you need to know where the user should get an API
key, what the OAuth callback expects, or what a vendor-specific field
(`account_id`, `subdomain`, `region`, etc.) is supposed to contain.

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

## 5. What we deliberately don't codegen

The codegen targets the Airbyte declarative manifest surface that maps cleanly
onto a portable Kafka Connect HTTP plugin. The following are out of scope and
should be skipped (stub task that throws on `start()`), not faked:

1. **Python custom classes** — `CustomAuthenticator`, `CustomPartitionRouter`,
   `CustomTransformation`, `CustomRecordExtractor`, `CustomPaginationStrategy`,
   etc. that point at a Python class path (`source_<x>.components.Foo`). These
   need a hand-written Java port via `CustomComponentRegistry`. Until the port
   exists, the manifest is a rule-5 skip. (Ports we already have: see
   `CustomComponentRegistry`.)
2. **Non-HTTP transports** — anything that isn't request/response over HTTP(S).
   gRPC, raw TCP, JDBC sources, file/SFTP sources. Not in scope.
3. **GraphQL sources** — `GraphQLSource` and any manifest whose request is a
   GraphQL query body that has to be assembled from a schema. We don't have a
   GraphQL emitter and won't add one without an explicit ask.
4. **Live vendor SDKs** — do not pull in third-party vendor SDKs (e.g.
   `com.google.api-ads:google-ads-java`, `com.google.apis:google-api-services-*`,
   AWS SDK, Salesforce SDK) to back a Custom* implementation. Adding a vendor
   SDK pins us to that vendor's release cadence, breaks on major
   protobuf/grpc upgrades, balloons the plugin JAR, and pulls transitive deps
   that conflict with Connect runtime classes. Implement Custom* requesters
   and decoders against the vendor's REST/HTTP API directly using
   `java.net.http.HttpClient` (already used by every generated task) and
   Jackson for JSON. If the vendor only ships gRPC, that falls under (2).

**Explicitly in scope, despite the historical "rule-5 skip" wording in older
notes:**

- **DynamicDeclarativeStream (DDS)** — manifests that resolve their stream
  list at runtime (e.g. Mailchimp's per-list streams, Notion's per-database
  streams). Implement the resolver path; do not emit a stub.
- **AsyncRetriever** — long-poll / job-create-then-poll flows. In scope; the
  codegen should emit the create-job → poll-status → fetch-result loop the
  Python `AsyncRetriever` runs.
- **HTTPAPIBudget / MovingWindowCallRatePolicy** — client-side rate limiting.
  In scope.

If you find yourself about to emit a stub for anything in the "in scope" list,
stop and implement it. Stubs are only for (1)–(4) above.

Stubs throw `ConnectException("not yet implemented")` (or the existing
DDS-style message) and the manifest is recorded as a rule-5 skip in
`working-connectors.md` with the specific reason — Python class name,
GraphQL, gRPC, or vendor-SDK-only.

## 6. Track unfillable connectors after every credentialing session

When the user asks for N connectors to credential and registration of one or
more fails, never lose that information. At the end of the session, write up
exactly which connectors did not get filled and why, so the next session
doesn't repeat the same dead-end suggestions.

Reasons a connector is "unfillable" include:
- Vendor signup requires a US mobile number, business email, or credit card
- Free tier doesn't expose the data the manifest expects (paid-only streams)
- OAuth flow needs a redirect URI we can't host / a domain we don't own
- The connector isn't in our codegen manifest set at all (no source-<x>.yaml)
- Codegen gap: rule-5 skip (Python custom class, GraphQL, gRPC, vendor-SDK-only)
  — note: DDS, AsyncRetriever, and HTTPAPIBudget are NOT rule-5 skips; implement them
- The credentials registered fine but the task is silent — vendor-config issue

What to do:

1. Before suggesting a connector for credentialing, verify it exists in
   `src/test/resources/manifests/` (a `find -iname "source-<name>*"` is
   enough), **and** check that it is not listed in the `## UNFILLABLE`
   section of `working-connectors.md`. Do not suggest connectors that
   aren't in our manifest set or that are already known unfillable.
2. At the end of any session that registered new connectors, update
   `working-connectors.md` with a per-connector outcome:
   - record count (or 0 if quiet) for RUNNING ones
   - the specific reason any requested connector wasn't filled
   - **append a row to the `## UNFILLABLE` table** for any connector that
     turned out to need a credit card, business gating, US phone, or other
     non-self-serve barrier — so we don't suggest it again next round
3. In the session's commit message, do NOT include diagnostic colour like
   "RUNNING but quiet — task silent after init". The user has asked for
   commit messages to stay neutral; keep the diagnostics in the doc body,
   not in git history.
4. If the user requested 10 and only 9 succeeded, call it out explicitly in
   your end-of-session report — do not silently drop the missing one.

## 7. Only register connectors to the broker when explicitly asked

Do not start the Kafka broker, start Kafka Connect, or POST connector configs
to `localhost:8083` on your own initiative — not at session start, not after a
codegen change, not as a "let me just verify" step. The broker and Connect
runtime stay stopped by default; storage stays cleared between sessions.

Only bring them up when the user explicitly asks to register new connectors
or add more connectors in this session (e.g. "register N more", "add
<connector>", "credential X"). When that ask comes:

1. Start the broker (KRaft) and Kafka Connect.
2. Register **only the connectors the user named**, using the Makefile's
   `ONLY=` filter — never bulk-register the whole credentials directory.
   Use `make -f Makefile.connect register ONLY=foo,bar` (or `register-all
   ONLY=foo,bar` if you also want to seed from manifests). Plain
   `make register` / `make register-all` with no `ONLY=` iterates every
   credentialed properties file and is forbidden in normal sessions —
   that's a "register everything" command, not what the user asked for.
3. When the session ends — or when the user says "stop" / "clear storage" —
   shut Connect and the broker back down and clear `/tmp/kraft-*` data
   directories. Do not leave them running across sessions.

End-to-end verification of codegen changes (rule 3's "e2e check") still
requires a running broker, but only do it when the user has asked for that
verification or for new connector registration in the current session. A
codegen-only change that the user has not asked to verify end-to-end should
stop at `compileJava` + tests.

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

## Coverage baseline (as of 2026-05-06, after Phase 5)

Of the 551 manifests under `src/test/resources/manifests/`:

| Class | Count | % | Meaning |
|---|---:|---:|---|
| Works correctly | 475 | 86.2% | Uses only features we implement |
| Works but wrong record shape | 0 | 0% | All transforms now implemented |
| Works until first transient error | 0 | 0% | Retry/backoff now implemented |
| Broken (uses unimplemented feature) | 63 | 11.4% | Fails at runtime or silently misses data |
| Stub (throws on `start()`) | 13 | 2.4% | `GenericDynamicStreamStub` |

Phases implemented so far:
- **Phase 1**: `DefaultErrorHandler` / `CompositeErrorHandler` / backoff strategies → wired
- **Phase 2**: All 11 transformation types (`AddFields`, `RemoveFields`, `RecordFilter`,
  key transforms, `FlattenFields`, `DpathFlattenFields`, config transforms) → wired
- **Phase 3**: POST/PUT HTTP methods + `request_body_json` / `request_body_data` +
  paginator `inject_into: body_json` / `body_data` → wired
- **Phase 4**: `DatetimeBasedCursor` with `step:` ISO-8601 window slicing → wired
- **Phase 5**: `CustomTransformation` + `CustomRecordExtractor` dispatch via
  `CustomComponentRegistry` → wired

Re-run with `./gradlew :connect-manifest-codegen:test --tests
ManifestCoverageReport.gradeEndToEndCoverage`; the report file
`ManifestCoverageReport.java` lives in `src/test/java/.../codegen/`.

### Top blockers (manifests using each feature)

| # | Missing feature | Effect |
|---:|---|---|
| 26 | `HTTPAPIBudget` rate limiting | 429-storm |
| 25 | `MovingWindowCallRatePolicy` | same |
| 14 | `CustomAuthenticator` | auth fails |
| 12 | `CustomPartitionRouter` | partitioning broken |
| 8 | `DynamicDeclarativeStream` | stub only |

### Path forward

1. ~~Phase 1: DefaultErrorHandler + backoff~~ ✓ Done.
2. ~~Phase 2: Transformations pipeline~~ ✓ Done.
3. ~~Phase 3: POST/PUT + request bodies~~ ✓ Done.
4. ~~Phase 4: `DatetimeBasedCursor.step` window slicing~~ ✓ Done.
5. ~~Phase 5: `CustomTransformation` + `CustomRecordExtractor` via registry~~ ✓ Done.
6. **Phase 6 (in flight)**: `DynamicDeclarativeStream` full codegen — runtime
   resolver helper + per-source codegen path. Unblocks mailchimp, notion,
   chargebee-shaped per-resource streams, ~8 stubbed manifests.
7. **Phase 7 (todo)**: `HTTPAPIBudget` + `MovingWindowCallRatePolicy` — client
   side rate limiting. Unblocks the largest single bucket (26 + 25 manifests).
8. **Phase 8 (todo)**: `AsyncRetriever` — create-job → poll-status → fetch-
   result loop. Unblocks google-ads / bing-ads / amazon-* / sendgrid.

Remaining 63 broken manifests are mostly waiting on Phase 6–8 above. The only
true ceiling now is the rule-5 skip list: Python custom classes that need
hand ports, GraphQL sources, gRPC-only vendors, and vendor-SDK-only connectors.
With Phases 6–8 done, projected coverage is ~98%; the residual ~2% is the
rule-5 skip set.

### Connectors that will not work without a rule-5 lift

`source-hubspot.yaml`, `source-stripe.yaml`, `source_mixpanel.yaml` —
each currently uses Python custom classes that need explicit Java ports via
`CustomComponentRegistry`. The vendor APIs are HTTP, so a port is in scope —
it just hasn't been written yet. Promise them only after the relevant
`Custom*` class has a Java implementation registered.
