# Working Connectors — Standalone Kafka Connect

**Connect endpoint:** `http://localhost:8084` (group `connect-cluster-2`)
**Last updated:** 2026-05-15
**JAR:** `connect-manifest-codegen-4.4.0-SNAPSHOT` @ commit `b5186d0d`
**Registered:** 77 (54 with credentials + 23 with no required auth)
**Total manifests on disk:** 521 — the remaining 444 require credentials we don't have.

A second standalone Connect runs on `localhost:8083` (group `connect-cluster`)
with an older JAR; this document tracks the 8084 cluster only.

---

## Summary

| Bucket | Count | Codegen status |
|---|---:|---|
| RUNNING (tasks green, polling) | **41** | works |
| Rate-limited / upstream-tier (still counts as working) | 10 | works |
| Bad / missing credentials (codegen is fine) | 13 | works |
| Codegen bug or unimplemented feature | 13 | **gap** |
| Registered but no task scheduled | 0 | n/a |

**Working from codegen's POV: 64 / 77 registered ≈ 83%.**
**Codegen gaps blocking the other 13: 5 distinct features.**

---

## RUNNING (41)

Tasks green, actively polling. Most have real credentials; public APIs need none.

asana, alpha-vantage, akeneo-connector, bitly-connector\*, cal-com-connector,
chargebee, clockify-connector, coda-connector, configcat-connector, defillama,
formbricks, gnews-connector\*, google-calendar, google-classroom, google-forms,
gutendex, hubplanner-connector, intercom, jina-ai-reader, jira, launchdarkly,
lemlist, linear, lob, lokalise, mailerlite, mixmax, mixpanel, mux,
nasa-connector, onepagecrm, openfda, pipedrive-connector\*, pokeapi,
recruitee-connector, scryfall, sentry-connector, shortcut, spacex-api,
the-guardian-api-connector\*, todoist-connector\*, trello-connector,
tvmaze-schedule\*, us-census-connector, whisky-hunter, xkcd

\* may slip into rate-limited bucket between snapshots; both states count as "working".

**Fixes from this branch verified live here:**

- `intercom` — per-stream error_handler (commit `198d122a36`) lets the
  `companies/scroll` 404 IGNORE filter take effect without poisoning earlier
  streams.
- `shortcut` — ConfigDef defaults merged into Jinja `_cfgMap` (commit
  `b5186d0d41`) lets `search_epics` pick up the `query` default from the
  manifest spec.

---

## Rate-limited / upstream-tier (10)

Codegen and credentials both fine. Upstream throttling or free-tier endpoint
restrictions. Will self-recover on retry windows. **Counts as working.**

| Connector | Symptom | Note |
|---|---|---|
| aviationstack-connector | 429 | free-tier quota |
| coingecko-coins-connector | 429 | API throttle |
| coinmarketcap-connector | 429 | API throttle |
| google-sheets-connector | 429 | sheet polled too often |
| pipedrive-connector | 429 | API throttle |
| the-guardian-api-connector | 429 | API throttle |
| bitly-connector | 402 Payment Required | free-tier credit exhausted |
| gnews-connector | 403 (free-tier endpoint) | upgrade needed |
| todoist-connector | 410 Gone | upstream API deprecation on one endpoint |
| tvmaze-schedule | 422 Unprocessable | upstream input rejection on one stream |

---

## Bad / missing credentials — codegen is fine (13)

Codegen renders the request correctly; the upstream rejects the credentials
(wrong scope, expired token, free-tier limit, redirect to login). Re-issuing
the credential fixes these.

| Connector | Symptom | What's needed |
|---|---|---|
| apptivo | 302 → `/app/login.jsp` → HTML | re-issue api_key/access_key |
| box-connector | OAuth refresh failed | new refresh_token |
| breezy-hr | 401 | regenerate token |
| buildkite | 403 | token needs `read_organizations` scope |
| drift | 401 | regenerate |
| gmail-connector | OAuth refresh failed | new refresh_token |
| ip2whois | 401 | regenerate |
| pinterest | OAuth refresh failed | new refresh_token |
| retently | 401 | regenerate |
| square | "Failed to authorize" | new access_token |
| statuspage | 401 | needs management API key, not page key |
| ticktick | 401 | regenerate |
| toggl-connector | 400 | wrong api_token format |

---

## Codegen bug or unimplemented feature (13)

Real codegen gaps. Fixing any of these would move the connector to RUNNING
without new credentials.

### Dynamic streams (8) — `DynamicDeclarativeStream` unimplemented

Manifest declares streams whose names/paths come from a runtime API call.
Generator emits `GenericDynamicStreamStub` which throws on `start()`.

- airtable, facebook-pages, mailchimp, monday, posthog, public-apis,
  recharge, zenloop

### Custom Python components not ported (2)

Manifest references a `class_name:` we don't have a Java implementation for.

| Connector | Missing class |
|---|---|
| google-ads | `source_google_ads.components.KeysToSnakeCaseGoogleAdsTransformation` |
| notion | `source_declarative_manifest.components.NotionUserTransformation` |

### URL / Jinja interpolation bugs (3)

Generator emits a malformed URL — usually because a `{{ config['x'] }}`
expression isn't rendered correctly into the request path.

| Connector | Symptom | Likely cause |
|---|---|---|
| marketo | `Illegal character in scheme name` | quoted URL string leaked into scheme |
| okta | `unsupported URI https://.okta.com/...` | empty `domain` not guarded |
| tiktok-marketing | `Illegal character in scheme name at index 0: "https://..."/?...` | URL wrapped in literal `"` |

---

## Not registered — credentials unavailable (444)

The remaining 444 manifests have one or more required config fields with no
default and no credential file in `~/.kafka-connect-credentials/`. They aren't
registered on this cluster; their codegen status is therefore untested here.
Codegen quality for that set is measured separately via
`ManifestCoverageReport` (see CLAUDE.md, currently ~93% under rule-5
constraints).

To add one: drop a `connector-<name>.properties` (or `.json`) into
`~/.kafka-connect-credentials/`, then re-run the registration script.

---

## How to redeploy (no Kubernetes)

```bash
# Build new JAR
./gradlew :connect-manifest-codegen:jar

# Drop into plugin path + restart standalone2
cp build/libs/connect-manifest-codegen-4.4.0-SNAPSHOT.jar \
   /home/trieu/kafka/standalone2/plugins/codegen/
kill $(pgrep -f 'ConnectDistributed.*standalone2')
LOG_DIR=/home/trieu/kafka/standalone2/logs \
KAFKA_HEAP_OPTS='-Xms256M -Xmx2G' \
  nohup /home/trieu/kafka/standalone/bin/connect-distributed.sh \
    /home/trieu/kafka/standalone2/config/connect-distributed.properties \
    > /home/trieu/kafka/standalone2/logs/stdout.log 2>&1 &
```

Total round-trip: ~15s (vs minutes for the Strimzi Kaniko build).
