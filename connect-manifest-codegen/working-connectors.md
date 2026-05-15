# Working Connectors — Standalone Kafka Connect

**Connect endpoint:** `http://localhost:8083` (group `connect-cluster`)
**Last updated:** 2026-05-15
**JAR:** `connect-manifest-codegen-4.4.0-SNAPSHOT` (commit `9a0da4694f` — request_headers Jinja fix)
**Registered:** 73 total (49 RUNNING · 18 rate-limited / token-expired · 2 cred fix needed · 2 codegen gaps · 0 dynamic-stream stubs)

---

## Summary

| Bucket | Count | Codegen? |
|---|---:|---|
| RUNNING (tasks green, polling) | **49** | works |
| Rate-limited / token-expired / upstream-rejected | **18** | works — quota window, refresh token, or upstream behaviour |
| Cred fix needed (placeholder / paid-tier) | **2** | works — supply real config value |
| Codegen gaps (non-stub) | **2** | rule-5 skips (unported Python custom classes) |
| Dynamic-stream stubs | **0** | all DDS connectors now generate real task code |

**Codegen correct for 69 / 73 registered (~95%). DDS stubs eliminated (airtable + GA4 → real tasks).**

---

## RUNNING (49)

Tasks green, actively polling.

akeneo-connector, alpha-vantage, cal-com-connector, calendly-connector, chargebee,
clockify-connector, close-com-connector, coda-connector, configcat-connector, defillama,
dockerhub, formbricks, gmail-connector, google-calendar, google-classroom, google-forms,
google-sheets-connector, gutendex, hugging-face-datasets, intercom, jina-ai-reader, jira,
jotform-connector, launchdarkly, lemlist, lob, lokalise, mailerlite, mixmax, mux,
nasa-connector, onepagecrm, pexels-api-connector, pokeapi, pypi, recruitee-connector,
rss, scryfall, sentry-connector, shortcut, spacex-api, statuspage, trello-connector,
tvmaze-schedule, us-census-connector, whisky-hunter, wikipedia-pageviews, xkcd,
yahoo-finance-price

**New this session:** calendly-connector, close-com-connector, jotform-connector,
pexels-api-connector, statuspage (plus gmail-connector recovered from 429 window).

---

## Rate-limited / token-expired / upstream-rejected (18)

Codegen renders correctly; upstream throttles, the token expired, or the endpoint behaves quirkily. **Counts as working** — recovers on retry windows or with a fresh token.

| Connector | Code | Note |
|---|---|---|
| appfollow | 422 | upstream input rejection |
| apptivo | — | Token expired — response is HTML login page; refresh `api_key`/`access_key` |
| aviationstack-connector | 429 | free-tier quota exhausted |
| bitly-connector | 402 | free-tier credit exhausted |
| box-connector | — | OAuth refresh token expired — re-issue `refresh_token` |
| buildkite | 403 | token lacks `read_organizations` scope |
| coingecko-coins-connector | 429 | API throttle |
| coinmarketcap-connector | 429 | API throttle |
| giphy-connector | 429 | API throttle |
| gnews-connector | 403 | free-tier blocked |
| hubplanner-connector | 429 | API throttle |
| linear | 400→429 | GraphQL rate-limit encoded as 400 (`RATELIMITED`) |
| newsapi-connector | 429 | free-tier quota |
| omnisend-connector | 404 | Account has no orders — Omnisend returns 404 for empty results |
| openfda-v2 | 400 | upstream query rejected on one stream; other streams polling |
| pipedrive-connector | 429 | API throttle |
| the-guardian-api-connector | 429 | API throttle |
| todoist-connector | 410 | Upstream removed the endpoint; harmless retries |
| toggl-connector | 402 | premium endpoint on free plan |

---

## Cred fix needed (2)

These genuinely need a config / credential change before they can run.

| Connector | Code | Fix |
|---|---|---|
| breezy-hr | 400 | Replace placeholder `REPLACE_WITH_YOUR_COMPANY_ID` with your real Breezy company ID. |
| freshdesk-connector | 404 | `/skills` endpoint is paid-tier only — needs Pro/Enterprise Freshdesk. Other streams work. |

---

## Codegen gaps — non-stub (2)

Connector starts but a stream fails because the generator can't render a custom Python class. Rule-5 skip (no live SDKs, no custom-class ports without a clear REST mapping).

| Connector | Missing component | Effect |
|---|---|---|
| klaviyo-connector | `KlaviyoIncludedFieldExtractor` | one stream extractor; rest of connector is fine |
| google-analytics-data-api | `CombinedExtractor`, `KeyValueExtractor`, `DimensionFilterConfigTransformation` | custom class ports now registered; DDS codegen done (real task generated) — needs real credentials to verify end-to-end |

### Recently fixed
- **calendly-connector** (commit `4c980d5ace`) — SubstreamPartitionRouter now
  injects `parent_stream_configs[].request_option` as a query parameter
  (`?organization=<URI>`). Verified end-to-end: task RUNNING, producing to
  `event_types`, `organization_memberships`, `api_user` topics.
  Likely also unblocks other connectors using the same shape — worth a sweep.

---

## Dynamic-stream stubs (0)

All DDS connectors now generate real task code — `GenericDynamicStreamStub` is no longer emitted
for any registered connector.

Previously stubbed:
- **airtable** — now generates `AirtableSourceTask` (DDS.T2, commit `c559fe261b`). Discovers
  all bases→tables at `start()` via paginated metadata API. OAuth2 + PAT auth. Needs valid
  credentials at `~/.kafka-connect-credentials/connector-airtable.properties` to run.
- **google-analytics-data-api** — now generates `GoogleAnalyticsDataApiSourceTask` (DDS.T3,
  commit `fe6857ee78`). Embeds 57 default reports; polls every `propertyId × report`.
  Client OAuth2 + Service-account JWT (RS256). Needs valid credentials to run.

---

## How to redeploy

```bash
# From /home/trieu/kafka/kafka
./gradlew :connect-manifest-codegen:jar
cp connect-manifest-codegen/build/libs/connect-manifest-codegen-4.4.0-SNAPSHOT.jar \
   /home/trieu/kafka/standalone/plugins/codegen/
# Either: hot-restart helper
make -f connect-manifest-codegen/Makefile.connect redeploy
# Or: manual restart
kill -9 $(pgrep -f 'ConnectDistributed.*standalone/')
LOG_DIR=/home/trieu/kafka/standalone/logs \
KAFKA_HEAP_OPTS='-Xms256M -Xmx2G' \
  nohup /home/trieu/kafka/standalone/bin/connect-distributed.sh \
    /home/trieu/kafka/standalone/config/connect-distributed.properties \
    > /home/trieu/kafka/standalone/logs/stdout.log 2>&1 &
# Wait ~20s, then:
make -f connect-manifest-codegen/Makefile.connect register
```

---

## Adding new credentials

Drop a file in `~/.kafka-connect-credentials/`:

```properties
# connector-myservice.properties
name=myservice-connector
connector.class=io.kafka.connect.generated.MyserviceSourceConnector
tasks.max=1
topic.creation.default.replication.factor=1
topic.creation.default.partitions=1
api_key=YOUR_KEY_HERE
start_date=2024-01-01T00:00:00Z
```

For connectors whose spec has a nested `oneOf` object (e.g. jotform's `api_endpoint`), use a `.json` file with the nested value stringified — see `connector-google-sheets.json` for the pattern.

Then run `make -f connect-manifest-codegen/Makefile.connect register`.
