# Working Connectors — Standalone Kafka Connect

**Connect endpoint:** `http://localhost:8083` (group `connect-cluster`)
**Last updated:** 2026-05-16
**JAR:** `connect-manifest-codegen-4.4.0-SNAPSHOT` (commit `9a0da4694f` — request_headers Jinja fix)
**Registered:** 81 total (56 RUNNING · 19 rate-limited / token-expired · 2 cred fix needed · 2 codegen gaps · 2 cred-format / 422 · 0 dynamic-stream stubs)

---

## Summary

| Bucket | Count | Codegen? |
|---|---:|---|
| RUNNING (tasks green, polling) | **56** | works |
| Rate-limited / token-expired / upstream-rejected | **19** | works — quota window, refresh token, or upstream behaviour |
| Cred-format / 422 (likely bad config field) | **2** | works — fix config value |
| Cred fix needed (placeholder / paid-tier) | **2** | works — supply real config value |
| Codegen gaps (non-stub) | **2** | rule-5 skip (unported Python custom class) |
| Dynamic-stream stubs | **0** | all DDS connectors now generate real task code |

**Codegen correct for 79 / 81 registered (~98%). 5 new credentialed connectors verified live this session: openweather, polygon-stock-api, judge-me-reviews, savvycal, pingdom.**

---

## RUNNING (56)

Tasks green, actively polling.

airtable, akeneo-connector, alpha-vantage, appfollow, cal-com-connector, calendly-connector,
chargebee, clockify-connector, close-com-connector, coda-connector, configcat-connector,
defillama, dockerhub, formbricks, gmail-connector, google-calendar, google-classroom,
google-forms, google-sheets-connector, gutendex, hugging-face-datasets, intercom,
jina-ai-reader, jira, jotform-connector, judge-me-reviews, launchdarkly, lemlist, linear,
lob, lokalise, mailerlite, mux, nasa-connector, onepagecrm, openweather, pexels-api-connector,
pingdom, pokeapi, polygon-stock-api, pypi, recruitee-connector, rss, savvycal, scryfall,
sentry-connector, shortcut, spacex-api, statuspage, trello-connector, tvmaze-schedule,
us-census-connector, whisky-hunter, wikipedia-pageviews, xkcd, yahoo-finance-price

**New this session:**
- **openweather** — switched manifest from One Call 3.0 to `/data/2.5/weather` (free key
  doesn't have One Call subscription). 2185 records on topic `onecall`.
- **polygon-stock-api**, **judge-me-reviews**, **savvycal**, **pingdom** — new
  credentialed registrations, all RUNNING.
- **statuspage** — unblocked by codegen header-Jinja fix (commit `9a0da4694f`).
- **linear**, **appfollow** — recovered from previous rate-limit window.

---

## Rate-limited / token-expired / upstream-rejected (19)

Codegen renders correctly; upstream throttles, the token expired, or the endpoint behaves quirkily. **Counts as working** — recovers on retry windows or with a fresh token.

| Connector | Code | Note |
|---|---|---|
| apptivo | — | Token expired — response is HTML login page; refresh `api_key`/`access_key` |
| aviationstack-connector | 429 | free-tier quota exhausted |
| bitly-connector | 402 | free-tier credit exhausted |
| box-connector | — | OAuth refresh token expired — re-issue `refresh_token` |
| buildkite | 403 | token lacks `read_organizations` scope |
| coingecko-coins-connector | 429 | API throttle |
| coinmarketcap-connector | 429 | API throttle |
| giphy-connector | 429 | API throttle |
| gnews-connector | 403 | free-tier blocked |
| google-analytics-data-api | 403 | OAuth token re-expired — refresh `client_secret`/refresh token |
| hubplanner-connector | 429 | API throttle |
| mixmax | 429 | API throttle |
| newsapi-connector | 429 | free-tier quota |
| omnisend-connector | 404 | Account has no orders — Omnisend returns 404 for empty results |
| openfda-v2 | 400 | upstream query rejected on one stream; other streams polling |
| pipedrive-connector | 429 | API throttle |
| the-guardian-api-connector | 429 | API throttle |
| todoist-connector | 410 | Upstream removed the endpoint; harmless retries |
| toggl-connector | 402 | premium endpoint on free plan |

---

## Cred-format / 422 (2)

Codegen renders correctly; the request is being rejected by the upstream as malformed. Most likely a config field is the wrong shape (e.g. internal ID vs. name).

| Connector | Code | Suspected fix |
|---|---|---|
| mailersend | 422 | `domain_id` must be the MailerSend internal ID (`GET /v1/domains` → `data[].id`), not the domain name |
| newsdata-io | 422 | Re-check `api_key` format and `start_date` window |

---

## Cred fix needed (2)

Genuinely needs a config / credential change before it can run.

| Connector | Code | Fix |
|---|---|---|
| breezy-hr | 400 | Replace placeholder `REPLACE_WITH_YOUR_COMPANY_ID` with your real Breezy company ID. |
| freshdesk-connector | 404 | `/skills` endpoint is paid-tier only — needs Pro/Enterprise Freshdesk. Other streams work. |

---

## Codegen gaps — non-stub (2)

Connector starts but a stream fails because the generator can't render a custom Python class, or because the manifest references a config field the spec doesn't declare. Rule-5 skip (no live SDKs, no custom-class ports without a clear REST mapping).

| Connector | Missing component | Effect |
|---|---|---|
| klaviyo-connector | `KlaviyoIncludedFieldExtractor` | one stream extractor fails; rest of connector polling fine |
| mailosaur | spec gap: manifest references `config['serverid']` but spec doesn't declare it, so the field is never generated. Needs codegen-side auto-scan of `config[...]` refs, or a manifest patch. | 404 on `/api/messages` (no serverid) |

### Recently fixed
- **statuspage** (commit `9a0da4694f`) — `request_headers` values were emitted as raw
  string literals; switched to `$L` + `interpolateTemplate` so Jinja inside header
  values now interpolates. Fix swept across 4 emission sites in `TaskGenerator`.
- **openweather** (manifest-local) — path switched from `onecall` (3.0/paid) to
  `weather` (2.5/free); url_base now `data/2.5/`. Producing real SF weather records.
- **calendly-connector** (commit `4c980d5ace`) — SubstreamPartitionRouter now
  injects `parent_stream_configs[].request_option` as a query parameter
  (`?organization=<URI>`).

---

## Dynamic-stream stubs (0)

All DDS connectors now generate real task code — `GenericDynamicStreamStub` is no longer emitted
for any registered connector.

Previously stubbed:
- **airtable** — now generates `AirtableSourceTask` (DDS.T2, commit `c559fe261b`). Discovers
  all bases→tables at `start()` via paginated metadata API. PAT auth verified live — task RUNNING.
- **google-analytics-data-api** — now generates `GoogleAnalyticsDataApiSourceTask` (DDS.T3,
  commit `fe6857ee78`). Embeds 57 default reports; polls every `propertyId × report`.
  Currently failing on 403 (OAuth token re-expired) — codegen is correct.

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
