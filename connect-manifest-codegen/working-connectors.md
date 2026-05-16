# Working Connectors — Standalone Kafka Connect

**Connect endpoint:** `http://localhost:8083` (group `connect-cluster`)
**Last updated:** 2026-05-16
**JAR:** `connect-manifest-codegen-4.4.0-SNAPSHOT` (commit `9a0da4694f` — request_headers Jinja fix)
**Registered:** 81 total (68 RUNNING · 11 rate-limited / token-expired · 1 cred fix needed · 1 codegen gap · 0 dynamic-stream stubs)

---

## Summary

| Bucket | Count | Codegen? |
|---|---:|---|
| RUNNING (tasks green, polling) | **68** | works |
| Rate-limited / token-expired / upstream-rejected | **11** | works — quota window, refresh token, or upstream behaviour |
| Cred fix needed (placeholder / paid-tier) | **1** | works — supply real config value |
| Codegen gaps (non-stub) | **1** | rule-5 skip (unported Python custom class) |
| Dynamic-stream stubs | **0** | all DDS connectors now generate real task code |

**Codegen correct for 79 / 81 registered (~98%). All 8 newly-credentialed connectors this session (openweather, polygon, judge-me, savvycal, pingdom, mailersend, mailosaur, newsdata-io) now RUNNING.**

---

## RUNNING (68)

Tasks green, actively polling.

airtable, akeneo-connector, alpha-vantage, bitly-connector, cal-com-connector,
calendly-connector, chargebee, clockify-connector, close-com-connector, coda-connector,
coinmarketcap-connector, configcat-connector, defillama, dockerhub, formbricks,
giphy-connector, gmail-connector, gnews-connector, google-calendar, google-classroom,
google-forms, google-sheets-connector, gutendex, hubplanner-connector,
hugging-face-datasets, intercom, jina-ai-reader, jira, jotform-connector,
judge-me-reviews, launchdarkly, lemlist, linear, lob, lokalise, mailerlite, mailersend,
mailosaur, mixmax, mux, nasa-connector, newsapi-connector, newsdata-io, onepagecrm,
openweather, pexels-api-connector, pingdom, pipedrive-connector, pokeapi,
polygon-stock-api, pypi, recruitee-connector, rss, savvycal, scryfall, sentry-connector,
shortcut, spacex-api, statuspage, the-guardian-api-connector, todoist-connector,
trello-connector, tvmaze-schedule, us-census-connector, whisky-hunter,
wikipedia-pageviews, xkcd, yahoo-finance-price

**New this session (all 8 credentialed RUNNING):**
- **openweather** — manifest switched 3.0/onecall → 2.5/weather (free key).
- **polygon-stock-api**, **judge-me-reviews**, **savvycal**, **pingdom** — straight-through.
- **mailersend** — `start_date` reset to within the free-tier 1-day retention window.
- **mailosaur** — manifest patch: added `serverid` to spec, dropped redundant
  `SubstreamPartitionRouter` so `request_parameters.server` is emitted as query param.
- **newsdata-io** — dropped paid-only `historical_news` stream from manifest; added
  `countries=us`, `domains=techcrunch`, `languages=en`, `categories=technology` to
  cred (API rejects empty filter values with 422).
- **statuspage** — unblocked by codegen header-Jinja fix (commit `9a0da4694f`).

---

## Rate-limited / token-expired / upstream-rejected (11)

Codegen renders correctly; upstream throttles, the token expired, or the endpoint behaves quirkily. **Counts as working** — recovers on retry windows or with a fresh token.

| Connector | Code | Note |
|---|---|---|
| appfollow | 422 | upstream input rejection |
| apptivo | — | Token expired — response is HTML login page; refresh `api_key`/`access_key` |
| aviationstack-connector | 429 | free-tier quota exhausted |
| box-connector | — | OAuth refresh token expired — re-issue `refresh_token` |
| buildkite | 403 | token lacks `read_organizations` scope |
| coingecko-coins-connector | 429 | API throttle |
| google-analytics-data-api | 403 | OAuth token re-expired — refresh `client_secret`/refresh token |
| omnisend-connector | 404 | Account has no orders — Omnisend returns 404 for empty results |
| openfda-v2 | 400 | upstream query rejected on one stream; other streams polling |
| toggl-connector | 402 | premium endpoint on free plan |
| freshdesk-connector | 404 | `/skills` endpoint is paid-tier only — needs Pro/Enterprise Freshdesk. Other streams work. |

---

## Cred fix needed (1)

Genuinely needs a config / credential change before it can run.

| Connector | Code | Fix |
|---|---|---|
| breezy-hr | 400 | Replace placeholder `REPLACE_WITH_YOUR_COMPANY_ID` with your real Breezy company ID. |

---

## Codegen gaps — non-stub (1)

Connector starts but a stream fails because the generator can't render a custom Python class. Rule-5 skip (no live SDKs, no custom-class ports without a clear REST mapping).

| Connector | Missing component | Effect |
|---|---|---|
| klaviyo-connector | `KlaviyoIncludedFieldExtractor` | one stream extractor fails; rest of connector polling fine |

### Recently fixed
- **statuspage** (commit `9a0da4694f`) — `request_headers` values were emitted as raw
  string literals; switched to `$L` + `interpolateTemplate` so Jinja inside header
  values now interpolates. Fix swept across 4 emission sites in `TaskGenerator`.
- **openweather** (manifest-local) — path switched from `onecall` (3.0/paid) to
  `weather` (2.5/free); url_base now `data/2.5/`.
- **mailosaur** (manifest-local) — added `serverid` to spec.properties; dropped
  the `SubstreamPartitionRouter` block so the codegen now emits
  `request_parameters.server={{ config['serverid'] }}` as a query param. Also
  uncovered a codegen bug: when a `SubstreamPartitionRouter` is present, sibling
  `request_parameters` are dropped — worth a fix.
- **newsdata-io** (manifest-local) — dropped `historical_news` (paid-only `/archive`
  endpoint) from streams list. Also a codegen bug surfaced: empty list-join Jinja
  templates (`{{ ','.join(config.get('countries', [])) }}`) render to empty
  string `&country=`, which newsdata-io's API rejects with 422 — codegen should
  omit URL params whose rendered value is empty.
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
kill -9 $(pgrep -f 'java.*ConnectDistributed.*standalone/')
LOG_DIR=/home/trieu/kafka/standalone/logs \
KAFKA_HEAP_OPTS='-Xms256M -Xmx2G' \
  nohup /home/trieu/kafka/standalone/bin/connect-distributed.sh \
    /home/trieu/kafka/standalone/config/connect-distributed.properties \
    > /home/trieu/kafka/standalone/logs/stdout.log 2>&1 &
# Wait ~20s, then:
make -f connect-manifest-codegen/Makefile.connect register
```

> Note: use `pgrep -f 'java.*ConnectDistributed'` — without `java.*` you may match
> only the shell-wrapper PID and the JVM keeps running with the old jar.

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
