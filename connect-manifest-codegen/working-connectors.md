# Working Connectors — Standalone Kafka Connect

**Connect endpoint:** `http://localhost:8083` (group `connect-cluster`)
**Last updated:** 2026-05-19
**JAR:** `connect-manifest-codegen-4.4.0-SNAPSHOT`
**Registered:** 118 total (116 RUNNING · 1 codegen gap · 1 OAuth expired)

---

## Summary

| Bucket | Count | Codegen? |
|---|---:|---|
| RUNNING (tasks green, polling) | **116** | works |
| OAuth expired / needs re-authorization | **1** | works — re-issue refresh token |
| Codegen gaps (non-stub) | **1** | rule-5 skip (unported Python custom class) |
| Dynamic-stream stubs | **0** | all DDS connectors now generate real task code |

**Codegen correct for 117 / 118 registered (~99%). 17 new connectors credentialed today — 10 producing real records, 7 RUNNING but quiet pending vendor config (see notes).**

---

## RUNNING (116)

Tasks green, actively polling.

activecampaign, airtable, akeneo-connector, algolia, alpha-vantage, amplitude, appfollow,
apptivo, asana, assemblyai, aviationstack-connector, bamboo-hr, beamer, bigmailer,
bitly-connector, boldsign, box-connector, breezy-hr, brevo, bugsnag, buildkite,
cal-com-connector, calendly-connector, chargebee, chartmogul, clockify-connector,
close-com-connector, coda-connector, coingecko-coins-connector, coinmarketcap-connector,
configcat-connector, defillama, dockerhub, emailoctopus, eventbrite, exchange-rates,
finnhub, formbricks, freshdesk-connector, freshsales, giphy-connector, gitlab,
gmail-connector, gnews-connector, google-calendar, google-classroom, google-forms, harvest,
google-sheets-connector, gutendex, hubplanner-connector, hugging-face-datasets, intercom,
ip2whois, jina-ai-reader, jira, jotform-connector, judge-me-reviews, klaviyo-connector,
launchdarkly, lemlist, linear, lob, lokalise, mailerlite, mailersend, mailosaur,
marketstack, mixmax, mux, nasa-connector, newsapi-connector, newsdata-io, nytimes,
omnisend-connector, onepagecrm, openaq, openfda, openfda-v2, openweather,
pexels-api-connector, pingdom, pipedrive-connector, plausible, pokeapi, polygon-stock-api,
postmarkapp, pypi, recruitee-connector, revenuecat, rollbar, rss, savvycal, scryfall,
sentry-connector, shippo, shortcut, spacex-api, square, statuspage, stockdata,
the-guardian-api-connector, ticketmaster, tmdb, todoist-connector, toggl-connector,
trello-connector, tvmaze-schedule, typeform, us-census-connector, watchmode, weatherstack,
whisky-hunter, wikipedia-pageviews, xkcd, yahoo-finance-price

**New connectors credentialed this session (2026-05-18 evening):**

| Connector | Records | Notes |
|---|---:|---|
| finnhub | 5,042,500 | stock_symbols (5.0M) + marketnews (16.6K) |
| ticketmaster | 64,223 | events 52.6K · attractions/venues 5.2K each · suggest 1.2K |
| openaq | 23,996 | 12 streams; `country_ids=[840]` filter applied |
| tmdb | 11,290 | 17 movie sub-streams |
| emailoctopus | 10,407 | lists stream |
| marketstack | 6,700 | exchanges only (other streams need paid plan) |
| postmarkapp | 2,312 | Server-Token endpoints only — account-level may 401 (same UUID used for both tokens) |
| nytimes | 800 | only Most Popular API enabled on the key |
| weatherstack | 36 | free tier: forecast + current_weather only (no historical) |
| exchange-rates | 4+ | manifest patched to hit `api.exchangeratesapi.io/v1/latest` with `access_key` query param (was the wrong apilayer.com endpoint) — EUR base, 168 currencies |

**New connectors credentialed this session (2026-05-19 morning) — fresh standalone after `data/` + `logs/` wipe:**

| Connector | Records | Notes |
|---|---:|---|
| watchmode | 883,343 | 8 movie/show streams — biggest haul of the batch |
| openfda | 397,098 | 5 of 9 streams active; needs `producer.override.max.request.size=10485760` (drug-labelling records exceed 1MB default) |
| eventbrite | 16,645 | 2 streams (events, categories) |
| shippo | 6,826 | 2 streams |
| revenuecat | 1,432 | 8 streams (Project API key, read scope) |
| ip2whois | 500 | single `domain=www.google.com` lookup loop — hits free-tier 500/mo cap |
| gitlab | 0 | RUNNING but quiet — `groups_list=["gitlab-org"]` not parsing into scan scope; try `groups=gitlab-org` |
| stockdata | 0 | RUNNING but quiet — `symbols=["AAPL","TSLA","MSFT"]` array not flowing through to API call |
| plausible | 0 | RUNNING but quiet — site_id `airbyte.com` doesn't belong to the user's account; needs a real owned domain |
| rollbar | 0 | RUNNING but quiet — same token used for both project + account; project-scoped streams 404. Needs a real Project Access Token alongside the Account Access Token |

**New connectors credentialed this session (2026-05-19 afternoon):**

| Connector | Notes |
|---|---|
| amplitude | BasicHttp (api_key + secret_key) — 6 streams |
| beamer | Bearer — 1 stream (nps) |
| bigmailer | ApiKey header (X-API-Key) — 10 streams |
| boldsign | ApiKey header (X-API-KEY) — 8 streams |
| typeform | Bearer (`credentials.access_token`) |
| harvest | Bearer (`credentials.api_token` + `account_id=2208984`) |
| square | Bearer (`credentials.api_key`, `is_sandbox=true`) |
**Earlier batch (still green):** activecampaign, bugsnag, assemblyai, algolia, asana,
bamboo-hr, freshsales, chartmogul, brevo — straight-through after codegen fixes
(brevo epoch format, bamboo-hr date format, gnews RFC-822 parse, per-stream error isolation).
- **algolia** — `logs` stream skipped (requires Logs ACL API key).
- **appfollow** — `app_lists` stream skipped (422 upstream).
- **bamboo-hr** — `timesheet_entries` skipped (Time Tracking not enabled on trial); `start_date=2025-05-18`.

---
---

## FAILED (2)

| Connector | State | Reason |
|---|---|---|
| google-analytics-data-api | Task FAILED | OAuth refresh token expired — re-issue via Google Cloud Console. Codegen is correct. |
| notion | Task FAILED | `NotionUserTransformation` custom Python class not yet ported. Rule-5 skip. |

---

## Stream-named topics

Each stream writes to a Kafka topic named after the stream (not the connector). To consume:

```bash
# List which topics a connector is writing to
curl http://localhost:8083/connectors/brevo/topics | python3 -m json.tool

# Consume from a specific stream topic
~/kafka/standalone/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic senders \
  --from-beginning --max-messages 5 --timeout-ms 5000
```

The `make sample` command iterates ALL connectors sequentially (89 × multiple topics × 5 s timeout per topic) and can take 10–15 minutes. For a single connector use direct topic consumption above.

**Note:** Non-windowed incremental streams (e.g., brevo `contacts`) re-emit the last record on every poll because Brevo's `modifiedSince` filter is inclusive — this is expected Airbyte at-least-once delivery semantics. Deduplicate on record `id` in your consumers.

---

## Codegen notes — known stream skips

Streams skipped via per-stream error isolation (task stays RUNNING, stream data absent):

| Connector | Skipped stream | Reason |
|---|---|---|
| appfollow | `app_lists` | 422 — upstream input rejection |
| bamboo-hr | `timesheet_entries` | 403 — Time Tracking not enabled on trial |
| algolia | `logs` | 403 — requires Logs ACL API key |
| brevo | `contacts_filters` | 400 — no filters configured |
| brevo | `webhooks` | 400 — no webhooks configured |
| marketstack | `tickers`, `currencies`, `eod`, `dividends`, `splits`, `intraday` | 401 — free plan only authorizes `exchanges` |
| nytimes | books, archive, articles, etc. | per-stream 401 — each API must be enabled on the developer portal |
| weatherstack | `historical`, `location_lookup` | free plan blocks historical + autocomplete |
| postmarkapp | account-level (`servers`, `domains`, `senders`, ...) | 401 — same UUID used for both Server-Token and Account-Token; need a real account token from `https://account.postmarkapp.com/account/edit` |

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
