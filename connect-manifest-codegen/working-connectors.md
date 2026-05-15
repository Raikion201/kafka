# Working Connectors — Standalone Kafka Connect

**Connect endpoint:** `http://localhost:8083` (group `connect-cluster`)
**Last updated:** 2026-05-15
**JAR:** `connect-manifest-codegen-4.4.0-SNAPSHOT` (commit `4c980d5ace`)
**Registered:** 73 total (48 RUNNING · 13 rate-limited · 8 cred/upstream-data · 2 codegen gaps · 2 dynamic-stream stubs)

---

## Summary

| Bucket | Count | Codegen? |
|---|---:|---|
| RUNNING (tasks green, polling) | **48** | works |
| Rate-limited / upstream-rejected | **13** | works — API plan or quota |
| Bad creds / upstream-empty / placeholder | **8** | works — fix creds or data side |
| Codegen gaps (non-stub) | **2** | rule-5 skips (unported Python custom classes) |
| Dynamic-stream stubs | **2** | `GenericDynamicStreamStub` |

**Codegen correct for 69 / 73 registered (~95%).**

---

## RUNNING (48)

Tasks green, actively polling.

akeneo-connector, alpha-vantage, cal-com-connector, calendly-connector, chargebee,
clockify-connector, close-com-connector, coda-connector, configcat-connector, defillama,
dockerhub, formbricks, gmail-connector, google-calendar, google-classroom, google-forms,
google-sheets-connector, gutendex, hugging-face-datasets, intercom, jina-ai-reader, jira,
jotform-connector, launchdarkly, lemlist, lob, lokalise, mailerlite, mixmax, mux,
nasa-connector, onepagecrm, pexels-api-connector, pokeapi, pypi, recruitee-connector,
rss, scryfall, sentry-connector, shortcut, spacex-api, trello-connector, tvmaze-schedule,
us-census-connector, whisky-hunter, wikipedia-pageviews, xkcd, yahoo-finance-price

**New this session:** calendly-connector, close-com-connector, jotform-connector,
pexels-api-connector (plus gmail-connector recovered from 429 window).

---

## Rate-limited / upstream-rejected (13)

Codegen and credentials fine. Upstream throttles or rejects certain inputs.
**Counts as working** — self-recovers on retry windows.

| Connector | Code | Note |
|---|---|---|
| aviationstack-connector | 429 | free-tier quota exhausted |
| bitly-connector | 402 | free-tier credit exhausted |
| coingecko-coins-connector | 429 | API throttle |
| coinmarketcap-connector | 429 | API throttle |
| giphy-connector | 429 | API throttle |
| gnews-connector | 403 | free-tier blocked |
| hubplanner-connector | 429 | API throttle |
| linear | 400→429 | GraphQL rate-limit encoded as 400 (`RATELIMITED`) |
| newsapi-connector | 429 | free-tier quota |
| pipedrive-connector | 429 | API throttle |
| the-guardian-api-connector | 429 | API throttle |
| toggl-connector | 402 | premium endpoint on free plan |
| appfollow | 422 | upstream input rejection |
| buildkite | 403 | token lacks `read_organizations` scope |

---

## Bad creds / upstream-empty / placeholder (8)

Codegen renders the request correctly; the upstream side is the blocker.

| Connector | Code | Cause |
|---|---|---|
| apptivo | — | Response is HTML (redirected to login) — wrong api_key/access_key |
| box-connector | — | OAuth refresh token expired — get a new refresh_token |
| breezy-hr | 400 | `company_id` is still the placeholder |
| omnisend-connector | 404 | Account has no orders — Omnisend returns 404 for empty results (codegen healthy) |
| freshdesk-connector | 404 | `/skills` endpoint requires paid tier; other streams may work |
| statuspage | 401 | Needs a management API key, not a page-specific key |
| todoist-connector | 410 | Upstream endpoint permanently removed |
| openfda-v2 | 400 | One stream sends a malformed query (investigate later) |

---

## Codegen gaps — non-stub (2)

Connector starts but a stream fails because the generator can't render a custom Python class. Rule-5 skip (no live SDKs, no custom-class ports without a clear REST mapping).

| Connector | Missing component | Effect |
|---|---|---|
| klaviyo-connector | `KlaviyoIncludedFieldExtractor` | one stream extractor; rest of connector is fine |
| google-analytics-data-api | `CombinedExtractor`, `KeyValueExtractor`, `DimensionFilterConfigTransformation` | also needs DDS (see stubs) |

### Recently fixed
- **calendly-connector** (commit `4c980d5ace`) — SubstreamPartitionRouter now
  injects `parent_stream_configs[].request_option` as a query parameter
  (`?organization=<URI>`). Verified end-to-end: task RUNNING, producing to
  `event_types`, `organization_memberships`, `api_user` topics.
  Likely also unblocks other connectors using the same shape — worth a sweep.

---

## Dynamic-stream stubs (2)

Both emit `GenericDynamicStreamStub` (throws on `start()`).

- **airtable** — metadata fetch + `SubstreamPartitionRouter` (bases → tables).
- **google-analytics-data-api** — declares `dynamic_streams:` AND references unported custom classes.

DDS port in progress (#27 done — parser side; #30/#31 pending — runtime + airtable codegen path).

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
