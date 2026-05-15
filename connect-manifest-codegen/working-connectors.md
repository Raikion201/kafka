# Working Connectors — Standalone Kafka Connect

**Connect endpoint:** `http://localhost:8083` (group `connect-cluster`)
**Last updated:** 2026-05-15
**JAR:** `connect-manifest-codegen-4.4.0-SNAPSHOT`
**Registered:** 71 total (42 RUNNING · 15 rate-limited · 10 credential failures · 4 dynamic-stream stubs)

---

## Summary

| Bucket | Count | Codegen? |
|---|---:|---|
| RUNNING (tasks green, polling) | **42** | works |
| Rate-limited / upstream-tier | **15** | works — API plan or quota |
| Bad / expired credentials | **10** | works — re-issue creds to fix |
| Codegen gaps (stubs) | **4** | not yet implemented |

**Codegen correct for 67 / 71 registered (94%).**  
The 4 stub connectors split between two gaps: `DynamicDeclarativeStream` (1
true case + 1 hybrid) and unported Python `class_name:` components (2 cases).

---

## RUNNING (42)

Tasks green, actively polling.

akeneo-connector, alpha-vantage, cal-com-connector, chargebee, clockify-connector,
coda-connector, configcat-connector, defillama, dockerhub, formbricks,
google-calendar, google-classroom, google-forms, hugging-face-datasets,
intercom, intercom-v2, jina-ai-reader, jira, launchdarkly, lemlist, lob,
lokalise, mailerlite, mux, nasa-connector, onepagecrm, openfda-v2, pokeapi,
pypi, recruitee-connector, rss, scryfall, sentry-connector, shortcut,
spacex-api, trello-connector, tvmaze-schedule, us-census-connector,
whisky-hunter, wikipedia-pageviews, xkcd, yahoo-finance-price

---

## Rate-limited / upstream-tier (15)

Codegen and credentials both fine. Upstream is throttling or requires a paid tier.
**Counts as working** — will self-recover on retry windows.

| Connector | Code | Note |
|---|---|---|
| aviationstack-connector | 429 | free-tier quota |
| coingecko-coins-connector | 429 | API throttle |
| coinmarketcap-connector | 429 | API throttle |
| gmail-connector | 429 | Gmail API quota |
| google-pagespeed-insights | 429 | API quota |
| google-sheets-connector | 429 | Sheets API quota |
| gutendex | 429 | API throttle |
| hubplanner-connector | 429 | API throttle |
| mixmax | 429 | API throttle |
| newsapi-connector | 429 | free-tier quota |
| pipedrive-connector | 429 | API throttle |
| the-guardian-api-connector | 429 | API throttle |
| appfollow | 422 | upstream input rejection |
| bitly-connector | 402 | free-tier credit exhausted |
| toggl-connector | 402 | premium endpoint on free plan |

---

## Bad / expired credentials (10)

Codegen renders the request correctly; the upstream rejects it.
Fix = re-issue the credential or supply the correct field value.

| Connector | Code | Cause |
|---|---|---|
| box-connector | — | OAuth refresh token expired — get a new refresh_token |
| microsoft-teams | — | OAuth refresh token expired — get a new refresh_token |
| apptivo | — | Response is HTML (redirected to login) — wrong api_key/access_key |
| breezy-hr | 400 | `company_id` is still the placeholder `REPLACE_WITH_YOUR_COMPANY_ID` |
| buildkite | 403 | API token lacks `read_organizations` scope |
| gnews-connector | 403 | API key invalid or free-tier blocked |
| ip2whois | 401 | API key invalid |
| statuspage | 401 | Needs a management API key, not a page-specific key |
| todoist-connector | 410 | Upstream endpoint permanently removed |
| linear | 400 | GraphQL rate-limit encoded as 400 (`RATELIMITED` extension code) |

---

## Codegen gaps (4)

All four currently emit `GenericDynamicStreamStub` (throws on `start()`)
because the parser drops every stream it can't render and falls through to
the stub path. Real blocker varies:

### DynamicDeclarativeStream (1)

Manifest declares streams whose names/paths come from a runtime API call;
needs the `DynamicDeclarativeStream` + `HttpComponentsResolver` runtime
machinery (currently only google-sheets shape is implemented).

- **airtable** — adds a nested `SubstreamPartitionRouter` (bases → tables)
  on top of the DDS metadata fetch.

### DDS + unported Python `class_name:` components (1)

- **google-analytics-data-api** — declares `dynamic_streams:` AND
  references `CombinedExtractor`, `KeyValueExtractor`,
  `DimensionFilterConfigTransformation`. Even with DDS support, the custom
  classes would still need to be ported (out of scope per current
  directive).

### Unported Python `class_name:` components only (2)

These don't actually use `DynamicDeclarativeStream`; they hit the stub
because every stream in the manifest references an unregistered Python
custom class, so the parser drops them all and the generator falls through:

| Connector | Missing class |
|---|---|
| public-apis | `source_public_apis.components.CustomExtractor` |
| zenloop | `source_zenloop.components.ZenloopPartitionRouter` |

These are out of scope per the "skip custom classes" directive.

---

## How to redeploy

```bash
# From /home/trieu/kafka/kafka
./gradlew :connect-manifest-codegen:jar
cp connect-manifest-codegen/build/libs/connect-manifest-codegen-4.4.0-SNAPSHOT.jar \
   /home/trieu/kafka/standalone/plugins/codegen/
kill $(pgrep -f 'ConnectDistributed.*standalone/')
LOG_DIR=/home/trieu/kafka/standalone/logs \
KAFKA_HEAP_OPTS='-Xms256M -Xmx2G' \
  nohup /home/trieu/kafka/standalone/bin/connect-distributed.sh \
    /home/trieu/kafka/standalone/config/connect-distributed.properties \
    > /home/trieu/kafka/standalone/logs/stdout.log 2>&1 &
# Wait ~15s, then:
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

Then run `make -f connect-manifest-codegen/Makefile.connect register`.
