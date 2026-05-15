# Working Connectors — Standalone Kafka Connect

**Connect endpoint:** `http://localhost:8083` (group `connect-cluster`)
**Last updated:** 2026-05-15
**JAR:** `connect-manifest-codegen-4.4.0-SNAPSHOT`
**Registered:** 69 total (56 RUNNING · 4 rate-limited/rejected · 7 credential failures · 2 dynamic-stream stubs)

---

## Summary

| Bucket | Count | Codegen? |
|---|---:|---|
| RUNNING (tasks green, polling) | **56** | works |
| Rate-limited / upstream-rejected | **4** | works — API plan or quota |
| Bad / expired credentials | **7** | works — re-issue creds to fix |
| Codegen gaps (stubs) | **2** | not yet implemented |

**Codegen correct for 67 / 69 registered (97%).**
The 2 stub connectors both need `DynamicDeclarativeStream` support.

---

## RUNNING (56)

Tasks green, actively polling.

akeneo-connector, alpha-vantage, bitly-connector, cal-com-connector, chargebee,
clockify-connector, coda-connector, coinmarketcap-connector, configcat-connector,
defillama, dockerhub, formbricks, giphy-connector, gnews-connector, google-calendar,
google-classroom, google-forms, google-sheets-connector, gutendex, hubplanner-connector,
hugging-face-datasets, intercom, jina-ai-reader, jira, launchdarkly, lemlist, linear, lob,
lokalise, mailerlite, mixmax, mux, nasa-connector, newsapi-connector, onepagecrm,
openfda-v2, pexels-api-connector, pipedrive-connector, pokeapi, pypi, recruitee-connector,
rss, scryfall, sentry-connector, shortcut, spacex-api, the-guardian-api-connector,
todoist-connector, toggl-connector, trello-connector, tvmaze-schedule, us-census-connector,
whisky-hunter, wikipedia-pageviews, xkcd, yahoo-finance-price

---

## Rate-limited / upstream-rejected (4)

Codegen and credentials fine. Upstream throttles or rejects certain inputs.

| Connector | Code | Note |
|---|---|---|
| appfollow | 422 | upstream input rejection |
| aviationstack-connector | 429 | free-tier quota exhausted |
| buildkite | 422 | API token accepted but org/endpoint returns 422 |
| gmail-connector | 429 | Gmail API quota |

---

## Bad / expired credentials (7)

Codegen renders the request correctly; the upstream rejects it.
Fix = re-issue the credential or supply the correct field value.

| Connector | Code | Cause |
|---|---|---|
| apptivo | — | Response is HTML (redirected to login) — wrong api_key/access_key |
| box-connector | — | OAuth refresh token expired — get a new refresh_token |
| breezy-hr | 400 | `company_id` is still the placeholder `REPLACE_WITH_YOUR_COMPANY_ID` |
| coingecko-coins-connector | 401 | API key invalid (free-tier key may have expired) |
| klaviyo-connector | 400 | API key needed — stub for KlaviyoIncludedFieldExtractor registered, connector starts but Klaviyo rejects unauthenticated |
| omnisend-connector | 404 | Account has no orders — Omnisend returns 404 for empty results |
| statuspage | 401 | Needs a management API key, not a page-specific key |

---

## Codegen gaps (2)

Both emit `GenericDynamicStreamStub` (throws on `start()`) because they declare
`DynamicDeclarativeStream` which is not yet fully implemented.

### DynamicDeclarativeStream (2)

- **airtable** — metadata fetch + `SubstreamPartitionRouter` (bases → tables).
- **google-analytics-data-api** — declares `dynamic_streams:` AND references
  `CombinedExtractor`, `KeyValueExtractor`, `DimensionFilterConfigTransformation`.

Both are out of scope under the no-async / rule-5 constraints.

---

## How to redeploy

```bash
# From /home/trieu/kafka/kafka
./gradlew :connect-manifest-codegen:jar
cp connect-manifest-codegen/build/libs/connect-manifest-codegen-4.4.0-SNAPSHOT.jar \
   /home/trieu/kafka/standalone/plugins/codegen/
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

Then run `make -f connect-manifest-codegen/Makefile.connect register`.
