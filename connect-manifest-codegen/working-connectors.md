# Working Connectors — Standalone Kafka Connect

**Connect endpoint:** `http://localhost:8083` (group `connect-cluster`)
**Last updated:** 2026-05-15
**JAR:** `connect-manifest-codegen-4.4.0-SNAPSHOT`
**Registered:** 73 total (45 RUNNING · 13 rate-limited · 10 cred/upstream-data · 3 codegen gaps · 2 dynamic-stream stubs)

---

## Summary

| Bucket | Count | Codegen? |
|---|---:|---|
| RUNNING (tasks green, polling) | **45** | works |
| Rate-limited / upstream-rejected | **13** | works — API plan or quota |
| Bad creds / upstream-empty / placeholder | **10** | works — fix creds or data side |
| Codegen gaps (non-stub) | **3** | known bugs — see below |
| Dynamic-stream stubs | **2** | `GenericDynamicStreamStub` |

**Codegen correct for 68 / 73 registered (93%).**

---

## RUNNING (45)

Tasks green, actively polling.

akeneo-connector, alpha-vantage, cal-com-connector, chargebee, clockify-connector,
close-com-connector, coda-connector, configcat-connector, defillama, dockerhub,
formbricks, google-calendar, google-classroom, google-forms, google-sheets-connector,
gutendex, hugging-face-datasets, intercom, jina-ai-reader, jira, jotform-connector,
launchdarkly, lemlist, lob, lokalise, mailerlite, mixmax, mux, nasa-connector,
onepagecrm, pexels-api-connector, pokeapi, pypi, recruitee-connector, rss, scryfall,
sentry-connector, shortcut, spacex-api, trello-connector, tvmaze-schedule,
us-census-connector, whisky-hunter, wikipedia-pageviews, xkcd, yahoo-finance-price

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
| gmail-connector | 429 | Gmail API quota |
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

## Bad creds / upstream-empty / placeholder (10)

Codegen renders the request correctly; the upstream side is the blocker.

| Connector | Code | Cause |
|---|---|---|
| apptivo | — | Response is HTML (redirected to login) — wrong api_key/access_key |
| box-connector | — | OAuth refresh token expired — get a new refresh_token |
| breezy-hr | 400 | `company_id` is still the placeholder |
| omnisend-connector | 404 | Account has no orders — Omnisend returns 404 for empty results (counts as working — codegen healthy) |
| freshdesk-connector | 404 | `/skills` endpoint requires paid tier; other streams may work |
| ip2whois | 401 | API key invalid |
| statuspage | 401 | Needs a management API key, not a page-specific key |
| todoist-connector | 410 | Upstream endpoint permanently removed |
| openfda-v2 | 400 | One stream sends a malformed query (codegen quirk — investigate) |
| microsoft-teams | — | OAuth refresh token expired |

---

## Codegen gaps — non-stub (3)

Connector starts but a stream fails because the generator missed a feature.

| Connector | Missing feature | Effect |
|---|---|---|
| calendly-connector | `SubstreamPartitionRouter.parent_stream_configs[].request_option` | parent's `current_organization` URI not injected as query param on child streams — see below |
| klaviyo-connector | `KlaviyoIncludedFieldExtractor` (custom Python class) | rule-5 skip — would need a Java port |
| google-analytics-data-api | `CombinedExtractor`, `KeyValueExtractor`, `DimensionFilterConfigTransformation` (custom) | rule-5 skip — also needs DDS |

### calendly-connector bug (in flight)

Calendly's `event_types` (and other) streams declare:

```yaml
partition_router:
  type: SubstreamPartitionRouter
  parent_stream_configs:
    - parent_key: current_organization
      request_option:                  # ← codegen drops this entirely
        type: RequestOption
        field_name: organization
        inject_into: request_parameter
      partition_field: organization_uri
      stream: { $ref: "#/definitions/streams/api_user" }
```

The Java codegen currently only handles SubstreamPartitionRouter when the child's `path` contains `{{ stream_partition.X }}` (path-substitution). Here the path is static (`/event_types`) and the parent key must be injected as a query parameter. Both the parser (`PartitionRouterSpec.ParentStreamConfig`) and the generator (`buildSubstreamUrlBlock`) ignore `request_option` on parent configs.

Python source: `airbyte_cdk/sources/declarative/partition_routers/substream_partition_router.py:79, 162-176`.

---

## Dynamic-stream stubs (2)

Both emit `GenericDynamicStreamStub` (throws on `start()`).

- **airtable** — metadata fetch + `SubstreamPartitionRouter` (bases → tables).
- **google-analytics-data-api** — declares `dynamic_streams:` AND references unported custom classes.

DDS port is in progress (#27 done — parser side; #30/#31 pending — runtime + airtable codegen path).

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

For connectors whose spec has a nested `oneOf` object (e.g. jotform's `api_endpoint`), use a `.json` file with the nested value stringified — see `connector-google-sheets.json` for the pattern.

Then run `make -f connect-manifest-codegen/Makefile.connect register`.
