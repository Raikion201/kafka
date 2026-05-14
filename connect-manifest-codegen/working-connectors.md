# Working Connectors — Standalone Kafka Connect

**Connect endpoint:** `http://localhost:8083`  
**Last updated:** 2026-05-14

---

## Currently RUNNING (47 connectors)

All tasks green, actively polling data into Kafka topics.

| Connector | Notes |
|---|---|
| akeneo-connector | |
| alpha-vantage | |
| asana | |
| aws-cloudtrail | |
| breezy-hr | |
| cal-com-connector | |
| clockify-connector | |
| coda-connector | |
| configcat-connector | |
| defillama | |
| finnworlds | |
| flowlu | |
| gitlab | |
| gmail-connector | OAuth2 refresh token |
| google-calendar | OAuth2 refresh token |
| google-classroom | OAuth2 refresh token |
| google-forms | OAuth2 refresh token |
| google-sheets-connector | OAuth2 refresh token |
| harvest | |
| jina-ai-reader | |
| launchdarkly | `access_token` |
| lemlist | `api_key` |
| lob | `api_key` (test env) |
| lokalise | `api_key` + `project_id` |
| mixpanel | |
| nasa-connector | Public API |
| onepagecrm | |
| poplar | |
| pypi | Public API |
| recruitee-connector | |
| scryfall | Public API |
| serpstat | |
| spacex-api | Public API |
| stigg | |
| stripe | |
| surveymonkey | |
| the-guardian-api-connector | `api_key` |
| trello-connector | OAuth1 |
| twelve-data | |
| us-census-connector | Public API |
| weatherstack | |
| whisky-hunter | |
| xkcd | Public API |
| xsolla | |
| youtube-analytics | |
| linear | `api_key` |

---

## Rate-limited / transient errors

Connectors with valid credentials that fail only due to API rate limits or 429/500s:

| Connector | Error |
|---|---|
| appfollow | 401 — check `api_secret` field name vs manifest |
| chargebee | Likely needs valid site subdomain |
| mux | Likely needs valid token |
| sentry-connector | 500 from Sentry API |
| aviationstack-connector | 500/rate limit |
| box-connector | OAuth2 not configured |
| apptivo | 401 — placeholder creds |
| the-guardian-api | 429 rate limit (duplicate of -connector) |
| coinmarketcap-connector | 429/exhausted retries |
| coingecko-coins-connector | 429/exhausted retries |
| newsapi-connector | 429/exhausted retries |
| gutendex | 429 rate limit |
| bitly | 429/auth |

---

## Credentials registered but FAILED

| Connector | Error |
|---|---|
| jira | 404 `GET /rest/api/3/avatar/system` — ListPartitionRouter slice not injected into URL |
| google-analytics-data-api | Missing required config |
| pipedrive | 401 — placeholder creds expired |
| mixmax | 401 — check API key |

---

## Manifest directory

`src/test/resources/manifests/` now contains **517 real Airbyte source connector manifests** only.
Test fixtures live in `src/test/resources/test-fixtures/`.

---

## How to re-register after `make redeploy`

```bash
# Wait for Connect to be fully up (plugins loaded), then:
make -f connect-manifest-codegen/Makefile.connect register-all
```

The `redeploy` target sometimes causes Connect to fail the first wave of registrations
due to a timing race with `CachedConnectors`. Always wait for Connect to be fully ready
before running `register`.

---

## Adding new credentials

Create a file in `~/.kafka-connect-credentials/`:

```properties
# connector-myservice.properties
name=myservice
connector.class=io.kafka.connect.generated.MyserviceSourceConnector
tasks.max=1
topic.creation.default.replication.factor=1
topic.creation.default.partitions=1
api_key=YOUR_KEY_HERE
start_date=2024-01-01T00:00:00Z
```

Then run `make -f connect-manifest-codegen/Makefile.connect register`.
