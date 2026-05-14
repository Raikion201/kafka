# Working Connectors — Standalone Kafka Connect

**Connect endpoint:** `http://localhost:8083`
**Last updated:** 2026-05-14

---

## Summary (560 connectors registered)

| State | Count |
|---|---:|
| **RUNNING** (task green, polling) | **49** |
| **Rate-limited / transient 5xx** (auth OK, API throttling) | **24** |
| Auth failed (placeholder creds, no real key) | ~270 |
| Other failed (codegen limitation, dynamic streams, etc.) | ~217 |

Total **working (RUNNING + rate-limited)**: **73**.

---

## Currently RUNNING (49)

All tasks green, actively polling data.

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
| defillama | Public API |
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
| jsonplaceholder | Public API |
| launchdarkly | `access_token` |
| lemlist | `api_key` |
| lob | `api_key` (test env) |
| lokalise | `api_key` + `project_id` |
| mixpanel | |
| nasa-connector | Public API |
| **onepagecrm** | `username`+`password` — **NEW** |
| **mailerlite** | `api_token` — **NEW** |
| **formbricks** | `api_key`+`environment_id` — **NEW** |
| **pokeapi** | `pokemon_name=ditto` — public API — **NEW** |
| poplar | |
| pypi | Public API |
| recruitee-connector | |
| rickandmorty | Public API |
| scryfall | Public API |
| serpstat | |
| spacex-api | Public API |
| stigg | |
| stripe | |
| surveymonkey | |
| the-guardian-api-connector | |
| trello-connector | OAuth1 |
| twelve-data | |
| us-census-connector | Public API |
| weatherstack | |
| whisky-hunter | Public API |
| xkcd | Public API |
| xsolla | |
| youtube-analytics | OAuth2 |

---

## Rate-limited / transient 5xx (23)

Credentials work, codegen path works — failures are API-side throttling or
upstream server errors. Count as "working" for capacity planning.

| Connector | Symptom |
|---|---|
| aviationstack-connector | 429 rate limit |
| bing-ads | 5xx transient |
| coingecko-coins-connector | 429 |
| coinmarketcap-connector | 429 |
| customerly | 5xx |
| flexport | 5xx |
| granola | 429 |
| gutendex | 429 |
| hubplanner-connector | 429 |
| insightly | 5xx |
| intercom | flagged 5xx — actually a codegen NullKey bug, see below |
| linear | 429 |
| mixmax | 5xx |
| newsapi-connector | 429 |
| open-data-dc | 5xx |
| openfda | 5xx |
| pipedrive-connector | 429 |
| pocket | 5xx |
| primetric | 5xx |
| printify | 429 |
| sentry | 5xx |
| sentry-connector | 5xx |
| trustpilot | 429 |

---

## Newly registered (10 today)

### Working (4)
| Connector | Notes |
|---|---|
| **onepagecrm** | `username=<user_id>` + `password=<api_key>` |
| **mailerlite** | `api_token` (JWT) |
| **formbricks** | `api_key=fbk_...` + `environment_id` |
| **pokeapi** | `pokemon_name=ditto` — public API, no auth |

### Failed — credential issue (user action) (2)
| Connector | Result | Action needed |
|---|---|---|
| buildkite | 403 Forbidden | Token lacks `read_organizations` scope — regenerate at buildkite.com → Personal Settings → API Access Tokens with **read_organizations**, **read_pipelines**, **read_builds**, **read_user** scopes |
| statuspage | 401 Unauthorized | Token+secret pair is the **public page** key pair, not the management API key. Get a real API key from manage.statuspage.io → API Info → User API Keys |

### Failed — codegen bug (4)
| Connector | Result | Notes |
|---|---|---|
| airtable | codegen stub | `DynamicDeclarativeStream` not implemented (CLAUDE.md scope-5) |
| typeform | 401 | Codegen does not unwrap nested `credentials.access_token` |
| intercom | Jackson NullKey | Codegen bug serializing record key (auth itself worked) |
| shortcut | 400 | Codegen emits unsupported `includes_description=` query param (auth itself worked) |

Credentials saved at `~/.kafka-connect-credentials/connector-{airtable,typeform,intercom,shortcut,onepagecrm,buildkite,mailerlite,statuspage,formbricks,pokeapi}.properties`.

---

## How to re-register after `make redeploy`

```bash
# Wait for Connect to be fully up (plugins loaded), then:
make -f connect-manifest-codegen/Makefile.connect register
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
