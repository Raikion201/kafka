# Working Connectors — Standalone Kafka Connect

**Connect endpoint:** `http://localhost:8083`
**Last updated:** 2026-05-14 (after imposter cleanup)
**Registered:** 55 credentialed (Airbyte manifests only — no local fixtures)

---

## Summary

| State | Count |
|---|---:|
| **RUNNING** (task green, polling) | **31** |
| FAILED — rate-limited (429) | 6 |
| FAILED — auth issue (401/403, credential side) | 5 |
| FAILED — codegen bug (NullKey, DynamicStream, query-param) | 4 |
| FAILED — upstream 5xx / connection reset | 7 |
| FAILED — 400 bad request | 3 |
| FAILED at register time (config validation) | 2 |

Working **right now** (RUNNING + transient): **~37**.

---

## RUNNING (31)

All tasks green, actively polling data.

| Connector | Notes |
|---|---|
| akeneo-connector | |
| alpha-vantage | |
| breezy-hr | |
| cal-com-connector | |
| chargebee | uses stream_slice + step (new codegen path) |
| clockify-connector | |
| coda-connector | |
| configcat-connector | |
| formbricks | `api_key`+`environment_id` |
| gmail-connector | OAuth2 refresh token |
| google-calendar | OAuth2 refresh token |
| google-classroom | OAuth2 refresh token |
| google-forms | OAuth2 refresh token |
| google-sheets-connector | OAuth2 refresh token |
| gutendex | Public API |
| launchdarkly | `access_token` |
| lemlist | `api_key` |
| linear | |
| lob | `api_key` (test env) |
| lokalise | `api_key` + `project_id` |
| mailerlite | `api_token` (JWT) |
| mux | |
| nasa-connector | Public API |
| onepagecrm | `username`+`password` |
| pokeapi | Public API (`pokemon_name=ditto`) |
| recruitee-connector | |
| scryfall | Public API |
| spacex-api | Public API |
| trello-connector | OAuth1 |
| us-census-connector | Public API |
| xkcd | Public API |

---

## FAILED — rate-limited (6)

Credentials work; APIs are throttling. Will return to RUNNING when retried.

| Connector | Symptom |
|---|---|
| aviationstack-connector | 429 |
| coinmarketcap-connector | 429 |
| hubplanner-connector | 429 |
| newsapi-connector | 429 |
| pipedrive-connector | 429 |
| the-guardian-api-connector | 429 |

---

## FAILED — auth issue (credential side, 5)

User action needed: regenerate token or use correct key type.

| Connector | Symptom | Action |
|---|---|---|
| appfollow | 401 | Token expired/wrong scope |
| buildkite | 403 | Token lacks `read_organizations` — regenerate with required scopes |
| coingecko-coins-connector | 401 | Pro API key needed for incremental endpoints |
| gnews-connector | 403 | Free-tier key may not include search endpoint |
| statuspage | 401 | Public page key supplied — needs management API key |

---

## FAILED — codegen bug (4)

| Connector | Result | Notes |
|---|---|---|
| airtable | DynamicStream stub | `DynamicDeclarativeStream` not implemented |
| google-analytics-data-api | DynamicStream stub | same |
| intercom | Jackson NullKey | Codegen bug serializing record key |
| shortcut | 400 | Codegen emits unsupported `includes_description=` query param |

---

## FAILED — upstream 5xx / connection reset (7)

Transient API-side issues. Restart usually recovers temporarily.

| Connector | Symptom |
|---|---|
| apptivo | 5xx |
| bitly-connector | connection reset |
| box-connector | upstream error |
| jira | upstream error |
| mixmax | 5xx |
| todoist-connector | upstream error |
| tvmaze-schedule | upstream error |

---

## FAILED — 400 bad request (3)

| Connector | Symptom |
|---|---|
| sentry-connector | 400 |
| shortcut | 400 (codegen — see above) |
| toggl-connector | 400 |

---

## FAILED at register time (2)

Config validation rejected — likely required field missing in credentials file.

| Connector | Symptom |
|---|---|
| illumina-connector | config invalid |
| typeform | config invalid (nested `credentials.access_token` codegen unwrap missing) |

---

## How to re-register after `make redeploy`

```bash
make -f connect-manifest-codegen/Makefile.connect register
```

Wait for Connect to be fully up (plugins loaded) before registering.

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

---

## Cleanup history

**2026-05-14:** Removed 3 imposter connector plugins from the codegen JAR
(`Rickandmorty`, `Jsonplaceholder`, `Zapier`) that were stale artifacts from
local test fixtures (`src/test/resources/test-fixtures/`), not real Airbyte
manifests. Also wiped 13 imposter entries from Connect's `connect-configs`
topic via topic delete + restart. Only the 522 real Airbyte manifests
(`src/test/resources/manifests/`) are now compiled into the plugin JAR.
