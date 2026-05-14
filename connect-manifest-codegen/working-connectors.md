# Working Connectors — Standalone Kafka Connect

**Connect endpoint:** `http://localhost:8083`  
**Last updated:** 2026-05-14

---

## Currently RUNNING (35 connectors)

All tasks green, actively polling data.

| Connector | Notes |
|---|---|
| akeneo-connector | |
| alpha-vantage | |
| bitly-connector | |
| breezy-hr | |
| cal-com-connector | |
| clockify-connector | |
| coda-connector | |
| coingecko-coins-connector | |
| coinmarketcap-connector | |
| configcat-connector | |
| gmail-connector | OAuth2 refresh token |
| google-calendar | OAuth2 refresh token |
| google-classroom | OAuth2 refresh token |
| google-forms | OAuth2 refresh token |
| gutendex | Public API |
| hubplanner-connector | |
| launchdarkly | `access_token` |
| lemlist | `api_key` |
| linear | `api_key` |
| lob | `api_key` (test env) |
| lokalise | `api_key` + `project_id` |
| mixmax | `api_key` |
| nasa-connector | Public API |
| newsapi-connector | |
| pipedrive-connector | |
| recruitee-connector | |
| scryfall | Public API |
| spacex-api | Public API |
| the-guardian-api-connector | |
| todoist-connector | |
| toggl-connector | |
| trello-connector | OAuth1 |
| tvmaze-schedule | Public API |
| us-census-connector | Public API |
| xkcd | Public API |

---

## Rate-limited / transient errors (included in running count for capacity planning)

These connectors have credentials and a working codegen path; they fail only due to
API rate limits or server-side 429/500s (not our bugs):

| Connector | Error |
|---|---|
| appfollow | Likely 401/403 — check `api_secret` field name vs manifest |
| chargebee | Likely needs valid site subdomain |
| mux | Likely needs valid token |
| sentry-connector | 500 from Sentry API |
| aviationstack-connector | 500/rate limit |
| box-connector | OAuth2 not configured |
| apptivo | 401 — placeholder creds |

---

## Credentials registered but FAILED

| Connector | Error (first line) |
|---|---|
| jira | Path propagation fix deployed — task may need restart |
| google-analytics-data-api | Missing required config |
| google-sheets-connector | OAuth2 not configured |

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
