# Filling Queue — Connectors Awaiting Credentials

**Last updated:** 2026-05-25
**Manifest coverage:** 522 / 522 Airbyte declarative-manifest sources present locally (100%).
The 76 Airbyte sources we don't have are pure-Python / JDBC / file-blob / vendor-SDK / GraphQL
hybrids — not codegen-eligible. See "Audit summary" at bottom.

For each candidate below: Rule-5 clean, self-serve free tier, no credit card.
Drop the JSON into `~/.kafka-connect-credentials/`. When done, list which ones you
filled — Kafka Connect + broker are currently STOPPED and will be started only to
register the named ones (Rule 7).

---

## Queue (10)

### 1. onesignal

1. Sign up at https://onesignal.com → **Sign Up** (top right; no card)
2. Dashboard → **New App/Website** → pick **Web Push** → name it → **Save & Continue**
3. Sidebar: **Settings → Keys & IDs** → copy **OneSignal App ID** and **REST API Key**
4. Top-right profile icon → **Account & API Keys** → copy **User Auth Key**

```json
{
  "name": "onesignal",
  "config": {
    "connector.class": "io.kafka.connect.generated.OnesignalSourceConnector",
    "tasks.max": "1",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "1",
    "user_auth_key": "PASTE_USER_AUTH_KEY",
    "applications": "[{\"app_name\":\"test\",\"app_id\":\"PASTE_APP_ID\",\"app_api_key\":\"PASTE_REST_API_KEY\"}]",
    "outcome_names": "os__session_duration.count,os__click.count",
    "start_date": "2026-01-01T00:00:00Z"
  }
}
```

### 2. plaid

1. Sign up at https://dashboard.plaid.com/signup (no card for Sandbox env)
2. Choose **"I'm a developer"** when prompted
3. Verify email → land in dashboard
4. Top of page: environment dropdown = **Sandbox**
5. Sidebar: **Team Settings → Keys** → copy **client_id** and **Sandbox Secret**

```json
{
  "name": "plaid",
  "config": {
    "connector.class": "io.kafka.connect.generated.PlaidSourceConnector",
    "tasks.max": "1",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "1",
    "client_id": "PASTE_CLIENT_ID",
    "api_key": "PASTE_SANDBOX_SECRET",
    "plaid_env": "sandbox",
    "access_token": "access-sandbox-PASTE",
    "start_date": "2026-01-01"
  }
}
```
*Note: Plaid also needs an `access_token` from a one-time sandbox `/link/token/create` → `/item/public_token/exchange` call. Walk-through provided at registration time.*

### 3. sendgrid

1. Sign up at https://signup.sendgrid.com (no card; free 100/day forever)
2. Verify email → skip onboarding wizard
3. Sidebar: **Settings → API Keys** → **Create API Key**
4. Name `kafka-connect`, **Full Access**, **Create & View**
5. Copy the key (starts with `SG.` — **shown once**)

```json
{
  "name": "sendgrid",
  "config": {
    "connector.class": "io.kafka.connect.generated.SendgridSourceConnector",
    "tasks.max": "1",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "1",
    "api_key": "SG.PASTE_KEY",
    "start_date": "2026-01-01T00:00:00Z"
  }
}
```

### 4. sparkpost

1. Sign up at https://app.sparkpost.com/sign-up (no card; free 500/month)
2. Pick **US** or **EU** region (remember which)
3. Verify email
4. Sidebar (gear icon, bottom): **Configuration → API Keys** → **New API Key**
5. Name `kafka-connect`, default read perms, **Save**
6. Copy the key (shown once)

```json
{
  "name": "sparkpost",
  "config": {
    "connector.class": "io.kafka.connect.generated.SparkpostSourceConnector",
    "tasks.max": "1",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "1",
    "api_key": "PASTE_KEY",
    "api_prefix": "api",
    "start_date": "2026-01-01T00:00:00Z"
  }
}
```
*Change `"api"` → `"api.eu"` for EU accounts.*

### 5. drift

1. Sign up at https://www.drift.com → **Get Drift Free** (no card; personal email OK)
2. Skip onboarding
3. Bottom-left gear icon → **App Settings → Integrations → Drift API**
4. Click **Connect** / **Get Token** → copy the **OAuth Access Token**

```json
{
  "name": "drift",
  "config": {
    "connector.class": "io.kafka.connect.generated.DriftSourceConnector",
    "tasks.max": "1",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "1",
    "credentials": "{\"credentials\":\"access_token\",\"access_token\":\"PASTE_TOKEN\"}"
  }
}
```

### 6. svix

1. Sign up at https://dashboard.svix.com/login (no card; free 10K msgs/mo)
2. Sign in with GitHub / Google / email
3. Top-right environment dropdown → **Development**
4. Sidebar: **API Access → Auth Tokens** → **Create Auth Token** → name → **Create**
5. Copy token (`testsk_xxx`, or `eu.testsk_xxx` for EU)

```json
{
  "name": "svix",
  "config": {
    "connector.class": "io.kafka.connect.generated.SvixSourceConnector",
    "tasks.max": "1",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "1",
    "api_key": "PASTE_TOKEN",
    "start_date": "2026-01-01T00:00:00Z"
  }
}
```

### 7. vantage

1. Sign up at https://console.vantage.sh/signup (no card)
2. **Skip** the "connect a cloud provider" prompt
3. Top-right profile → **Personal Settings**
4. Left tab: **Personal Access Tokens** → **Create Access Token** → default scopes → **Create**
5. Copy token (starts with `vntg_tkn_`)

```json
{
  "name": "vantage",
  "config": {
    "connector.class": "io.kafka.connect.generated.VantageSourceConnector",
    "tasks.max": "1",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "1",
    "access_token": "vntg_tkn_PASTE"
  }
}
```

### 8. zapsign

1. Sign up at https://app.zapsign.co/registrar (no card; 5 free signatures/mo)
2. Verify email
3. (Optional) Top-right profile → **Idioma → English**
4. Top-right profile → **Settings** (or **Configurações**) → **API** tab
5. Copy the **API Token**

```json
{
  "name": "zapsign",
  "config": {
    "connector.class": "io.kafka.connect.generated.ZapsignSourceConnector",
    "tasks.max": "1",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "1",
    "api_token": "PASTE_TOKEN",
    "start_date": "2026-01-01T00:00:00Z"
  }
}
```

### 9. simfin

1. Sign up at https://app.simfin.com/login/register (no card)
2. Verify email
3. Top-right username → **Account Settings** → **API Key** section
4. Copy the key (free tier = US data, daily refresh)

```json
{
  "name": "simfin",
  "config": {
    "connector.class": "io.kafka.connect.generated.SimfinSourceConnector",
    "tasks.max": "1",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "1",
    "api_key": "PASTE_KEY"
  }
}
```

### 10. wufoo

1. Sign up at https://www.wufoo.com/sign-up → **Free plan** (no card)
2. Choose subdomain (e.g. `mytest` → `mytest.wufoo.com`)
3. Verify email
4. Top-right username → **Account → API Information**
   (or visit `https://YOUR-SUBDOMAIN.wufoo.com/account/api/`)
5. Copy the **API Key**

```json
{
  "name": "wufoo",
  "config": {
    "connector.class": "io.kafka.connect.generated.WufooSourceConnector",
    "tasks.max": "1",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "1",
    "api_key": "PASTE_KEY",
    "subdomain": "your-subdomain"
  }
}
```

---

## Workflow

1. Fill the JSONs you can in `~/.kafka-connect-credentials/`
2. Tell me which ones are ready
3. I start Kafka Connect + broker
4. Register only the named ones via `make -f Makefile.connect register ONLY=foo,bar`
5. Watch task status + topic for records
6. Record outcomes in `working-connectors.md` (RUNNING / quiet / failed / unfillable)
7. Stop Connect + broker, clear KRaft storage (Rule 7)

---

## Audit summary (2026-05-25)

Cross-checked our 522 local manifests against `airbytehq/airbyte` master.

| Bucket | Count |
|---|---:|
| Airbyte source connectors total | 598 |
| Airbyte sources WITH declarative `manifest.yaml` | **522** |
| Local manifests in `src/test/resources/manifests/` | **522** |
| **Missing-but-addable** | **0** |
| **Missing Rule-5 violations** | **0** |
| Python-only (no manifest, not codegen-eligible) | 76 |

**Manifest set is a complete mirror of Airbyte's declarative surface.** Nothing
to add to codegen. The remaining 76 Airbyte sources are either:

- Database / warehouse connectors (postgres, mysql, snowflake, bigquery,
  mongodb-v2, etc.) — JDBC/CDC, not HTTP
- File / blob readers (s3, gcs, azure-blob, sftp, google-drive, etc.) —
  not HTTP request/response
- Test scaffolds (e2e-test, faker, datagen) — not real sources
- Pure-Python custom-logic sources (facebook-marketing, github GraphQL,
  shopify, salesforce-bulk, netsuite, zuora, dv-360, etc.) — Rule-5 by design

The credentialing queue above is the entire forward path: each row is one of
the existing 522 manifests that hasn't been credentialed yet.

---

## See also

- [`working-connectors.md`](working-connectors.md) — full registry of RUNNING / failed / unfillable connectors
- [`CLAUDE.md`](CLAUDE.md) — Rule 5 (codegen scope), Rule 6 (unfillable tracking), Rule 7 (start/stop discipline)
