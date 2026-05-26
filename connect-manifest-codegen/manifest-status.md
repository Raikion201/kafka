# Manifest Status Tracker

**Last updated:** 2026-05-26
**Manifests in local set:** 522 (100% of Airbyte declarative sources)

| Status | Count | % |
|---|---:|---:|
| RUNNING (registered, polling) | 169 | 32.4 |
| UNFILLABLE (manifest exists but vendor blocks self-serve) | 15 | 2.9 |
| FAILED (registered but task error) | 2 | 0.4 |
| UNFILLED (no credentials attempted yet) | 336 | 64.4 |
| **Total** | **522** | **100.0** |

Plus 6 unfillable sources NOT in our manifest set (no Airbyte declarative manifest): discord, figma, github, heroku, shopify, supabase.

See [`working-connectors.md`](working-connectors.md) for per-connector details (record counts, session notes, specific errors). See [`filling-connector.md`](filling-connector.md) for the current credentialing queue.

---

## RUNNING (169)

Tasks registered and polling. Some have caveats (OAuth expired, account empty,
rule-5 stub on individual streams) — see `working-connectors.md` for details.

activecampaign, airtable, akeneo, algolia, alpha-vantage, amazon-ads, amazon-seller-partner, amplitude
appfollow, apptivo, asana, assemblyai, auth0, aviationstack, babelforce, bamboo-hr
basecamp, beamer, bigmailer, bitly, boldsign, box, breezy-hr, brevo
bugsnag, buildkite, buzzsprout, cal-com, calendly, canny, chargebee, chartmogul
clockify, close-com, coda, coingecko-coins, coinmarketcap, configcat, convertkit, customer-io
defillama, dockerhub, drift, dropbox-sign, easypost, emailoctopus, eventbrite, exchange-rates
fillout, finnhub, formbricks, freshdesk, freshsales, fulcrum, gainsight-px, giphy
gitbook, gitlab, gmail, gnews, google-calendar, google-classroom, google-forms, google-sheets
grafana, gutendex, harvest, hibob, hubplanner, hugging-face-datasets, huntr, imagga
insightly, intercom, ip2whois, jina-ai-reader, jira, jotform, judge-me-reviews, klarna
klaviyo, launchdarkly, lemlist, linear, lob, lokalise, mailchimp, mailerlite
mailersend, mailosaur, mailtrap, marketo, marketstack, microsoft-teams, mixmax, mux
nasa, news-api, newsdata-io, nytimes, omnisend, onepagecrm, onesignal, openaq
open-exchange-rates, openfda, openweather, partnerize, persistiq, pexels-api, pingdom, pipedrive
plaid, plausible, pokeapi, polygon-stock-api, posthog, postmarkapp, pypi, recruitee
reply-io, revenuecat, rollbar, rss, savvycal, scryfall, sendgrid, sentry
shippo, shortcut, sigma-computing, simfin, slack, snapchat-marketing, spacex-api, sparkpost
square, statsig, statuspage, stockdata, strava, stripe, survicate, svix
the-guardian-api, ticketmaster, tiktok-marketing, tmdb, todoist, toggl, trello, tvmaze-schedule
typeform, us-census, uservoice, vantage, vercel, watchmode, weatherstack, whisky-hunter
wikipedia-pageviews, workflowmax, wufoo, xkcd, yahoo-finance-price, zapsign, zendesk-support, zenefits
zenloop, 

---

## UNFILLABLE (15)

Manifest exists in our set but vendor blocks self-serve credentialing
(credit card, business gating, sunset, rule-5 custom-class port required, etc.).
**Do not re-suggest these for credentialing rounds.**

datadog, delighted, elasticemail, mailgun, mailjet-mail, mailjet-sms, monday, n8n
opsgenie, pagerduty, paystack, reddit, rentcast, shortio, smaily, 

### Plus 6 sources not in our manifest set (no Airbyte declarative manifest)

discord, figma, github, heroku, shopify, supabase, 

---

## FAILED (2)

Registered, task is in FAILED state. Reason in `working-connectors.md`.

google-analytics-data-api, notion, 

---

## UNFILLED (336)

Manifest exists, no credentials attempted yet. Candidates for future
credentialing rounds — verify each one is fillable (no credit card,
no business gating) before suggesting.

100ms, 7shifts, acuity-scheduling, adjust, adobe-commerce-magento, agilecrm, aha, airbyte
aircall, alpaca-broker-api, amazon-sqs, apify-dataset, appcues, appfigures, apple-search-ads, ashby
avni, awin-advertiser, aws-cloudtrail, bigcommerce, bing-ads, blogger, bluetally, braintree
braze, breezometer, brex, bunny-inc, callrail, campaign-monitor, campayn, capsule-crm
captain-data, care-quality-commission, castor-edc, chameleon, chargedesk, chargify, chift, churnkey
cimis, cin7, circa, circleci, cisco-meraki, clarif-ai, clazar, clickup-api
clockodo, cloudbeds, coassemble, codefresh, coin-api, commercetools, concord, confluence
copper, countercyclical, criteo-marketing, customerly, datascope, dbt, deputy, devin-ai
ding-connect, dixa, docuseal, dolibarr, dremio, drip, dwolla, easypromos
ebay-finance, ebay-fulfillment, e-conomic, employment-hero, encharge, eventee, eventzilla, everhour
ezofficeinventory, facebook-pages, factorial, fastbill, fastly, feishu, finage, financial-modelling
finnworlds, firehydrant, fleetio, flexmail, flexport, float, flowlu, free-agent-connector
freightview, freshbooks, freshcaller, freshchat, freshservice, front, fullstory, getgist
getlago, glassfrog, gocardless, goldcast, gologin, gong, google-ads, google-pagespeed-insights
google-search-console, google-tasks, google-webfonts, gorgias, granola, greenhouse, greythr, guru
harness, height, hellobaton, help-scout, high-level, hoorayhr, hubspot, humanitix
illumina-basespace, incident-io, inflowinventory, insightful, instagram, instatus, interzoid, intruder
invoiced, invoiceninja, iterable, jamf-pro, jobnimbus, justcall, just-sift, k6-cloud
katana, keka, kisi, kissmetrics, klaus-api, leadfeeder, less-annoying-crm, lever-hiring
lightspeed-retail, linkedin-ads, linkedin-pages, linkrunner, looker, luma, mantle, mendeley
mention, mercado-ads, merge, metabase, metricool, microsoft-entra-id, microsoft-lists, miro
missive, mixpanel, mode, my-hours, navan, nebius-ai, newsdata, nexiopay
nexus-datasets, ninjaone-rmm, nocrm, northpass-lms, nutshell, nylas, okta, oncehub
onfleet, open-data-dc, opinion-stage, opuswatch, orb, oura, outlook, outreach
oveit, pabbly-subscriptions-billing, paddle, pandadoc, paperform, papersign, pardot, partnerstack
payfit, paypal-transaction, pendo, pennylane, perigon, perk, persona, phyllo
picqer, pinterest, pipeliner, pivotal-tracker, piwik, planhat, pocket, poplar
prestashop, pretix, primetric, printify, productboard, productive, public-apis, pylon
qonto, qualaroo, quickbooks, railz, rd-station-marketing, recharge, recreation, recurly
referralhero, repairshopr, retailexpress-by-maropost, retently, revolut-merchant, ringcentral, rocket-chat, rocketlane
rootly, ruddr, safetyculture, sage-hr, salesflare, salesloft, sap-fieldglass, secoda
segment, sendinblue, sendowl, sendpulse, senseforce, serpstat, sharetribe, shipstation
shopwired, shutterstock, signnow, simplecast, simplesat, smartengage, smartreach, smartwaiver
solarwinds-service-desk, sonar-cloud, split-io, spotify-ads, spotlercrm, squarespace, stigg, surveymonkey
survey-sparrow, systeme, taboola, tavus, teamtailor, teamwork, tempo, testrail
thinkific, thinkific-courses, thrive-learning, tickettailor, ticktick, timely, tinyemail, track-pms
tremendous, trustpilot, twelve-data, twilio, twilio-taskrouter, twitter, tyntec-sms, ubidots
unleash, uppromote, uptick, veeqo, visma-economic, vitally, vwo, waiteraid
wasabi-stats-api, web-scrapper, when-i-work, woocommerce, wordpress, workable, workramp, wrike
xero, xsolla, yotpo, you-need-a-budget-ynab, younium, yousign, youtube-analytics, youtube-data
zapier-supported-storage, zendesk-chat, zendesk-sell, zendesk-sunshine, zendesk-talk, zoho-analytics-metadata-api, zoho-bigin, zoho-billing
zoho-books, zoho-campaign, zoho-desk, zoho-expense, zoho-inventory, zoho-invoice, zonka-feedback, zoom
