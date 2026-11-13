# Payments Data Platform: Engineering Sample

**Author:** Suresh Srivatsavai · [LinkedIn](https://www.linkedin.com/in/suresh-srivatsavai/)

An end-to-end sample showing how I design, migrate and operate a regulated payments data platform:
near-real-time ingestion with data contracts, incremental migration from a legacy on-prem estate,
a governed Snowflake layer, contract-enforced dbt data products (payments and collections),
source-to-target reconciliation, orchestration, infrastructure as code and CI/CD.

> Representative code written for this sample, based on patterns from my payments work.
> Names are generic; no employer-proprietary code or data.

## Scenario

A payments platform processes millions of payment attempts a day across multiple processors.
Historical data lives in an on-prem Hadoop / Oracle estate. The goal: move reporting onto Snowflake,
add near-real-time event ingestion from Kafka, enforce data contracts and PII controls, and prove the
migrated data reconciles to the source before cutover. The patterns are cloud-agnostic.

## Architecture

```
Payment events (Kafka) ──► Python consumer ──► Snowflake RAW ──┐
   (contract validation, dead-letter topic)                     │
                                                                ├─► Silver (MERGE, PII masking) ─► Gold (dbt data products)
Legacy Oracle ──► Spark on Hadoop ──► Hadoop raw ──► Snowflake ─┘                                   │
                  (incremental watermark)          (stage + COPY)                                    ▼
                                                                         Finance · Risk · Collections · Reporting
Orchestration: Airflow (volume gate, reconciliation gate, SLAs) · Terraform (Snowflake) · GitHub Actions (dbt slim CI)
```

## Repository layout

| Path | What it does |
|---|---|
| `contracts/payment_attempt.v2.json` | Versioned data contract for payment events |
| `ingestion/payment_events_consumer.py` | Kafka consumer: validates events, routes bad ones to a dead-letter topic, loads Snowflake |
| `spark/extract_legacy_settlements.py` | Incremental (high-watermark) extract of settlements from legacy Oracle to Hadoop |
| `snowflake/silver_payments.sql` | Staged loads, stream + task MERGE (latest payment state), PII masking policy |
| `snowflake/load_settlements.sql` | Idempotent daily COPY of staged settlement files |
| `dbt/models/gold/payments/` | `fct_payment_attempts` data product with an enforced contract and tests |
| `dbt/models/gold/collections/` | `fct_collections_balances`: balances owed, aging buckets, agency recovery rate |
| `quality/reconcile.py` | Legacy vs. Snowflake reconciliation (rows and dollars, to the cent) |
| `dags/payments_platform_daily.py` | Airflow DAG: extract → stage → load → volume gate → dbt → reconciliation gate |
| `terraform/snowflake_payments.tf` | Snowflake database, warehouse, PII role and grants as code |
| `.github/workflows/dbt_ci.yml` | Slim CI: builds and tests only changed dbt models on each pull request |

## Business context

| Term | Meaning | Why the data matters |
|---|---|---|
| Payment attempt | One try to charge a customer for an order; an order can have several | Grain of the core fact table; drives Payment Success Rate |
| Authorization | Issuing bank approves and holds funds; no money moved yet | First health metric for checkout |
| Capture | Merchant collects the authorized funds | Captured amount = recognized payment volume |
| Decline (hard / soft) | Hard = permanent (stolen card); soft = temporary (insufficient funds) | Soft declines are retry-able, recoverable revenue |
| Settlement | Processor pays the merchant, net of fees, 1–3 days later | Finance reconciles settled cash to captured payments |
| Refund / Chargeback | Merchant returns money / customer disputes with their bank | Revenue leakage and risk signals |
| Collections | Recovering money owed after payments fail, often via external agencies | Delinquent balances, aging, recovery rate by agency |
| Customer experience | Contacts and satisfaction (CSAT) related to payment problems | Shows the downstream cost of a failed payment |

## Key engineering decisions

| Decision | Why |
|---|---|
| Data contracts at ingestion + enforced dbt contracts | Breaking changes are caught at the boundary or in CI, never by downstream consumers |
| Dead-letter topic instead of dropping bad events | No silent data loss; every rejected record is traceable and replayable (audit) |
| Immutable raw layer, MERGE-based silver | Full replay/backfill; idempotent handling of late-arriving updates |
| Role-based PII masking | Least-privilege access to customer data in a regulated environment |
| Reconciliation gate before cutover | Rows and dollars must match legacy to the cent before finance signs off |
| Volume + freshness SLAs with on-call alerts | Broken upstream feeds are caught before reports publish |
| Snowflake as code + slim CI | Reproducible environments; fast, low-cost PR checks |
| Cost controls (auto-suspend, clustering, incremental models) | Platform scales with volume without compute cost scaling linearly |
