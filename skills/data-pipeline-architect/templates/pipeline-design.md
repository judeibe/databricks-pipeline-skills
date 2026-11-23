# Pipeline Design: <pipeline-name>

> Fill every section. Empty sections are design gaps. Validate the resulting bundle with `databricks bundle validate --strict`.

## 1. Requirements
- **Purpose / consumers:** <BI / metric views / Genie / app / ML / external engines>
- **Freshness SLA:** <sub-second | seconds | minutes | hourly | daily>
- **Volume:** <rows/day, GB/day, peak>
- **Data sensitivity / PII:** <yes/no; masks, row filters, retention>
- **Owners / on-call / notification target:**

## 2. Sources and ingestion path
| Source | Path (Lakeflow Connect / Auto Loader / streaming read / Zerobus / Federation / Jobs task) | Incremental mechanism | Change handling (append / Auto CDC / snapshot) | Volume | Notes |
|---|---|---|---|---|---|
| | | | | | |

## 3. Dataset design
- **Bronze:** <streaming table(s); source-faithful; technical columns>
- **Silver:** <streaming table + Auto CDC (keys, `sequence_by`, SCD 1/2, deletes) / cleaned streaming table>
- **Gold:** <materialized views / marts; the business dimensions kept>
- **Intermediate logic:** <temporary views>
- **Trigger mode:** <triggered | continuous | Real-Time Mode> because:
- **Table format:** <Delta (default) | Iceberg / UniForm / Compatibility Mode, reason>
- **Layout:** <liquid clustering keys or `CLUSTER BY AUTO`>
- **Catalog / schema per layer (bundle variables):**

## 4. Idempotency (REQUIRED)
- **Business key(s):**
- **Mechanism per write:** <streaming-table checkpoint | Auto CDC | MV recompute | MERGE | REPLACE WHERE | CREATE OR REPLACE | txnVersion/txnAppId>
- **Dedup / recency tiebreaker:**
- **Re-run safety statement:** running range [A,B] twice yields identical output because ...

## 5. Non-streamable sources (only if a Jobs task ingests)
- **Watermark column and where it is read (must be the target table):**
- **Lookback window:**
- **Late-arriving handling:**

## 6. Schema evolution policy
- **Bronze:** <Auto Loader mode (`addNewColumns` / `rescue` / ...), `_rescued_data` handling>
- **Silver / Gold:** <explicit schema + constraints; breaking change -> fail | quarantine | alert>
- **Push sources (Zerobus):** <table-first rollout, rescue column>

## 7. Orchestration and compute
- **Jobs and tasks:** <pipeline_task, depends_on, run_if, for_each>
- **Trigger:** <cron | periodic | file_arrival | table_update | continuous>
- **Retries / timeout / queue / max concurrent runs:**
- **Alerting:** <email | webhook | health rules | pipeline notifications>
- **Compute:** <serverless (default) or classic with the blocker>, `performance_target`: <STANDARD | PERFORMANCE_OPTIMIZED> because:

## 8. Data quality gates (REQUIRED)
| Stage | Check | Mechanism | Action (warn / drop / fail / quarantine / alert) |
|---|---|---|---|
| ingest | schema, row count, freshness | Auto Loader mode, expectation, SQL alert | |
| silver | keys not null, accepted values, referential | expectations | |
| gold | reconciliation, volume, freshness, constraints | job SQL task, data quality monitoring, CHECK | |

## 9. Deployment (bundle)
- **Bundle name and resources:** <`<name>.pipeline.yml`, `<name>.job.yml`>
- **Targets:** <dev (development mode), prod (production mode)>; **variables per target:** <catalog, schema, warehouse_id, notification_email>
- **`run_as`:** <service principal for prod>
- **Permissions:**
- **Validation and deploy commands:** `bundle validate --strict`, `bundle deploy -t <target>`, `bundle run <resource>`

## 10. Backfill and recovery
- **Parameterised by:** <job parameters / one-time append flow>
- **Backfill command:**
- **Concurrency limit during backfill:**
- **Full refresh policy:** requires explicit approval; reason it may be needed:
- **Rollback:** `RESTORE` / repair run:

## 11. Governance and observability
- **Grants (groups, least privilege):**
- **Row filters / column masks / tags:**
- **Event-log and system-table queries to monitor:**
- **Lineage / audit needs:**
- **Cost levers and estimate:**

## 12. Consumers
- <metric views, dashboards, Genie, Lakebase synced tables, Iceberg readers, ML/AI>; the dimensions each needs in gold:

## 13. Open risks / decisions
-
