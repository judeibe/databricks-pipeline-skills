# Worked Example: Orders Pipeline on Databricks

**Scenario:** An operational `orders` table is replicated as JSON change files (one row per insert, update or delete, with an `op` flag and an `updated_at` timestamp) into a Unity Catalog Volume. Finance needs daily revenue dashboards, and a churn model needs order history. About 2M changed rows a day, about 3 GB a day, freshness SLA 1 hour, no PII beyond a pseudonymous `customer_id`.

If a managed Lakeflow Connect connector exists for the source database, use it for step 2 instead and build silver and gold on its landing tables (`databricks-lakeflow-connect`).

## 1. Requirements
- Consumers: Finance dashboards (gold marts, metric view), churn ML (silver history).
- Freshness SLA: 1 hour. Volume: ~2M changed rows/day.
- PII: `customer_id` only; no masks needed. Deletes must be honoured.

## 2. Ingestion path
- **Auto Loader** over the Volume: a streaming table, incremental by file, checkpoint managed by the pipeline.
- **Triggered** pipeline run every 30 minutes by a job (well inside the SLA, cheaper than a continuous pipeline).
- Bronze is append-only and keeps everything, including `_rescued_data`.

## 3. Dataset design

| Layer | Dataset | Type | Notes |
|---|---|---|---|
| Bronze | `bronze_orders` | Streaming table (Auto Loader) | `_ingested_at`, `_source_file`, `_rescued_data` |
| Silver | `silver_orders` | Streaming table + Auto CDC (SCD1) | key `order_id`, sequenced by `updated_at`, `op = 'D'` deletes |
| Silver | `silver_orders_quarantine` | Streaming table | rows that fail rules, with the rescued data |
| Gold | `gold_daily_revenue` | Materialized view | grain: day x region x status, so dashboards can filter and slice |

## 4. Pipeline code

```python
# src/orders_pipeline/transformations/bronze_orders.py
from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE_PATH = spark.conf.get("orders_source_path")   # e.g. /Volumes/<catalog>/<schema>/raw/orders/


@dp.table(
    name="bronze_orders",
    comment="Raw order change events, append-only.",
    cluster_by_auto=True,
)
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaHints", "order_id BIGINT, amount DECIMAL(12,2), updated_at TIMESTAMP")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(SOURCE_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
```

```python
# src/orders_pipeline/transformations/silver_orders.py
from pyspark import pipelines as dp
from pyspark.sql.functions import expr

RULES = {
    "valid_amount": "amount IS NULL OR amount >= 0",
    "has_status": "status IN ('placed', 'shipped', 'cancelled')",
}


@dp.temporary_view()
@dp.expect_or_fail("required_key", "order_id IS NOT NULL AND updated_at IS NOT NULL")
@dp.expect_all_or_drop(RULES)
def valid_order_changes():
    return spark.readStream.table("bronze_orders").where("_rescued_data IS NULL")


dp.create_streaming_table(
    name="silver_orders",
    comment="Latest version of each order; deletes applied.",
    cluster_by=["order_id"],
    table_properties={"delta.enableRowTracking": "true"},
)

dp.create_auto_cdc_flow(
    target="silver_orders",
    source="valid_order_changes",
    keys=["order_id"],
    sequence_by="updated_at",
    apply_as_deletes=expr("op = 'D'"),
    stored_as_scd_type=1,
)


# Quarantine: nothing is silently lost. The filter is the inverse of the rules above (plus rescued data).
@dp.table(name="silver_orders_quarantine", comment="Rows that failed parsing or value rules.")
def silver_orders_quarantine():
    return spark.readStream.table("bronze_orders").where(
        "_rescued_data IS NOT NULL "
        "OR NOT coalesce((amount IS NULL OR amount >= 0) AND status IN ('placed','shipped','cancelled'), false)"
    )
```

```python
# src/orders_pipeline/transformations/gold_daily_revenue.py
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_daily_revenue",
    comment="Daily revenue by region and status.",
    cluster_by=["order_date", "region"],
)
def gold_daily_revenue():
    return (
        spark.read.table("silver_orders")
        .withColumn("order_date", F.to_date("updated_at"))
        .groupBy("order_date", "region", "status")
        .agg(F.sum("amount").alias("revenue"), F.count("*").alias("orders"))
    )
```

Notes:
- Datasets are referenced by bare name inside the pipeline; the pipeline's default catalog and schema apply.
- The temporary view carries the expectations because the CDC flow cannot read a subquery, and a SQL temp view would not accept constraints; in Python the decorators work on the view.
- Row tracking on `silver_orders` lets the gold materialized view refresh incrementally (serverless only).

## 5. Bundle resources

```yaml
# databricks.yml
bundle:
  name: orders

include:
  - resources/*.yml

variables:
  catalog:
    description: Catalog for this bundle's tables
  schema:
    description: Schema for this bundle's tables
  notification_email:
    description: Failure notification address

targets:
  dev:
    default: true
    mode: development
    variables:
      catalog: dev_main
      schema: orders
      notification_email: data-dev@example.com
  prod:
    mode: production
    workspace:
      # Required in production mode. Use the deploying identity's folder (a CI service principal in practice);
      # /Workspace/Shared is writable by everyone and fails --strict.
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    variables:
      catalog: prod_main
      schema: orders
      notification_email: data-oncall@example.com
```

```yaml
# resources/orders_pipeline.pipeline.yml
resources:
  pipelines:
    orders_pipeline:
      name: "[${bundle.target}] orders_pipeline"
      catalog: ${var.catalog}
      schema: ${var.schema}
      serverless: true
      continuous: false
      root_path: ../src/orders_pipeline
      libraries:
        - glob:
            include: ../src/orders_pipeline/transformations/**
      configuration:
        orders_source_path: /Volumes/${var.catalog}/${var.schema}/raw/orders/
      tags:
        team: finance-data
```

```yaml
# resources/orders_refresh.job.yml
resources:
  jobs:
    orders_refresh:
      name: "[${bundle.target}] orders_refresh"
      max_concurrent_runs: 1
      queue:
        enabled: true
      performance_target: PERFORMANCE_OPTIMIZED   # runs every 30 min against a 1h SLA
      timeout_seconds: 3600
      schedule:
        quartz_cron_expression: "0 0/30 * * * ?"
        timezone_id: UTC
        pause_status: UNPAUSED
      email_notifications:
        on_failure: ["${var.notification_email}"]
        on_duration_warning_threshold_exceeded: ["${var.notification_email}"]
      health:
        rules:
          - metric: RUN_DURATION_SECONDS
            op: GREATER_THAN
            value: 1800
      tasks:
        - task_key: refresh_pipeline
          max_retries: 2
          min_retry_interval_millis: 60000
          pipeline_task:
            pipeline_id: ${resources.pipelines.orders_pipeline.id}
            full_refresh: false
```

A 30-minute cadence is below the 1-hour minimum for `trigger.periodic`, so this uses a cron `schedule`. `PERFORMANCE_OPTIMIZED` is justified by the frequency; a nightly job would use `STANDARD`.

## 6. Schema evolution
- Bronze: `addNewColumns` adds new optional fields; the stream fails once with `UnknownFieldException` and the job's retry restarts it. Unparseable or mistyped fields land in `_rescued_data`.
- Silver and gold: explicit expectations. A dropped or renamed required field trips `required_key` (fail the update) and the on-call is alerted; fix the contract, then replay from bronze.

## 7. Data quality gates

| Stage | Check | Mechanism | Action |
|---|---|---|---|
| Bronze | parse and schema problems | `_rescued_data` | quarantine |
| Silver | `order_id` and `updated_at` present | `expect_or_fail` | fail update |
| Silver | amount >= 0, status in the allowed set | `expect_all_or_drop` | drop and quarantine |
| Gold | revenue reconciles with silver | SQL task or alert | fail the task |
| Gold | freshness within 1 hour | data quality monitoring or SQL alert | alert |

## 8. Backfill and recovery

```bash
# Re-run the whole pipeline refresh (selective refresh of one table is preferred over full refresh)
databricks bundle run orders_refresh -t dev --profile <PROFILE>

# Load history: land the old files in the watched Volume (Auto Loader processes each file once).
# If history lives in a table instead, define bronze with dp.create_streaming_table(...) plus two
# append flows: the live Auto Loader flow and a once=True flow that batch-reads the archive with
# spark.read (Auto Loader / cloudFiles is streaming-only, so it cannot be used in a once flow).

# After a failed multi-task run: repair re-runs only the failed and skipped tasks
# (see the Jobs repair-run API; it applies to jobs with two or more tasks)

# Undo a bad write to a Delta table outside a pipeline
#   DESCRIBE HISTORY gold.some_table;   RESTORE TABLE gold.some_table TO VERSION AS OF <n>;
```

A **full refresh** of `bronze_orders` would reprocess every file and discard streaming state; only do it with explicit approval and when the files still exist in the Volume.

## 9. Why this is safe

- Bronze is append-only and never mutated: you can replay silver and gold from it without touching the source.
- Streaming-table checkpoints give exactly-once file processing, and Auto CDC applies each change by key and sequence, so a retry cannot duplicate orders.
- The gold materialized view is recomputed from silver, so re-runs converge.
- Bad rows are dropped *and* quarantined, never silently lost; a missing key stops the update.
- Everything is deployed as code with `bundle validate --strict`, so the dev and prod targets differ only by variables.

## 10. Variations

- **Event stream instead of files:** replace Auto Loader with a Kafka/Kinesis/Event Hubs streaming read, or let producers push through Zerobus (at-least-once, so keep the Auto CDC dedupe).
- **Daily full extract instead of changes:** read the latest extract with a materialized view and use `create_auto_cdc_from_snapshot_flow`.
- **Reverse ETL for an app:** sync `silver_orders` into Lakebase with a synced table (enable change data feed on the source).
