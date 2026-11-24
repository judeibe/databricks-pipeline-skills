# Ingestion Patterns

Map every source to exactly one ingestion path first. Then choose how changes are applied downstream.

## 1. Choose the ingestion path

| Source | Use | Skill to load |
|---|---|---|
| SaaS app or database with a managed connector (Salesforce, Workday, ServiceNow, GA4, HubSpot, Confluence, SQL Server, PostgreSQL, MySQL, ...) | **Lakeflow Connect** | `databricks-lakeflow-connect` |
| Files in cloud storage or a UC Volume | **Auto Loader** (`cloudFiles` / `STREAM read_files`) | `databricks-pipelines` |
| Kafka, Kinesis, Pub/Sub, Pulsar, Event Hubs | **Streaming read** in a streaming table | `databricks-pipelines` (`kafka.md`) |
| An app, service or device that pushes events at you | **Zerobus Ingest** | `databricks-zerobus-ingest` |
| Data you only need to query where it lives | **Lakehouse Federation** (no copy) | `databricks-dbsql` (`remote_query`) |
| A partner's Delta share | **Delta Sharing** | `databricks-docs` (not covered by a product skill) |
| An API or source none of the above reach | A **Jobs task** writing a bronze Delta table | `databricks-jobs` |

Key facts for each path:

- **Lakeflow Connect** pipelines carry an `ingestion_definition` (never a `libraries` block), are **triggered-only** (`continuous: false`), and are scheduled with a Jobs `pipeline_task`. SaaS OAuth U2M connections are created in the UI. Database connectors use a classic-compute gateway plus a serverless ingestion pipeline. Output is source-faithful landing tables; build bronze/silver/gold on top with SDP. `CREATE TABLE ... FROM CONNECTION` is Lakehouse Federation syntax, not Lakeflow Connect.
- **Auto Loader**: do not set `cloudFiles.schemaLocation` inside a pipeline; the pipeline manages schema location and checkpoint. At scale prefer file notification (`cloudFiles.useNotifications` / `useManagedFileEvents`) over directory listing. Authenticate with a UC service credential (`databricks.serviceCredential`) instead of inline keys.
- **Zerobus Ingest** is push-based. The target must be a pre-created UC managed Delta table; Zerobus never creates or alters tables. Grant `MODIFY` and `SELECT` directly on the table (`ALL PRIVILEGES` is not enough). Delivery is **at-least-once**, so duplicates are possible: dedupe downstream with Auto CDC if the use case needs unique rows. Acknowledged is durable, not yet queryable (about 5 seconds).
- **Federation / JDBC** reads are batch: use them in a materialized view or a Jobs task, not a streaming table.

## 2. Incremental reads: let the platform track state

In SDP you do not build watermark tables. State is the pipeline's checkpoint:

- Auto Loader tracks which files it has processed.
- A streaming read of a Delta table (`spark.readStream.table(...)`, `STREAM(table)`) tracks the table version it has consumed.
- Never call `.writeStream`, `.start()`, or pass checkpoint options inside a pipeline.

If the upstream table has updates or deletes (for example an Auto CDC target or a GDPR purge), a downstream streaming read fails unless you set `skipChangeCommits`:

```python
@dp.table()
def downstream():
    return spark.readStream.option("skipChangeCommits", "true").table("upstream_with_deletes")
```

### When you do need a manual watermark

Only for sources the platform cannot stream: REST APIs, federated JDBC pulls into a Jobs task. Rules:

1. Read the high-watermark from the **target table** (`SELECT MAX(updated_at) FROM target`) inside the same task that appends, not from a file or a driver variable. A retry then sees the advanced watermark and appends nothing, instead of appending the same rows twice.
2. Query with a lookback overlap (`> watermark - interval`) and dedupe, so late and same-timestamp rows are not lost.
3. Prefer a monotonic id, then a reliably maintained `updated_at`, then a log offset.
4. Fail loudly on unexpected errors. Treating a permission error or a renamed watermark column as "first load" silently re-ingests everything.

```sql
SELECT * FROM source_table
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), TIMESTAMP '1970-01-01') FROM bronze.orders)
                   - INTERVAL 2 HOURS;
```

## 3. Applying changes: Auto CDC

For "keep the latest row per key", deletes, or history, use Auto CDC instead of hand-written `ROW_NUMBER` dedup or `MERGE`. The target is a streaming table created first; the flow reads a table or temporary view (never a DataFrame or subquery).

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import expr

@dp.temporary_view()
def order_changes():
    return spark.readStream.table("bronze_orders").filter("order_id IS NOT NULL")

dp.create_streaming_table(name="silver_orders")

dp.create_auto_cdc_flow(
    target="silver_orders",
    source="order_changes",
    keys=["order_id"],
    sequence_by="updated_at",              # struct("updated_at", "_ingest_ts") for tie-breaks
    apply_as_deletes=expr("op = 'D'"),
    stored_as_scd_type=1,                  # 2 keeps history (__START_AT / __END_AT)
)
```

```sql
CREATE OR REFRESH STREAMING TABLE silver_orders;

CREATE FLOW silver_orders_flow AS AUTO CDC INTO silver_orders
FROM STREAM(order_changes)
KEYS (order_id)
APPLY AS DELETE WHEN op = 'D'
SEQUENCE BY updated_at
STORED AS SCD TYPE 1;
```

- SQL clause order matters: put `APPLY AS DELETE` / `APPLY AS TRUNCATE` **before** `SEQUENCE BY`.
- `APPLY AS TRUNCATE` is SCD Type 1 only.
- Multi-column ordering: `SEQUENCE BY STRUCT(col1, col2)` in SQL, `struct("col1", "col2")` in Python.
- SCD Type 2 adds `__START_AT` and `__END_AT`; current rows are `WHERE __END_AT IS NULL`. Use `track_history_column_list` / `TRACK HISTORY ON` to limit which columns create history rows.
- **Periodic full snapshots** (daily extract compared with the previous state): `dp.create_auto_cdc_from_snapshot_flow` (Python) or SQL `AUTO CDC ... FROM SNAPSHOT`; check the runtime and channel requirement in `databricks-pipelines`. Expectations are not supported on snapshot CDC, so validate in the upstream view.
- The sequence column must never be NULL and must keep the same type across runs; changing `keys` or the sequence type needs a full refresh of the target, which the user approves and runs.
- For append-only streams with no semantic updates, `dropDuplicates` / `SELECT DISTINCT` is cheaper than Auto CDC.
- Legacy names (`apply_changes`, `APPLY CHANGES INTO`, `import dlt`, `LIVE.`) must be migrated, not extended.

## 4. Late and out-of-order data

- Aggregate on **event time**, not processing time; keep `_ingested_at` for debugging only.
- Order Auto CDC by a sequence column so late events do not overwrite newer rows.
- Every stateful operation (windowed aggregation, streaming dedup) needs a watermark (`.withWatermark(...)`, SQL `WATERMARK col DELAY OF INTERVAL ...`), or state grows without bound and the aggregation is not incremental. Choose the smallest window that meets the freshness SLA.
- Stream-to-stream joins need a watermark on **both** sides and a time-bounded join condition.
- Stream-static joins read the static side at the start of each micro-batch; later dimension changes are not applied to facts already processed. Use a materialized view if they must be.
- If late events land in already-aggregated gold windows, a materialized view recomputes them; a streaming-table aggregate does not.

## 5. Technical columns

Stamp these at bronze for lineage and debugging:

- `_ingested_at` (`current_timestamp()`), `_source`, and a run or batch id when you have one.
- File-based sources: `_metadata.file_path` (SQL) / `F.col("_metadata.file_path")` (Python). `input_file_name()` is legacy.
- `_rescued_data` from Auto Loader (see `schema-evolution.md`).

## 6. When SDP is not enough: Structured Streaming

Use plain Spark Structured Streaming (load `databricks-spark-structured-streaming`) only when SDP cannot express the job: custom `foreachBatch` merge logic across several sinks, unusual stateful operators, or Real-Time Mode outside a pipeline. Then:

- Checkpoints live in a UC Volume, one unique checkpoint per stream.
- Set watermarks for every stateful operation.
- Make multi-sink writes idempotent with `txnVersion` / `txnAppId`.
- On serverless use `trigger(availableNow=True)`; `processingTime` and `continuous` triggers are not supported. For always-on, run it in a continuous job.
- Use a fixed-size cluster for classic streaming (no autoscaling).
- To move an existing Structured Streaming job into a pipeline without reprocessing, import its checkpoint (`import_checkpoint`) instead of starting from scratch.
