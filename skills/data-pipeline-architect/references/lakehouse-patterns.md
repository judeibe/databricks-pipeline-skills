# Lakehouse Patterns: ELT, Dataset Types, Triggers, Table Formats

Databricks is ELT by default: land raw data in Unity Catalog (UC) tables, then transform with Lakeflow Spark Declarative Pipelines (SDP, formerly Delta Live Tables / DLT). SDP, LDP and DLT are the same product family; write new code with the modern API (`from pyspark import pipelines as dp`).

## ELT vs a pre-load step

Land raw, keep it, transform after. Add a pre-load step only when the data must not land as-is:

- PII must be dropped or masked before storage (compliance). Prefer landing it and protecting it with UC column masks and row filters (see `governance-and-observability.md`) unless policy forbids storing it at all.
- The source can only be read as an event stream you must filter before it is stored.

Otherwise: bronze is a streaming table over the source, and nothing is transformed in flight.

## Which dataset type? (the central decision)

Choose the dataset type first, then layer features (Auto Loader, Auto CDC, expectations) on top.

| Source / need | Dataset type | Notes |
|---|---|---|
| Files, message bus, Delta table that grows (append-only) | **Streaming table** | Exactly-once, incremental, checkpoint managed for you |
| Keep the latest row per key, SCD1/SCD2, apply deletes | **Streaming table + Auto CDC** | `create_auto_cdc_flow`; from periodic full snapshots use `create_auto_cdc_from_snapshot_flow` |
| Several sources into one table | **Streaming table + append flows** | Never `UNION` streams |
| Historical load plus live stream | **Streaming table + one-time append flow** | `@dp.append_flow(once=True)` |
| Aggregation, joins across the full dataset, gold marts | **Materialized view** | Batch read (`spark.read` / no `STREAM`) |
| JDBC / federated / small static reference data | **Materialized view** | Batch read |
| Intermediate logic reused inside the pipeline | **Temporary view** | Default for intermediate steps. Private table/MV only when materialising once saves real reprocessing |
| Output to an external system (Kafka, external Delta, custom) | **Sink** (Python only, append-only) | `dp.create_sink` with an append flow; `@dp.foreach_sink()` for per-row custom writes; `foreach_batch_sink` is Public Preview. Expectations do not run on sinks |
| Reload a slice (a day, a region) inside the pipeline | **`REPLACE WHERE` flow** (Beta) | Incremental refresh where eligible; expectations are not supported on its target |
| Sub-second latency | **Real-Time Mode** | Python only; `@dp.update_flow`; continuous pipeline; Public Preview |

Rules that bite:

- A streaming table cannot be changed into a materialized view in place (or the reverse). Full refresh does not fix it; drop the table or rename the dataset.
- An aggregation over a streaming source belongs in a materialized view with a batch read, because streaming tables are append-only and will not recompute aggregates when upstream rows change.
- Gold keeps the business dimensions people filter by (location, product, segment, time). Over-aggregating loses information that cannot be recovered.
- Materialized view incremental refresh needs **serverless** compute and Delta row tracking on the sources. Tables the pipeline creates have row tracking on by default; set `delta.enableRowTracking = true` only on sources from outside the pipeline. Non-deterministic functions, and filtering on **both** ends of a date range, force a full recompute; the MV query profile shows why a refresh was not incremental.
- Changing a stateful streaming query (watermark, aggregation or dedup keys) makes existing state incompatible and needs a full refresh of that table, which the user approves and runs. Keep bronze minimal and source-faithful so silver and gold can always be rebuilt from it.
- SQL does not support `CREATE OR REPLACE STREAMING TABLE`; use `CREATE OR REFRESH`.

## Batch vs streaming trigger

SDP pipelines run **triggered** (`continuous: false`, the default; schedule it from a Lakeflow Job) or **continuous** (always on). Pick by the freshness SLA and cost tolerance, not novelty.

| Need | Pattern |
|---|---|
| Daily or hourly reporting | Triggered pipeline on a Jobs schedule |
| Minutes-fresh | Triggered pipeline on a short schedule, or a Jobs trigger on file arrival or table update |
| Seconds-fresh, steady flow | Continuous pipeline |
| Sub-second | Real-Time Mode (see `databricks-pipelines/references/real-time-mode.md`) |
| Replication from an operational database | Lakeflow Connect database connector (see `ingestion-patterns.md`) |

Start triggered. Triggered pipelines with `availableNow`-style incremental processing satisfy most "real-time" requests, and continuous pipelines cost compute while idle. Lakeflow Connect pipelines are triggered-only.

## Table formats

- **Delta Lake on Unity Catalog managed tables** is the default. It gives ACID commits, `MERGE`, time travel, schema evolution, liquid clustering, and change data feed.
- Use **Iceberg** only when an external engine must read or write the data: managed Iceberg tables (`USING ICEBERG`), UniForm on Delta tables, or Compatibility Mode for streaming tables and materialized views. External engines connect through the Iceberg REST Catalog. Do not install an Iceberg library into Databricks Runtime. Load the `databricks-iceberg` skill before choosing between them.
- Avoid bare CSV or JSON beyond a landing zone. Read them with Auto Loader or `read_files` into Delta.

## Layout

- Use **liquid clustering**. Default to `cluster_by_auto=True` / `CLUSTER BY AUTO`; it needs Unity Catalog managed tables and predictive optimization, and picks keys from the query workload. Use explicit keys (`cluster_by=[...]`, `CLUSTER BY (...)`) when the filter columns are known up front; you can give them as hints and still enable AUTO. `partition_cols` / `PARTITIONED BY` is legacy, and setting both partitioning and clustering on one table is an error.
- Explicit keys: at most **four**, chosen from the columns queries filter on most. Fewer is better for tables under 10 TB, and of two highly correlated columns keep one. Use the original timestamp rather than a derived low-cardinality date column.
- Cluster keys must be numeric, string, date or timestamp (not BOOLEAN, ARRAY, MAP, STRUCT or BINARY), and within the columns that collect statistics (the first 32 by default). SDP does not pre-validate this, so a bad key fails at first write.
- Auto CDC targets support liquid clustering; cluster them on the key.

## Quick selection rules

1. Destination is the lakehouse → ELT into UC tables.
2. Append-only growth → streaming table. Changes by key → Auto CDC. Whole-dataset recompute → materialized view.
3. Freshness of a few minutes or more → triggered. Seconds → continuous. Sub-second → Real-Time Mode.
4. Keep immutable raw (bronze) so you can replay or backfill without re-reading the source.
5. Prefer serverless compute (see `orchestration.md`).
