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
| Output to an external system (Kafka, external Delta, custom) | **Sink** (Python only) | `dp.create_sink`, `foreach_batch_sink` is Public Preview |
| Sub-second latency | **Real-Time Mode** | Python only; `@dp.update_flow`; continuous pipeline; Public Preview |

Rules that bite:

- A streaming table cannot be changed into a materialized view in place (or the reverse). Full refresh does not fix it; drop the table or rename the dataset.
- An aggregation over a streaming source belongs in a materialized view with a batch read, because streaming tables are append-only and will not recompute aggregates when upstream rows change.
- Gold keeps the business dimensions people filter by (location, product, segment, time). Over-aggregating loses information that cannot be recovered.
- Materialized view incremental refresh needs **serverless** compute and Delta row tracking (`delta.enableRowTracking = true`) on the sources. Without both it falls back to a full recompute.
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

- Use **liquid clustering** (`cluster_by=[...]` in Python, `CLUSTER BY (...)` in SQL, or `cluster_by_auto=True` / `CLUSTER BY AUTO`). `partition_cols` / `PARTITIONED BY` is legacy.
- Cluster keys must be numeric, string, date or timestamp (not BOOLEAN, ARRAY, MAP, STRUCT or BINARY). SDP does not pre-validate this, so a bad key fails at first write.

## Quick selection rules

1. Destination is the lakehouse → ELT into UC tables.
2. Append-only growth → streaming table. Changes by key → Auto CDC. Whole-dataset recompute → materialized view.
3. Freshness of a few minutes or more → triggered. Seconds → continuous. Sub-second → Real-Time Mode.
4. Keep immutable raw (bronze) so you can replay or backfill without re-reading the source.
5. Prefer serverless compute (see `orchestration.md`).
