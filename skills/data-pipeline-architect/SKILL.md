---
name: data-pipeline-architect
description: Designs and reviews Databricks data pipelines end to end with Lakeflow Spark Declarative Pipelines (streaming tables, materialized views, Auto CDC, expectations), Auto Loader, Lakeflow Connect, Zerobus, Lakeflow Jobs and Declarative Automation Bundles on Unity Catalog. Use this skill when the user asks to design, build, or review a pipeline on Databricks; ingest from files, databases, SaaS apps, Kafka or pushed events into Delta; choose streaming table vs materialized view; handle CDC, SCD, late or duplicate data; make a load idempotent or backfill-safe; manage schema drift; add data quality gates, freshness checks or quarantine; schedule, trigger, retry or repair jobs; pick serverless vs classic compute; or plan catalogs, grants and monitoring.
license: MIT
---
# Data Pipeline Architect (Databricks)

## Overview

This skill designs and reviews production data pipelines **on Databricks**. It covers the full lifecycle: choosing an ingestion path, modelling bronze/silver/gold datasets, applying changes (Auto CDC), idempotency and backfills, schema evolution, data quality gates, orchestration with Lakeflow Jobs, serverless compute, deployment as a bundle, and governance and monitoring.

It holds the **architecture decisions**. For exact API syntax and CLI behaviour it routes you to the Databricks product skills, which stay current with the platform. Do not copy their reference tables into a design.

Keywords: Lakeflow, SDP, DLT, streaming table, materialized view, Auto CDC, SCD, Auto Loader, Lakeflow Connect, Zerobus, Lakehouse Federation, expectations, Lakeflow Jobs, DABs, bundle, Unity Catalog, medallion, bronze silver gold, idempotency, backfill, schema evolution, liquid clustering, serverless.

Use it to produce a pipeline design (`templates/pipeline-design.md`), to review an existing pipeline, or to scaffold pipeline and bundle skeletons.

## Prerequisites

1. Load **`databricks-core`** first (CLI, auth, bundles). Confirm the CLI is current.
2. **Never auto-select a CLI profile.** List profiles (`databricks auth profiles`), show them with their workspace URLs, let the user choose, and pass `--profile <name>` on every command (each Bash call is a separate shell).
3. Load the product skills the design touches (table below) **before** writing code or YAML.

## Skill routing

| Concern | Load |
|---|---|
| Datasets, Auto CDC, Auto Loader, expectations, pipeline code | `databricks-pipelines` |
| Managed SaaS or database ingestion | `databricks-lakeflow-connect` |
| App, service or device pushing events | `databricks-zerobus-ingest` |
| Plain Structured Streaming (what SDP cannot express) | `databricks-spark-structured-streaming` |
| Jobs, tasks, triggers, retries, notifications | `databricks-jobs` |
| Bundle files, targets, deploy and run | `databricks-dabs` |
| Compute choice, `performance_target` | `databricks-serverless` (and `databricks-serverless-migration` for existing jobs) |
| Catalogs, grants, masks, system tables, volumes | `databricks-unity-catalog` |
| SQL features, warehouses, federated queries | `databricks-dbsql` |
| Iceberg, UniForm, external engine access | `databricks-iceberg` |
| Governed metrics, dashboards, Genie | `databricks-metric-views`, `databricks-aibi-dashboards`, `databricks-genie-agents`, `databricks-data-discovery` |
| Postgres serving, reverse ETL | `databricks-lakebase` |
| Running and testing code, SDK | `databricks-execution-compute`, `databricks-setup-local`, `databricks-python-sdk` |
| Test data, including bad rows | `databricks-synthetic-data-gen` |
| Anything not covered | `databricks-docs` (llms.txt index; it has no pages for expectations, Auto Loader schema evolution, MERGE or job retries, so use the pipelines and jobs skill references for those) |

Downstream consumers such as ML training, Vector Search, Agent Bricks, Model Serving and AI Functions are not part of pipeline design; record their needs in the design's consumers section.

## Decision: ELT on the lakehouse

Land raw data in Unity Catalog tables first, then transform with SDP. Add a pre-load step only when data must not land as-is (for example PII that policy forbids storing). See `references/lakehouse-patterns.md`.

## Workflow

Follow these steps in order, filling `templates/pipeline-design.md` as you go.

1. **Clarify requirements.** Sources, destination, freshness SLA, volume, data sensitivity, consumers, owners. Do not design before you know freshness and volume; they drive triggered vs continuous and compute.
2. **Choose the ingestion path** per source: Lakeflow Connect, Auto Loader, streaming read, Zerobus, Federation, or a Jobs task. See `references/ingestion-patterns.md`.
3. **Choose dataset types** per layer: streaming table, Auto CDC, append flows, materialized view, temporary view. Decide triggered, continuous or Real-Time Mode. See `references/lakehouse-patterns.md`.
4. **Design idempotency.** Every write must be safe to re-run. Use the platform mechanism (checkpoint, Auto CDC, MV recompute) before hand-written `MERGE` or `REPLACE WHERE`. See `references/idempotency.md`.
5. **Plan incremental reads.** Let Auto Loader and streaming checkpoints hold state. Use a manual watermark only for sources that cannot stream, and read it from the target table. See `references/ingestion-patterns.md`.
6. **Set a schema evolution policy per layer.** Additive in bronze, explicit in silver, a strict contract in gold. See `references/schema-evolution.md`.
7. **Add data quality gates** as expectations at layer boundaries, with the action (warn, drop, fail, quarantine) chosen deliberately. See `references/data-quality.md`.
8. **Design orchestration.** Pipelines resolve their own dependencies; use Lakeflow Jobs for triggers, cross-pipeline order, retries, timeouts and alerts. See `references/orchestration.md`.
9. **Choose compute.** Serverless by default, `performance_target` by SLA, classic only for a named blocker. See `references/orchestration.md`.
10. **Define deployment** as a bundle: resources, variables for catalog and schema, targets, `run_as`, permissions. See `references/orchestration.md`.
11. **Plan backfill and recovery.** Parameterised runs, one-time append flows, selective refresh, repair runs, `RESTORE`. Full refresh needs explicit approval.
12. **Govern and observe.** Grants, masks and tags, event log, system tables, cost levers. See `references/governance-and-observability.md`.
13. **Plan serving.** Which gold columns and grain each consumer needs. See `references/serving-and-consumers.md`.
14. **Validate.** `databricks bundle validate --strict -t <target> --profile <profile>`, then deploy to a dev target and run. Poll the update, not the pipeline's top-level state.

## Quick Heuristics

- **Land raw, append-only, keep it.** Bronze is your replay log; never mutate it.
- **Pick the dataset type first.** Append-only growth is a streaming table, changes by key are Auto CDC, whole-dataset recompute is a materialized view.
- **One business key, everywhere.** Auto CDC, `MERGE` and dedup all hinge on it.
- **Serverless first.** Leave out cluster settings; justify classic with a named blocker.
- **Quality gates are part of the pipeline.** A failed critical expectation must stop promotion.
- **Make every task retry-safe before adding retries.**
- **Parameterise catalog and schema.** Never hardcode them; use bundle variables.
- **Triggered before continuous.** Move to continuous only when the SLA demands it.
- **Keep gold dimensions.** Over-aggregating loses what dashboards and Genie need to filter by.

## Best Practices

- Use the modern API: `from pyspark import pipelines as dp`, `@dp.table`, `@dp.materialized_view`, `@dp.temporary_view`, `dp.create_auto_cdc_flow`. Migrate legacy `dlt` code; do not extend it.
- Use liquid clustering (`cluster_by`, `CLUSTER BY AUTO`), not `partition_cols`.
- Keep one dataset per file, named after the dataset, in a `transformations/` folder.
- Use `@dp.append_flow` for multiple sources into one table; never `UNION` streams.
- Use a temporary view for intermediate logic, including pre-filtering before Auto CDC.
- Authenticate to cloud storage with UC service credentials; keep secrets in secret scopes.
- Tag jobs and pipelines (team, environment) for cost attribution.
- Run production as a service principal (`run_as`).

## Common Pitfalls

- **Blind appends on retry** duplicate rows. Use Auto CDC, `MERGE`, or `REPLACE WHERE`.
- **Changing a dataset's type** (streaming table to materialized view or back) cannot be done in place, and full refresh does not fix it.
- **Full refresh of a streaming table** discards streaming state and can lose data when the source no longer holds the history.
- **Aggregating over a streaming source in a streaming table** never recomputes; use a materialized view with a batch read.
- **Setting `cloudFiles.schemaLocation`, a checkpoint, or `.writeStream` inside a pipeline** conflicts with the managed ones.
- **Treating Zerobus as exactly-once.** It is at-least-once; dedupe downstream.
- **Treating Lakeflow Connect as continuous.** It is triggered-only; schedule it with a Jobs `pipeline_task`.
- **Using `CREATE TABLE ... FROM CONNECTION` for Lakeflow Connect.** That is Federation syntax; Connect uses `ingestion_definition`.
- **Reading an updated or deleted upstream table as a stream** fails without `skipChangeCommits`.
- **Expectations that only warn** let bad data reach consumers; use drop or fail for anything load-bearing.
- **No backfill story.** Parameterise by date or run a one-time flow from day one.
- **Hardcoded catalogs or schemas** in code or YAML break multi-environment deploys.
- **Classic compute "by habit"**; it costs more and starts slower than serverless for most jobs.

## Bundled Resources

- `references/lakehouse-patterns.md`: ELT, dataset-type decision, triggers, table formats, layout.
- `references/ingestion-patterns.md`: ingestion path choice, Auto Loader, Lakeflow Connect, Zerobus, Auto CDC, manual watermarks, Structured Streaming fallback.
- `references/idempotency.md`: per-write idempotency patterns, recovery and rollback, checklist.
- `references/schema-evolution.md`: Auto Loader modes, Delta schema features, rescue quarantine, contracts.
- `references/orchestration.md`: Lakeflow Jobs, triggers, retries, backfill and repair, pipeline and job YAML, compute, deployment.
- `references/data-quality.md`: expectations, quarantine, event-log queries, freshness and volume checks, testing the checks.
- `references/governance-and-observability.md`: Unity Catalog structure and grants, masks, system tables, cost levers.
- `references/serving-and-consumers.md`: metric views, dashboards, Genie, Lakebase, Iceberg readers.
- `templates/pipeline-design.md`: fill-in design document.
- `examples/orders-pipeline.md`: worked end-to-end example with code and bundle YAML.
