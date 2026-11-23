---
name: data-pipeline-review
description: Reviews and audits existing Databricks data and ML pipelines against Databricks methodology - Lakeflow Spark Declarative Pipelines (streaming tables, materialized views, Auto CDC, expectations), Auto Loader, Lakeflow Jobs, Declarative Automation Bundles, serverless compute, Unity Catalog, MLflow and Model Serving. Use this skill when the user asks to review, audit, check or find problems in a Databricks pipeline, job, bundle, notebook or model; investigate duplicate rows, gaps, stale data, wrong counts or schema drift; verify idempotency, retries, repair or backfill safety; check expectations, quarantine, freshness or reconciliation; review run_as, permissions, compute, catalogs or PII handling; check lineage and reproducibility; or audit MLflow training (leakage, registry aliases) and model serving rollout. For designing a new pipeline use data-pipeline-architect.
license: MIT
---

# Data Pipeline Review (Databricks)

## Purpose

Audit existing Databricks work and report what is unsafe, wrong or non-idiomatic, with a concrete fix for each finding. The standard is the Databricks methodology: let the platform provide idempotency, state and quality gates before hand-written code, and treat anything that needs a human to remember a step as a risk.

This skill is the review counterpart to `data-pipeline-architect`, which designs pipelines. It does not restate API syntax. Each finding points to the product skill or architect reference that holds the fix.

## Prerequisites

1. Load **`databricks-core`** first (CLI, auth, bundles).
2. **Never auto-select a CLI profile.** List them (`databricks auth profiles`), let the user choose, and pass `--profile <name>` on every command (each Bash call is a separate shell).
3. A code-only review needs no profile. Anything that reads the workspace (event log, system tables, `bundle validate`, `jobs get-run`) does.
4. Load the product skills the surface touches (routing table) before judging against them.

## Skill routing

| Concern | Load |
|---|---|
| Datasets, Auto CDC, Auto Loader, expectations | `databricks-pipelines` |
| Plain Structured Streaming | `databricks-spark-structured-streaming` |
| Jobs, tasks, triggers, retries, repair, notifications | `databricks-jobs` |
| Bundle files, targets, variables, permissions, deploy | `databricks-dabs` |
| Compute, `performance_target`, classic blockers | `databricks-serverless` (`databricks-serverless-migration` for existing jobs) |
| Catalogs, grants, masks, tags, lineage, system tables, data profiling | `databricks-unity-catalog` |
| Managed ingestion, push ingestion | `databricks-lakeflow-connect`, `databricks-zerobus-ingest` |
| Training, registry, features, batch scoring | `databricks-ml-training` |
| Endpoint rollout, readiness, traffic | `databricks-model-serving` (`databricks-unity-gateway` for Unity Gateway) |
| Model and agent evaluation gates | `databricks-mlflow-evaluation` |
| Running code, local setup, SDK | `databricks-execution-compute`, `databricks-setup-local`, `databricks-python-sdk` |
| Bad-row test data | `databricks-synthetic-data-gen` |
| Anything else | `databricks-docs` |

Fix guidance for designs lives in the `data-pipeline-architect` skill (its idempotency, orchestration, data-quality, schema-evolution and governance-and-observability references). Use it when installed; the checks below stand alone.

## Area references

Open only the reference that matches the surface under review.

| Surface | Reference |
|---|---|
| Expectations, quarantine, freshness, volume, reconciliation, contracts | `references/data-quality.md` |
| Reproducibility, Delta versions, lineage, tags, PII, grants | `references/lineage-and-governance.md` |
| MLflow training, leakage, registry, features | `references/ml-training.md` |
| Batch scoring, endpoints, rollout, monitoring | `references/model-serving.md` |
| Notebooks, source layout, parameters, secrets, tests | `references/notebooks-and-code.md` |

Report format: `templates/review-report.md`.

## Procedure

### 1. Discover the Databricks surface

Find assets by signature, not by path. Layouts differ between repos, and a repo can hold several bundles.

```bash
# Bundles and the resources they include
find . -name databricks.yml -not -path '*/node_modules/*' -not -path '*/.databricks/*'
find . \( -name '*.job.yml' -o -name '*.pipeline.yml' -o -path '*/resources/*.yml' \) -not -path '*/.databricks/*'

# Pipeline code: modern API, legacy DLT, SQL datasets
grep -rlE "from pyspark import pipelines|import dlt|CREATE OR REFRESH|CREATE (LIVE|STREAMING) " . --include='*.py' --include='*.sql'

# Notebooks in both formats
find . -name '*.ipynb' -not -path '*/.ipynb_checkpoints/*'
grep -rl "^# Databricks notebook source" . --include='*.py' --include='*.sql'

# Writes, state and ML
grep -rnE "MERGE INTO|REPLACE WHERE|INSERT (INTO|OVERWRITE)|saveAsTable|cloudFiles|checkpointLocation|txnVersion" . --include='*.py' --include='*.sql' --include='*.ipynb'
grep -rlE "import mlflow|mlflow\." . --include='*.py' --include='*.ipynb'
```

For each bundle list: pipelines, jobs, tasks and their types, targets (`mode`, `run_as`, `workspace.root_path`), variables, owners. If nothing Databricks-shaped is found, say so and stop; do not fall back to a generic review. In a multi-bundle repo, review each bundle against its own targets.

### 2. Check load semantics (idempotency)

For every write, classify the mechanism. Preferred order: streaming table checkpoint, Auto CDC, materialized view recompute, then `MERGE`, `INSERT ... REPLACE WHERE`, `CREATE OR REPLACE TABLE`, `foreachBatch` with `txnVersion` + `txnAppId`.

- Flag a blind `INSERT INTO` / `.mode("append")` in any task that can retry or be repaired.
- Flag legacy DLT (`import dlt`, `apply_changes`, `LIVE.`, `partition_cols`); recommend the modern `dp` API and `cluster_by`.
- Flag `UNION` of streams instead of `@dp.append_flow`.
- Flag `MERGE` without a stable key or a recency tiebreaker.
- Flag `REPLACE WHERE` whose predicate differs from the grain of the `SELECT`.

### 3. Check incremental state

- Streaming tables and Auto Loader hold state in checkpoints. A hand-rolled watermark is justified only for sources that cannot stream, and must be read from the target table inside the retry-safe step.
- Flag a pipeline that sets its own checkpoint location, `cloudFiles.schemaLocation` or `.writeStream`.
- Flag a streaming read of an upstream table that gets updates or deletes without `skipChangeCommits`.
- Flag Zerobus or message-bus ingest with no downstream dedupe (those sources are at-least-once).
- Flag aggregation over a streaming source inside a streaming table; that needs a materialized view with a batch read.

### 4. Check orchestration safety

Per job and task: `max_retries` and `min_retry_interval_millis` are set deliberately and the task is idempotent; `timeout_seconds` is set; `max_concurrent_runs` with `queue.enabled` stops overlapping runs from writing the same data; `run_if` matches intent; triggers fit the upstream (`table_update` / `file_arrival` over fixed cron where no fixed time exists); failure notifications and `health.rules` exist. SDP resolves dependencies inside one pipeline, so job tasks should order work between pipelines and non-pipeline work, not between datasets of one pipeline. Lakeflow Connect ingestion pipelines are triggered only and need a job to schedule them.

### 5. Check backfill and recovery

- A job-parameter or one-time-flow path exists (`@dp.append_flow(once=True)`) and runs through the same idempotent writes.
- Selective refresh is preferred. **Any full refresh (`--full-refresh`, `full_refresh: true`) in a schedule or script is a blocker**: it destroys streaming state and can lose data when the source no longer holds history.
- Rollback exists: `DESCRIBE HISTORY` / `RESTORE`, within the table's retention window.
- Repair of failed runs is safe because tasks are idempotent.
- A streaming table cannot be converted to a materialized view in place; flag any plan that assumes it.

### 6. Check compute and deployment

- Serverless by default; classic only with a named blocker (R/Scala cells, custom images, OS packages, custom JARs, required runtime or instance). `performance_target` fits the SLA (`STANDARD` unless a deadline or a person is waiting).
- Script, wheel and JAR tasks use a job-level `environments` block with a pinned version and pinned dependencies.
- Production targets run as a **service principal** (`run_as`), not a person; `workspace.root_path` is set and is not `/Workspace/Shared`.
- Catalog, schema and warehouse come from bundle variables, never hardcoded.
- Resources are tagged (team, environment); permissions are explicit.
- `databricks bundle validate --strict -t <target> --profile <profile>` passes (run only once the user has chosen a profile).

### 7. Route to the area references

Open the matching reference for quality gates, lineage and governance, ML training, serving, and notebooks. Fold their findings into the one report.

## Concrete checks

- [ ] Every write is idempotent by platform mechanism or a keyed `MERGE` / `REPLACE WHERE`; no blind append on a retryable path.
- [ ] No legacy `dlt`, `apply_changes`, `LIVE.` or `partition_cols`; liquid clustering is used.
- [ ] No pipeline-owned checkpoint or schema location; no UNION of streams.
- [ ] Incremental state comes from checkpoints or from the target table, not wall-clock time.
- [ ] Retries are bounded, tasks have timeouts, overlapping runs are prevented or queued.
- [ ] Triggers are data-aware where upstream timing is not fixed.
- [ ] Failures notify someone; stale data is checked separately from run success.
- [ ] Backfill is parameterised or a one-time flow; no scheduled full refresh.
- [ ] Serverless unless a blocker is named; `performance_target` matches the SLA; environments pinned.
- [ ] Production `run_as` is a service principal; `root_path` is not shared; variables carry catalog and schema.
- [ ] Critical expectations fail or quarantine; none are warn-only on load-bearing tables.
- [ ] Models load by alias, not `@latest` or stage; sensitive columns are tagged and masked.

## Common issues & anti-patterns

- Blind `INSERT` / append inside a task with retries, so every retry duplicates rows.
- A scheduled job or script that passes `--full-refresh`.
- `import dlt` code extended instead of migrated to `from pyspark import pipelines as dp`.
- Expectations that only warn, so bad rows reach gold.
- Cron schedules that start before upstream data lands.
- Classic clusters "by habit"; `/Workspace/Shared` as a production root; a person as `run_as` in prod.
- Hardcoded catalog or schema names that break multi-environment deploys.
- Training that registers a model version per tuning trial, or serving that loads `@latest`.
- Logic buried in notebooks with no tests and no parameters.

## Required output

Produce the report in `templates/review-report.md`:

1. **Surface map**: bundles, pipelines, jobs, notebooks, models, targets.
2. **Idempotency verdict**: per write, mechanism and whether re-runs are safe.
3. **Incremental correctness**: state source, late data, ordering.
4. **Orchestration and compute safety**: retries, concurrency, triggers, alerts, serverless, `run_as`.
5. **Backfill and recovery plan**: parameters, refresh mode, rollback, blast radius.
6. **Findings table**: `severity | file:line | issue | risk | fix | skill or reference`. Severity is blocker (data loss, duplication, destructive default), major, or minor.
7. **Next safe action**: the single highest-priority fix.

## Safety

- Review is read-only. Never run `bundle deploy`, `bundle run`, `bundle destroy`, `jobs run-now`, `pipelines start-update`, any full refresh, `RESTORE`, `VACUUM`, `GRANT`/`REVOKE`, endpoint create or update, or alias moves without explicit approval. Describe them instead.
- `--help`, `bundle validate --strict`, `pipelines get`, `jobs get-run` and read-only SQL are acceptable once the user has chosen a profile.
- Treat tokens, secret values, connection strings and sample data as sensitive: name the variable or column, redact the value.
- Never relax or delete a failing expectation or constraint to make a run green; investigate the data.
- Do not change schedules, triggers, permissions or production targets without sign-off.

## Foreign tooling to Databricks equivalent

When reviewing legacy or migrating code, recommend the Databricks mechanism:

| Found | Use instead |
|---|---|
| Airflow, Dagster, Prefect, cron | Lakeflow Jobs with a `pipeline_task`, triggers and bundle-managed resources |
| dbt models | A dbt task in a job, or SDP datasets |
| Great Expectations, pandera, Soda | SDP expectations, table constraints, data quality monitoring |
| DVC, lakeFS, data files in git | Delta tables and volumes in Unity Catalog, table versions, lineage |
| papermill, nbconvert | Job parameters and notebook tasks |
| FastAPI or BentoML serving, `mlflow models serve` | Model Serving endpoints, or batch scoring to a gold table |
| Hand-rolled watermarks | Auto Loader, streaming checkpoints, Auto CDC |
