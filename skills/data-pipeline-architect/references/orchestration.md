# Orchestration, Compute and Deployment

On Databricks the orchestrator is **Lakeflow Jobs**, the transforms are **SDP pipelines**, and both are deployed as code with **Declarative Automation Bundles (DABs**, formerly Databricks Asset Bundles). Load `databricks-jobs`, `databricks-dabs` and `databricks-serverless` before writing YAML.

## Layering: medallion on Unity Catalog

```
source ─ingest─▶ BRONZE (raw, append-only streaming tables, minimal typing)
        ─clean──▶ SILVER (deduped via Auto CDC, typed, validated by expectations)
        ─model──▶ GOLD   (materialized views / marts for BI, ML, reverse ETL)
```

- Bronze is the replay log; never mutate it.
- Silver is where dedup and quality gates live.
- Gold is consumer-facing and contract-stable; keep business dimensions.
- Separate layers by catalog or schema, and parameterise them with bundle variables (`${var.catalog}`, `${var.schema}`). Never hardcode them.
- SDP resolves dependencies **inside** one pipeline from the table references, so do not model them as job tasks. Use job tasks for ordering **between** pipelines and non-pipeline work.
- One dataset per file, named after the dataset.

### How big should one pipeline be?

A triggered update runs at most **16 dataset updates in parallel**; more queue and stretch wall-clock time even when compute is free.

- **Keep together:** one domain's connected DAG that shares a cadence and trigger, under the 16-update ceiling.
- **Split** at ownership (team or domain), layer (bronze ingestion apart from silver/gold, so an ingestion failure does not block transformation), and latency (continuous apart from daily batch) boundaries, or when a pipeline would pass 16 parallel updates.
- Prefer a few medium, domain-aligned pipelines over one monolith. Order split pipelines with job tasks, and watch them together through `system.lakeflow.pipelines` and `system.lakeflow.job_run_timeline`.

## What orchestrates what

| Need | Use |
|---|---|
| Transform datasets and their dependencies | One SDP pipeline |
| Schedule or trigger a pipeline | A Lakeflow Job with a `pipeline_task` |
| Run order across pipelines, notebooks, SQL, dbt, wheels | Job tasks with `depends_on` |
| Fan out over a list (regions, tables) | `for_each_task` |
| Run on a clock | `schedule` (cron) or `trigger.periodic` |
| Run when upstream data lands | `trigger.file_arrival` or `trigger.table_update` |
| Always-on | Continuous pipeline, or a continuous job |

Prefer data-aware triggers (`table_update`, `file_arrival`) over fixed cron when the upstream has no fixed time, to avoid a downstream job running before fresh data lands.

## Job skeleton (DAB)

```yaml
# resources/orders.job.yml
resources:
  jobs:
    orders_refresh:
      name: "[${bundle.target}] Orders refresh"
      max_concurrent_runs: 1
      queue:
        enabled: true
      performance_target: STANDARD          # PERFORMANCE_OPTIMIZED only for tight SLAs
      timeout_seconds: 7200
      health:
        rules:
          - metric: RUN_DURATION_SECONDS
            op: GREATER_THAN
            value: 3600
      email_notifications:
        on_failure: ["${var.notification_email}"]
        on_duration_warning_threshold_exceeded: ["${var.notification_email}"]
      trigger:
        pause_status: UNPAUSED
        table_update:
          table_names: ["${var.catalog}.${var.schema}.bronze_orders"]
          condition: ANY_UPDATED
          min_time_between_triggers_seconds: 300
      tags:
        team: data-engineering
      tasks:
        - task_key: refresh_pipeline
          max_retries: 2
          min_retry_interval_millis: 60000
          pipeline_task:
            pipeline_id: ${resources.pipelines.orders_pipeline.id}
            full_refresh: false
        - task_key: gold_checks
          depends_on:
            - task_key: refresh_pipeline
          run_if: ALL_SUCCESS
          sql_task:
            warehouse_id: ${var.warehouse_id}
            file:
              path: ../src/checks/gold_checks.sql
```

Declare every `${var.*}` you use (`catalog`, `schema`, `notification_email`, `warehouse_id`, ...) under `variables:` in `databricks.yml` and set values per target.

Triggers: `schedule` (Quartz cron, `pause_status`), `trigger.periodic` (interval of at least 1 hour; units HOURS, DAYS, WEEKS), `trigger.file_arrival` (storage URL or `/Volumes/...`), `trigger.table_update` (UC tables; the job identity needs `SELECT`), `continuous`. A job can combine triggers. With `max_concurrent_runs: 1` and `queue.enabled: true`, extra runs queue instead of being skipped.

`run_if` values: `ALL_SUCCESS` (default), `ALL_DONE`, `AT_LEAST_ONE_SUCCESS`, `NONE_FAILED`, `ALL_FAILED`, `AT_LEAST_ONE_FAILED`.

## Retries, timeouts, alerts

- **Retries:** `max_retries`, `min_retry_interval_millis`, `retry_on_timeout` on a task. Make the task idempotent first (see `idempotency.md`); retries amplify non-idempotent bugs.
- **Pipeline retries:** the platform's defaults (several update and flow retries) absorb transient failures. For dev iteration you may set `pipelines.numUpdateRetryAttempts` and `pipelines.maxFlowRetryAttempts` to `"0"` to fail fast; remove those overrides for production.
- **Timeouts:** `timeout_seconds` at job and task level (`0` means none). A hung task blocks the schedule.
- **Alerts:** `email_notifications` / `webhook_notifications` (create the notification destination first), `health.rules` for run duration and streaming backlog, `notification_settings` to suppress skipped or cancelled noise. Pipelines have their own `notifications` (`on-update-failure`, `on-flow-failure`, ...).
- **SLA and freshness:** a run can succeed with stale data. Check freshness separately (see `data-quality.md`).

## Parameters and backfill

- Define job-level `parameters` (they reach every task) and read them with `dbutils.widgets.get`, or pass them as task `base_parameters` using `{{job.parameters.name}}`.
- Backfill a range by running the **same** job over historical parameters. Idempotent writes guarantee no duplicates.

```bash
databricks jobs run-now --json '{"job_id": 123, "job_parameters": {"start_date": "2026-01-01", "end_date": "2026-01-31"}}' --profile <PROFILE>
```

- In a pipeline, load history with a one-time append flow (`@dp.append_flow(once=True)`) beside the live flow.
- Prefer **selective refresh** of one table over full refresh. Full refresh destroys streaming state and can lose data; never run it without explicit approval.
- Throttle concurrent backfill runs (`max_concurrent_runs`) so you do not hammer sources or warehouses.
- **Repair** a failed multi-task run to re-run only the failed and skipped tasks. Repair applies to jobs with two or more tasks, uses current settings, and does not make tasks idempotent.

## Pipeline resource (DAB)

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
      channel: CURRENT
      root_path: ../src/orders_pipeline
      libraries:
        - glob:
            include: ../src/orders_pipeline/transformations/**
      configuration:
        source_catalog: ${var.source_catalog}
        source_schema: ${var.source_schema}
      notifications:
        - email_recipients: ["${var.notification_email}"]
          alerts: ["on-update-failure", "on-update-fatal-failure", "on-flow-failure"]
      tags:
        team: data-engineering
```

Source layout under `root_path`:

```
src/orders_pipeline/
  transformations/   # dataset definitions only; every file here is evaluated on each update
  utilities/         # plain PySpark functions, importable and unit-tested with pytest
  explorations/      # notebooks and ad hoc queries; never referenced by libraries
```

Keep transformation logic in `utilities/` as plain functions that take and return DataFrames, and make each `transformations/` file a thin `@dp` wrapper around them.

- Use `schema`, not the deprecated `target`; new pipelines use the default publishing mode. Read `configuration` values in Python with `spark.conf.get("source_catalog")` and in SQL with `${source_catalog}`; values are strings. SQL can also use the pipeline `parameters` map as `:source_catalog` or `IDENTIFIER(:source_catalog)`. Job run parameters override job parameters, which override pipeline task parameters, which override pipeline parameters.
- Declare Python dependencies in the pipeline's environment, pinned with `==`, never `%pip install` in source files.
- Use `root_path` plus `libraries.glob`. Dataset names are unqualified; use a fully qualified `catalog.schema.table` name only to write to a different location than the pipeline default.
- `development: true` suits dev targets (faster startup, no retry-on-failure). Production targets leave it off.
- `run_as` takes a user or a service principal (pipelines also accept an account group); use a service principal for production.
- Lakeflow Connect ingestion pipelines carry `ingestion_definition` instead of `libraries` and are never continuous.

## Compute

- **Serverless is the default** for jobs and pipelines. Omit `new_cluster`, `job_clusters`, `node_type_id`, `num_workers` so tasks run serverless. For a pipeline use `serverless: true` and no `clusters` block.
- Use classic compute only for R or Scala notebook cells, custom Docker images, OS packages or native libraries without a pip equivalent, custom Spark data-source JARs, a stated instance or runtime requirement, or a workspace without serverless. Give only the blocked task a cluster.
- Script, wheel and JAR tasks take libraries from a job-level `environments` block with a pinned `environment_version` and dependencies; notebook tasks need nothing extra.
- `performance_target`: `STANDARD` (default; scheduled batch, no tight deadline, starts within minutes and costs less) or `PERFORMANCE_OPTIMIZED` (a named SLA, a person waiting, or runs every 30 minutes or more often). It applies to the whole job, including a `pipeline_task`'s update.
- Serverless code: DataFrame/SQL APIs only (no RDDs or `sparkContext`), three-part UC names, `/Volumes/...` paths instead of DBFS mounts, no `.cache()` / `.persist()`, pinned dependencies, never init scripts. To move an existing job, use `databricks-serverless-migration`.

## Deployment workflow

```bash
databricks bundle validate --strict -t dev --profile <PROFILE>
databricks bundle deploy   -t dev --profile <PROFILE>
databricks bundle run orders_refresh -t dev --profile <PROFILE>
databricks bundle summary  -t dev --profile <PROFILE>
```

- Resource files are named `<name>.<type>.yml` (`orders.job.yml`, `orders_pipeline.pipeline.yml`) under `resources/` and included from `databricks.yml` with `include: [resources/*.yml]`.
- Path resolution: use `../src/...` inside `resources/*.yml`, and `./src/...` in `databricks.yml`.
- Parameterise catalog, schema and warehouse with `variables:`, set per `targets:` (`dev` in development mode, `prod` in production mode). A `mode: production` target must set `workspace.root_path` or validation fails. Use the deploying identity's folder, for example `/Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}`; `/Workspace/Shared` is writable by all users and fails `--strict`.
- Run `bundle validate --strict` after any configuration change. Code changes only take effect after `bundle deploy`.
- Poll the **update** (`databricks pipelines get <id>`; the update's own state and its `error.exceptions[0].message`), not the pipeline's top-level state. A first serverless run can sit in `INITIALIZING` for a few minutes; that is normal.
- `bundle destroy` is destructive; confirm the target first. Deleted UC pipelines keep their tables by default, so plan to drop them separately.
- Pass `--profile <PROFILE>` explicitly and let the user choose; never auto-select one.

## Observability

Emit and watch: run duration, rows in and out, expectation results, freshness lag, failures. See `governance-and-observability.md`.
