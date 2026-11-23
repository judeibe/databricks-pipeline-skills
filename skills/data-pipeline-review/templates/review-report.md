# Databricks Review Report

- **Scope:** <bundles, pipelines, jobs, notebooks, models reviewed>
- **Targets reviewed:** <dev / qa / prod, each target's mode and run_as>
- **Profile used:** <name, or "none (code-only review)">
- **Reviewed against:** <product skills and references loaded>

## 1. Surface map

| Bundle | Resource | Type (pipeline / job / notebook / model / endpoint) | Trigger or cadence | Output |
|---|---|---|---|---|

## 2. Idempotency verdict

| Write (file:line) | Mechanism (checkpoint / Auto CDC / MV / MERGE / REPLACE WHERE / append) | Safe to re-run? | Note |
|---|---|---|---|

## 3. Incremental correctness

State source (checkpoint, Auto Loader, target-table watermark), late-data handling, upstream update or delete handling, at-least-once sources and their dedupe.

## 4. Orchestration and compute safety

| Job or pipeline | Retries | Timeout | Concurrency / queue | Trigger fit | Alerts | Compute (serverless / classic + blocker) | performance_target | run_as |
|---|---|---|---|---|---|---|---|---|

## 5. Backfill and recovery plan

Parameter path or one-time flow, refresh mode (selective or full), rollback method and retention window, blast radius.

## 6. Area findings (include only the areas reviewed)

Data quality, lineage and governance, ML training, serving, notebooks and code: one short paragraph each, linking to the findings below.

## 7. Findings

| Severity | File:line | Issue | Risk | Fix | Skill or reference |
|---|---|---|---|---|---|
| blocker / major / minor | | | | | |

Severity guide: **blocker** is data loss, duplication, or a destructive default (scheduled full refresh, blind append on a retryable task); **major** is wrong or unsafe behaviour that will bite; **minor** is idiom or maintainability.

## 8. Next safe action

The single highest-priority fix, and the command or edit that applies it (not run during review).

## Not verified

List anything that needed workspace access or approval that was not given (event log, system tables, endpoint state, `bundle validate`).
