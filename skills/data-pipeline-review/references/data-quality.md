# Data Quality Review (Databricks)

## Purpose

Check that bad data cannot silently reach consumers. On Databricks the primary tools are SDP expectations, table constraints, Auto Loader rescued data, and Unity Catalog data quality monitoring. A failed critical check must stop promotion to the next layer or quarantine the batch.

Load `databricks-pipelines` for expectation syntax and `databricks-unity-catalog` (`references/7-data-profiling.md`) for monitoring. Design guidance: `data-pipeline-architect` `references/data-quality.md` if installed.

## When to use

- A pipeline feeds dashboards, ML features or reports that people rely on.
- Bad data (nulls in keys, duplicates, out-of-range values, stale or short loads) reached a consumer.
- A new source, schema change or gold table is being added.
- Checks exist but nobody knows what happens when they fail.

## Procedure

### 1. Inventory the checks

```bash
# Expectations, constraints, quarantine, rescue column, alerts, monitors
grep -rnE "@dp\.expect|expect_all|CONSTRAINT .* EXPECT|ON VIOLATION|ADD CONSTRAINT|CHECK \(|SET NOT NULL|_rescued_data|QUALITY MONITOR|quarantine|raise_error" . --include='*.py' --include='*.sql' --include='*.yml' --include='*.ipynb'
```

### 2. Map gates to layers

Expect gates at each boundary: ingest to bronze (schema match, row count, freshness), bronze to silver (keys not null, valid values, dedup), silver to gold (referential checks, reconciliation, metric sanity), gold to consumers (freshness, volume, constraints). Note which boundaries have none.

### 3. Audit the action against consumer criticality

| Action | Meaning | Acceptable for |
|---|---|---|
| warn (`@dp.expect`, `EXPECT`) | rows kept, metrics logged | soft quality being measured |
| drop (`expect_or_drop`, `ON VIOLATION DROP ROW`) | violating rows removed | cleansing where some loss is acceptable, ideally with quarantine |
| fail (`expect_or_fail`, `FAIL UPDATE`) | the update stops | invariants that must never reach consumers |

Warn-only on a load-bearing table is a finding. Expectation conditions must be SQL expressions: no Python UDFs, subqueries over other tables or external calls; aggregate checks (uniqueness, row counts, missing keys) belong in a materialized view with `expect_or_fail`.

Also flag:

- Expectations where they do not run: sinks, `AUTO CDC ... FROM SNAPSHOT`, tables targeted by a `REPLACE WHERE` flow. They silently check nothing.
- `expect_or_fail` relied on to halt the whole pipeline. In triggered mode only the failing flow rolls back; independent flows still publish. Work that must stop needs a separate pipeline or a job task with `depends_on`.
- Datasets that can receive bad data with no expectation at all.
- Rules copied between datasets by hand instead of loaded from one table or module (`get_rules(tag)` pattern).

### 4. Check quarantine

Dropped rows should land somewhere observable. Confirm the quarantine predicate uses `coalesce(..., false)` so NULL results go to quarantine rather than vanishing, that it is derived from the same rules as the clean path (a hand-written inverse drifts), and that something alerts when quarantine grows.

### 5. Check what is not a row rule

| Category | Databricks mechanism to look for |
|---|---|
| Schema | Auto Loader schema evolution mode and `_rescued_data`; explicit silver schema; Delta constraints |
| Uniqueness | Auto CDC keys or a gold test (an expectation is a weak way to enforce it) |
| Referential | Join check in a gold test or materialized view; PK/FK constraints are informational only |
| Freshness, volume | Unity Catalog data quality monitoring (anomaly detection, enabled per schema, serverless, billed separately) or a scheduled SQL alert on `max(_ingested_at)` and trailing row counts |
| Distribution, drift | Data profiling (`QUALITY MONITOR`) with drift metrics |
| Reconciliation | A job task that compares control totals and fails with `raise_error` |

### 6. Check whether results are visible and the checks are tested

Expectation metrics appear in the pipeline Data quality tab and the event log (`event_log('<pipeline_id>')`, `flow_progress` events). Confirm someone reads them. Confirm the checks were tested, with pipeline Python unit tests (Beta) or with injected bad rows (nulls in keys, negative amounts, malformed JSON, a new column), for example generated with `databricks-synthetic-data-gen`.

## Concrete checks

- [ ] Every consumed table has a declared schema; `NOT NULL` and `CHECK` constraints enforced where invariants exist.
- [ ] Keys are not null and unique (by Auto CDC key, `MERGE` key or a test).
- [ ] Critical expectations fail or quarantine; warn is limited to soft rules.
- [ ] A quarantine path exists and is monitored.
- [ ] A freshness check and a volume check cover each gold table, measured on event or ingest time, not only run success.
- [ ] A reconciliation control total compares source and gold.
- [ ] Schema changes are handled by an explicit policy per layer (additive in bronze, explicit in silver, contract in gold).
- [ ] Checks live in version control and a severity change is reviewed.
- [ ] Checks were exercised with bad rows before production.

## Anti-patterns

- Checks that run after publish, so consumers already read the bad data.
- `expect` (warn) on a primary key or amount column.
- Uniqueness enforced only in code that a retry bypasses.
- Freshness measured by the job's finish time, hiding a stalled upstream.
- Quarantine tables nobody reads.
- Expectations assumed to run on a failed update; failed updates may not report metrics.
- PK/FK constraints treated as enforced.
- Monitoring promised without checking availability and cost in the workspace.

## Required output

1. **Coverage matrix**: table by layer by check type, present or missing.
2. **Failure-mode audit**: action per check against consumer criticality.
3. **Freshness, volume, reconciliation**: what detects silent partial loads.
4. **Findings table**: `severity | file:line | gap | risk | fix | skill`.
5. **Next safe action**.

## Safety

- Do not relax, drop or downgrade an existing failing check to make a run pass.
- Do not run an update, alert or monitor refresh against production to test a check; propose it against a dev target.
- Redact sample rows; reference columns by name.
- Do not change a check's action on a production table without approval.
