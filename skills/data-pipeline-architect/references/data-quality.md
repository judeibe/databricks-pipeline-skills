# Data Quality

Quality checks are part of the pipeline, not a cron afterthought. A failed critical check must **stop promotion** to the next layer or quarantine the batch. Warn-only checks let bad data through; avoid them for anything load-bearing.

## Where to place gates

```
ingest ─▶ [schema match (rescue column), row count, freshness]   ─▶ bronze
bronze ─▶ [expectations: keys not null, valid values, dedup ok]   ─▶ silver
silver ─▶ [referential checks, reconciliation, metric sanity]     ─▶ gold
gold   ─▶ [freshness, volume, contract / constraints]            ─▶ consumers
```

## Expectations (the primary tool)

Expectations are SQL Boolean conditions attached to a streaming table, materialized view or view. Pick the action by consequence:

| Action | Python | SQL | Use for |
|---|---|---|---|
| Warn (rows kept, metrics logged) | `@dp.expect` / `expect_all` | `CONSTRAINT n EXPECT (cond)` | Soft quality you want to measure |
| Drop violating rows | `@dp.expect_or_drop` / `expect_all_or_drop` | `... ON VIOLATION DROP ROW` | Cleansing where some loss is acceptable |
| Fail the update atomically | `@dp.expect_or_fail` / `expect_all_or_fail` | `... ON VIOLATION FAIL UPDATE` | Invariants that must never reach consumers |

```python
@dp.materialized_view(comment="Clean customers")
@dp.expect_or_fail("required_id", "id IS NOT NULL")
@dp.expect_or_drop("valid_email", "email LIKE '%@%'")
@dp.expect("valid_age", "age BETWEEN 0 AND 120")
def customers_clean():
    return spark.read.table("raw_customers")
```

```sql
CREATE OR REFRESH STREAMING TABLE customers_clean (
  CONSTRAINT required_id EXPECT (id IS NOT NULL)      ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_email EXPECT (email LIKE '%@%')    ON VIOLATION DROP ROW,
  CONSTRAINT valid_age   EXPECT (age BETWEEN 0 AND 120)
) AS SELECT * FROM STREAM(raw_customers);
```

Rules:

- Names are unique within the dataset and show up in metrics, so make them descriptive.
- The condition is a SQL expression that returns true or false per row: built-in functions, `CASE` and date arithmetic are fine; **no** Python UDFs, subqueries over other tables or external calls. For checks that need other tables (row counts, missing keys, uniqueness), compute them in a materialized view and put the expectation on that (see "Dataset-level checks").
- Group same-action checks with `expect_all*`; stack decorators for mixed actions.
- On an explicit-target streaming table, use `dp.create_streaming_table(name=..., expect_all_or_drop={...})`.
- Expectations run on streaming tables, materialized views and temporary views. They do **not** run on sinks, on `AUTO CDC ... FROM SNAPSHOT`, or on a table targeted by a `REPLACE WHERE` flow; validate in an upstream view instead. In SQL, put constraints on the table or MV; in Python, decorators work on `@dp.temporary_view`.
- Metrics appear in the pipeline UI Data quality tab and the event log. Warn and drop actions report metrics; `fail` records none. A view queried by several datasets can report several metric sets.
- **What `fail` stops.** In a triggered pipeline a failing flow rolls back only its own update; independent flows in the same update still complete. In a continuous pipeline the failing flow and its dependents stop. Expectations control which rows reach a dataset, not orchestration: to stop *other* work, put it in a separate pipeline or task gated with `depends_on` and `run_if: ALL_SUCCESS`.
- Every dataset that can receive bad data gets at least one expectation; "input assumed clean" in a comment is not a check.
- **Reusable rules:** keep rules in a Delta table (`name`, `constraint`, `tag`) or a Python module, load them with a helper such as `get_rules(tag)`, and apply with `@dp.expect_all_or_drop(get_rules("validity"))`. Expectations can also be stored and managed in Unity Catalog tables. SQL cannot load rules dynamically.

### Dataset-level checks

Row rules cannot see other rows. Put aggregate checks in a materialized view and fail it:

```python
@dp.materialized_view(name="orders_pk_check", private=True)
@dp.expect_or_fail("unique_order_id", "num_entries = 1")
def orders_pk_check():
    return spark.read.table("silver_orders").groupBy("order_id").count().withColumnRenamed("count", "num_entries")
```

The same shape covers row-count reconciliation (`a_count = b_count` over two counts) and missing-record detection (left join, `EXPECT (r_key IS NOT NULL)`).

### Quarantine pattern

Keep the clean path clean without silently losing rows:

```python
RULES = {"valid_amount": "amount > 0", "has_id": "order_id IS NOT NULL"}
# coalesce so a NULL result (e.g. NULL amount) lands in quarantine instead of vanishing
QUARANTINE = "NOT coalesce(" + " AND ".join(f"({r})" for r in RULES.values()) + ", false)"

@dp.table()
@dp.expect_all_or_drop(RULES)
def silver_orders():
    return spark.readStream.table("bronze_orders")

@dp.table()
def silver_orders_quarantine():
    return spark.readStream.table("bronze_orders").where(QUARANTINE)
```

Use the rescued-data quarantine in `schema-evolution.md` for parse and schema problems; use expectations for value rules. Alert when quarantine grows. Always derive the quarantine predicate from the same `RULES` dict so the two paths cannot drift. The Databricks docs also show a flag-column variant (`@dp.expect_all(rules)` plus an `is_quarantined` column and two views); if you use it, cluster rather than partition by the flag.

## Reading the results

Expectation metrics live in the pipeline event log. Query it with the `event_log` table-valued function and the flow-progress events:

```sql
CREATE OR REPLACE TEMP VIEW event_log_raw AS SELECT * FROM event_log('<pipeline_id>');

SELECT
  row_expectations.dataset AS dataset,
  row_expectations.name    AS expectation,
  SUM(row_expectations.passed_records) AS passing_records,
  SUM(row_expectations.failed_records) AS failing_records
FROM (
  SELECT EXPLODE(FROM_JSON(
           details:flow_progress:data_quality:expectations,
           "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>")) AS row_expectations
  FROM event_log_raw
  WHERE event_type = 'flow_progress'
)
GROUP BY row_expectations.dataset, row_expectations.name;
```

## Beyond row rules

| Category | How on Databricks | Severity default |
|---|---|---|
| Schema | Auto Loader modes and `_rescued_data`; explicit silver schema; Delta constraints | block |
| Completeness, validity, uniqueness | Expectations (uniqueness is better enforced by Auto CDC keys or a gold test than by an expectation) | block |
| Referential | Join check in a gold test or a materialized view; PK/FK constraints are informational only | block |
| Freshness | Unity Catalog data quality monitoring (anomaly detection, schema level) or a scheduled SQL alert on `max(_ingested_at)` | block/alert |
| Volume / completeness | Same monitoring (row counts vs predicted range) or a scheduled SQL alert against a trailing average | alert |
| Distribution / drift | Data profiling (formerly Lakehouse Monitoring): summary statistics, drift, custom metrics | alert |
| Reconciliation | A control-total query in a job task: `sum(gold)` equals `sum(source)`; fail the task on mismatch | block |

Freshness check as a failing SQL task (use `raise_error`, not `error`):

```sql
SELECT CASE
  WHEN max(_ingested_at) < current_timestamp() - INTERVAL 6 HOURS
  THEN raise_error('STALE: orders not refreshed within SLA')
END
FROM silver.orders;
```

Data quality monitoring (anomaly detection) is enabled per schema, runs on serverless compute, and is billed as its own product; confirm availability and cost before promising it.

## Constraints on tables

```sql
ALTER TABLE gold.orders ADD CONSTRAINT positive_amount CHECK (amount >= 0);
ALTER TABLE gold.orders ALTER COLUMN order_id SET NOT NULL;
```

`NOT NULL` and `CHECK` are enforced on write. Primary and foreign key constraints are informational (optimizer hints and documentation).

## Testing the checks

Test expectations before production. Pipeline Python unit tests (Beta) run datasets, expectations and Auto CDC against mock data. Also generate synthetic data with realistic skew and **inject bad rows** (nulls in keys, negative amounts, malformed JSON, a new column) using `databricks-synthetic-data-gen`, run the pipeline against a dev target, and confirm the right rows are dropped, quarantined or fail the update.

## Principles

- Make critical checks **blocking**; reserve alerts for anomalies you cannot auto-decide.
- Test keys and contracts first; they cause the worst downstream damage.
- Add an end-to-end reconciliation control total; it catches silent row loss.
- Version checks with the code; review any change to severity.
