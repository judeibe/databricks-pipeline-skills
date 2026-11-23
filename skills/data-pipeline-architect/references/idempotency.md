# Idempotency & Safe Re-runs

**Definition:** running a step N times produces the same target state as running it once. It makes retries, repairs, backfills and recovery safe.

On Databricks most of this is built in if you pick the right dataset type. Hand-written idempotency is for the Jobs tasks and notebooks that sit outside a pipeline.

## Anti-pattern

```sql
-- NEVER on a retryable task: a re-run duplicates rows
INSERT INTO target.orders SELECT * FROM staging.orders;
```

Lakeflow Jobs does not make tasks idempotent. A task that wrote partial output before failing can duplicate data when it is retried or repaired, so the write itself must be safe.

## Pick the pattern by dataset or write type

| Situation | Idempotent because | Use |
|---|---|---|
| Streaming table over Auto Loader / Delta / Kafka | The pipeline checkpoint gives exactly-once processing | Nothing extra; never add your own checkpoint |
| Latest-row-per-key, deletes, history | Auto CDC applies each event by key and sequence | `create_auto_cdc_flow` (see `ingestion-patterns.md`) |
| Aggregates and marts | A materialized view is recomputed from its sources | `@dp.materialized_view` / `CREATE OR REFRESH MATERIALIZED VIEW` |
| Upsert in a Jobs task | Re-applying the same latest version changes nothing | `MERGE INTO` on a stable key |
| Reload one slice (a day, a region) | The predicate replaces exactly the slice | `INSERT INTO ... REPLACE WHERE` |
| Rebuild a whole table | Replacement is a single atomic commit | `CREATE OR REPLACE TABLE ... AS SELECT` |
| Push ingest (Zerobus), at-least-once sources | Duplicates are collapsed by key | Dedupe downstream with Auto CDC or `MERGE` |
| `foreachBatch` writing to several tables | Each write is tagged and skipped if already applied | `txnVersion` + `txnAppId` options |

## Pattern 1: MERGE on a stable key (Jobs tasks)

Requires a stable business key and a recency tiebreaker.

```sql
MERGE INTO silver.orders AS t
USING (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY order_id ORDER BY updated_at DESC, _ingested_at DESC) AS rn
    FROM bronze.orders
    WHERE _ingested_at >= :batch_start
  ) WHERE rn = 1
) AS s
ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

- Concurrent `MERGE`s on the same table can conflict. Retry on a concurrent-commit exception, and keep the target on liquid clustering or deletion vectors so row-level concurrency applies.
- Inside a pipeline, prefer Auto CDC over `MERGE`.

## Pattern 2: Replace a slice with REPLACE WHERE

```sql
INSERT INTO TABLE gold.daily_sales
REPLACE WHERE sales_date = :run_date
SELECT ... FROM silver.orders WHERE order_date = :run_date;
```

- The predicate is replaced atomically in one commit, so there is no delete-then-insert gap and no multi-statement transaction to manage.
- By default the write fails if any inserted row falls outside the predicate. Keep the predicate and the `SELECT` filter on the same grain.
- `INSERT ... REPLACE USING (cols)` (DBR 16.3 or later) replaces rows whose listed columns match the new data; prefer it to legacy dynamic partition overwrite.
- Multi-statement transactions (`BEGIN ATOMIC ... END`) exist in DBSQL but are Preview. Do not make them the default idempotency mechanism.

## Pattern 3: Atomic rebuild

```sql
CREATE OR REPLACE TABLE gold.dim_customer AS SELECT ...;
```

Consumers see either the old version or the new one. Delta time travel keeps the prior version.

## Pattern 4: Deterministic keys and dedup on read

Hash the business columns into a deterministic key so re-ingested rows collide instead of multiplying, then collapse with Auto CDC or `MERGE`:

```sql
SELECT sha2(concat_ws('|', order_id, line_no), 256) AS _pk, * FROM bronze.order_lines;
```

## Pattern 5: Exactly-once writes from foreachBatch

```python
def write_batch(df, batch_id):
    (df.write.format("delta")
       .option("txnVersion", batch_id)
       .option("txnAppId", "orders_silver_writer")   # stable and unique per writer
       .mode("append").saveAsTable("silver.orders"))
```

Use one `txnAppId` per logical writer, and a deterministic `txnVersion` such as the batch id. Replaying a batch is then a no-op.

## Recovery and rollback

- `DESCRIBE HISTORY table` shows each commit; `RESTORE TABLE table TO VERSION AS OF n` (or `TIMESTAMP AS OF`) rolls a bad write back.
- **Full refresh** of a pipeline dataset reprocesses streaming sources from scratch and resets streaming state. Rows the source no longer retains cannot be recovered. Prefer selective refresh, and leave any full refresh or `RESTORE` to the user.
- A streaming table cannot be converted to a materialized view in place; full refresh does not help.
- Repair a failed job run to re-run only failed and skipped tasks; make sure those tasks are idempotent first.

## Idempotency checklist

- [ ] Every table has a stable key, or a slice the task fully owns.
- [ ] No blind `INSERT` into a target on a retryable path.
- [ ] `REPLACE WHERE` predicate matches the inserted grain.
- [ ] Dedup keeps a deterministic "latest" (recency plus a tiebreaker).
- [ ] Re-running the range [A, B] twice yields identical output (test it).
- [ ] Manual watermarks are read from the target inside the retry-safe step.
- [ ] Multi-sink `foreachBatch` writes set `txnVersion` and `txnAppId`.
- [ ] Nothing in the pipeline sets its own checkpoint location.

## At-least-once versus exactly-once

Sources and push services (Zerobus, message buses) are usually at-least-once. You get effectively exactly-once results by making the write idempotent (Auto CDC, `MERGE`, `txnVersion`), not by trusting the transport. Design for duplicates and eliminate them with keys.
