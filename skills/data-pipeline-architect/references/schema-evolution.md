# Schema Evolution

Schemas change. Absorb safe changes automatically, surface breaking ones loudly, and never corrupt downstream consumers.

## Change taxonomy

| Change | Safe? | Default handling |
|---|---|---|
| Add nullable column | Safe (additive) | Auto-add in bronze |
| Widen a type (int to bigint, etc.) | Usually safe | Delta type widening; validate downstream |
| Drop column | Breaking | Alert; never silent |
| Rename column | Breaking | Treat as drop plus add; map explicitly (Delta column mapping) |
| Narrow or incompatible type change | Breaking | Fail or rescue; needs a migration |
| Same name, new meaning | Breaking and sneaky | Only a contract test or expectation catches it |

## Policy per layer

- **Bronze:** additive auto-evolution. A new optional field must not block ingestion; keep everything, missing fields are null for old rows. Capture anything that does not fit in `_rescued_data`.
- **Silver:** explicit, typed columns. Map raw to typed deliberately and enforce with expectations (see `data-quality.md`). A breaking upstream change should fail here, not in a dashboard.
- **Gold:** strict contract. Changes go through review and versioning. Never auto-evolve consumer-facing tables.

## Auto Loader (files)

`cloudFiles.schemaEvolutionMode` decides what happens when a new column appears:

| Mode | New column | Stream |
|---|---|---|
| `addNewColumns` (default when no schema is supplied) | Added to the end of the schema | Fails once with `UnknownFieldException`, then resumes on restart |
| `rescue` | Never evolves; new data goes to `_rescued_data` | Keeps running |
| `failOnNewColumns` | Schema not updated | Fails until you update the schema or remove the file |
| `none` (default when you supply a schema) | Ignored | Keeps running; data rescued only if `rescuedDataColumn` is set |

Notes:

- With `addNewColumns`, run the pipeline from a Lakeflow Job with retries so the restart after the schema update is automatic.
- `addNewColumns` is not allowed with an explicit schema; pass the schema as `cloudFiles.schemaHints` instead.
- `cloudFiles.inferColumnTypes` turns on type inference (JSON/CSV/XML default to all strings). `cloudFiles.schemaHints` types a few columns without declaring the rest.
- If you provide an explicit `schema=`, include the rescued-data column (`_rescued_data STRING`, renamable with `rescuedDataColumn`).
- Inside a pipeline never set `cloudFiles.schemaLocation`.

### Rescued-data quarantine

Route rows with parse or schema problems to a side table so the clean path stays clean and nothing is silently dropped:

```sql
CREATE OR REFRESH STREAMING TABLE bronze_events AS
SELECT *, current_timestamp() AS _ingested_at
FROM STREAM read_files('/Volumes/<catalog>/<schema>/raw/events/', format => 'json');

CREATE OR REFRESH STREAMING TABLE bronze_quarantine AS
SELECT * FROM STREAM(bronze_events) WHERE _rescued_data IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE silver_clean AS
SELECT * FROM STREAM(bronze_events) WHERE _rescued_data IS NULL;
```

Use this for schema and parse problems. Use an expectation for value rules such as `amount > 0`. Alert on `bronze_quarantine` growth.

## Delta tables

| Need | Mechanism |
|---|---|
| Add columns on write | `mergeSchema` write option (or `ALTER TABLE ... ADD COLUMN`) |
| Rename or drop columns without rewriting data | Column mapping: `ALTER TABLE t SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')` |
| Widen types | Delta type widening (`delta.enableTypeWidening`) |
| See what changed and when | `DESCRIBE HISTORY t` |
| Roll back a bad schema change | `RESTORE TABLE t TO VERSION AS OF n` |
| Enforce rules | `NOT NULL`, `CHECK` constraints; primary and foreign keys are informational only (not enforced) |

Check the current docs for each property's runtime requirement before relying on it, and note that column mapping is required before UniForm/Iceberg compatibility on existing tables.

## Push ingest (Zerobus)

The **table is the contract**. Update the table schema before you update producers. Fields that do not fit can go to a rescue column (Beta, JSON only). Match field names and types to the table; TIMESTAMP columns take epoch microseconds.

## Managed connectors

Lakeflow Connect applies source schema changes automatically for sources with change tracking or CDC. Data type changes typically require a full snapshot reload; plan for that outage window.

## Detecting drift and handling breaks

1. **Additive:** evolve, log the change, continue (bronze only).
2. **Breaking:** fail or quarantine, alert, and do not promote to silver or gold. Replay from bronze after the contract is fixed.
3. **History:** `DESCRIBE HISTORY` and the UC `information_schema.columns` view answer "what did the schema look like"; do not hand-build a schema history table unless you need an audit trail beyond Delta retention.

## Data contracts

A contract is a versioned, testable agreement between producer and consumer: names, types, nullability, semantics and freshness. Enforce it with:

- explicit schemas and expectations at the silver boundary,
- `CHECK`/`NOT NULL` constraints on gold tables,
- CI that runs `databricks bundle validate --strict` and pipeline dry runs against a dev target.

Breaking the contract should fail the producer's pipeline, not a consumer's dashboard.
