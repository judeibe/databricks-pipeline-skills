# Governance, Observability and Cost

Load `databricks-unity-catalog` for the commands. This file says what a pipeline design must decide.

## Unity Catalog structure

- Hierarchy: metastore, catalog, schema, then tables, views, volumes, functions, models. Use three-part names (`catalog.schema.table`) everywhere.
- Decide where each layer lives (catalog per layer, or schema per layer) and put the names in bundle variables, not in code.
- Prefer **managed** tables and **managed volumes**. Use external locations and storage credentials only for data that must stay in storage you own. Landing files go in a Volume (`/Volumes/<catalog>/<schema>/<volume>/...`).
- Schemas and catalogs must exist before the pipeline runs. The identity that creates tables needs `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, and `MODIFY` on the target schema.

## Access control

- Run production pipelines and jobs as a **service principal** (`run_as`), not a person. The deploying identity may need the Service Principal User role on it.
- Grant to **groups**, at the narrowest securable that works, least privilege. Example for a consumer group:

```sql
GRANT USE CATALOG ON CATALOG analytics TO `data_readers`;
GRANT USE SCHEMA  ON SCHEMA  analytics.gold TO `data_readers`;
GRANT SELECT      ON SCHEMA  analytics.gold TO `data_readers`;
```

- Check with `SHOW GRANTS ON TABLE catalog.schema.table`.
- Job and pipeline permissions go on the resource in the bundle: `CAN_VIEW`, `CAN_MANAGE_RUN` (jobs) or `CAN_RUN` (pipelines), `CAN_MANAGE`. Volumes use `grants`, not `permissions`.
- A table-update job trigger needs `SELECT` for the job identity on the watched tables.
- Zerobus writers need `MODIFY` and `SELECT` granted directly on the target table.

## Protecting sensitive data

- Prefer **row filters** and **column masks** (UC functions) over copying a redacted table. Declare them on the dataset: `row_filter="ROW FILTER fn ON (col)"` and `col TYPE MASK fn USING COLUMNS (col2)` in the dataset `schema=`. These are Public Preview; confirm before depending on them.
- A row filter on a source table forces full refresh of downstream materialized views.
- Keep secrets out of code: Databricks secret scopes (`dbutils.secrets.get`), UC connections, and UC service credentials for cloud access.
- Tag sensitive columns and tables so audits can find them.

## Observability

| Question | Where to look |
|---|---|
| Did the update succeed, and why not? | Pipeline update state and `error.exceptions[0].message` (poll the update, not the pipeline); job run page |
| Did data-quality rules pass? | Pipeline event log (`event_log(...)`, `flow_progress` events); Data quality tab (see `data-quality.md`) |
| Who read or changed what? | `system.access.audit` |
| What feeds this table (table and column lineage)? | `system.access.table_lineage` and the column lineage tables |
| How long and how often do jobs run, and where do they fail? | Job-run system tables; job health rules |
| What does it cost? | `system.billing.usage`, grouped by SKU, workspace and tags |
| Is a table stale or short on rows? | Unity Catalog data quality monitoring; scheduled SQL alerts |

System tables can be large: always filter by date. Grant access explicitly (`USE CATALOG` on `system`, `USE SCHEMA`, `SELECT`). Run system-table SQL from a SQL warehouse or the SDK rather than the experimental CLI command.

```sql
SELECT source_table_full_name, source_column_name
FROM system.access.table_lineage
WHERE target_table_full_name = 'catalog.schema.table'
  AND event_date >= current_date() - 7;
```

Tag pipelines and jobs (`tags:` in the bundle: team, environment, product) so billing and ownership can be sliced later.

## Cost levers

- Serverless compute with `performance_target: STANDARD` for non-urgent batch.
- Triggered pipelines instead of continuous unless the SLA demands always-on.
- Materialized views refresh incrementally only on serverless with row tracking; otherwise they recompute fully.
- Liquid clustering (or `CLUSTER BY AUTO`) to prune scans; avoid over-partitioning.
- Predicate and column pruning in gold reads; pre-aggregate wide fact tables only when queries need it.
- Lakeflow Connect is billed in DBUs; database connectors add classic-compute gateway DBUs on top of the serverless ingestion pipeline.
- Clean up dev bundles (`bundle destroy -t dev`) after experiments, and give dev targets short retention.
- Review the budget with `system.billing.usage` before and after a change.
