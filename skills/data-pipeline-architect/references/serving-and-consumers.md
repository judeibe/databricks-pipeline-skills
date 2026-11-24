# Serving and Consumers

A pipeline exists to feed consumers. Decide the serving shape while designing gold, because it constrains the dimensions and grain you must keep.

| Consumer | Use | Skill |
|---|---|---|
| Ad hoc SQL, BI, reports | Gold tables or materialized views queried through a SQL warehouse (serverless preferred) | `databricks-dbsql` |
| Governed KPIs shared by dashboards, Genie and SQL | **Metric views** over gold (`MEASURE()`), defined in YAML | `databricks-metric-views` |
| Dashboards | AI/BI dashboards on gold; every dashboard filter must be a gold column | `databricks-aibi-dashboards` |
| Natural-language questions | Genie agents over curated gold and metric views | `databricks-genie-agents`, `databricks-data-discovery` |
| Low-latency app lookups (OLTP) | Sync gold into **Lakebase** Postgres with synced tables (needs change data feed on the source) | `databricks-lakebase` |
| External engines (Snowflake, Spark, Trino, Flink) | Managed Iceberg, UniForm, or Compatibility Mode on streaming tables and MVs, read through the Iceberg REST Catalog | `databricks-iceberg` |
| Apps | Databricks Apps reading gold or Lakebase | `databricks-apps`, `databricks-apps-python` |
| ML features and training | Gold or feature tables feeding training; point-in-time-correct joins | `databricks-ml-training` |
| AI/RAG | Parsed and chunked tables feeding Vector Search or Agent Bricks | `databricks-vector-search`, `databricks-agent-bricks`, `databricks-ai-functions` |

Design consequences:

- **Metric views compute measures at query time**, so `COUNT(DISTINCT ...)`, ratios and window measures (trailing, cumulative, semi-additive `first`/`last`) stay correct at whatever grain the user groups by. Define them there rather than pre-aggregating in gold. The additive-only restriction applies to materialization rollups, not to the metric view itself. Keep the dimensions the metric view needs as gold columns.
- **Keep the grain fine enough** for every dashboard filter and Genie question; aggregate further at query time rather than recovering lost dimensions.
- **Lakebase synced tables** require change data feed on the source (`delta.enableChangeDataFeed = true`) and a regular UC catalog for the pipeline storage. They are read-only from the Postgres side except for reads, indexes and drop.
- **Iceberg readers** need compatible library versions, and UniForm requires deletion vectors to be disabled. Check `databricks-iceberg` before promising external access.
- Downstream ML, vector search and agent skills are consumers of your gold tables, not part of the pipeline design. Keep their requirements (point-in-time keys, text columns, access) in the design doc's consumer section.

## Metric views

Load `databricks-metric-views` for the full YAML reference. What a design must decide:

- **Spec:** `version: 1.1`. `source` is a gold table, view or SQL query; `joins` (star or snowflake, `many_to_one` by default) add dimensions; `fields` (formerly `dimensions`) and `measures` hold the definitions. Quote the `'on':` key in joins, because unquoted `on` is a YAML boolean.
- **Create** with `CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML AS $$ ... $$`. Query every measure through `MEASURE(<name>)`; `SELECT *` is not allowed, and a metric view cannot be joined directly (wrap the query in a CTE and join that).
- **Not pipeline code.** A metric view cannot be declared inside an SDP pipeline's source. Create it after the gold tables exist, as a SQL step in the job or a separate deployment step, and treat its YAML as code under review.
- **Materialization** (optional, DBR 17.3+ and serverless): a `materialization` block with `schedule`, `mode: relaxed` and `materialized_views` (`aggregated` for known query shapes, one `unaggregated` for varied ones). Databricks runs it as a managed Lakeflow pipeline. Always set a `schedule` aligned with the source pipeline's cadence; without one, refreshes are manual. Rollup rewrite only serves additive measures; `COUNT(DISTINCT)` and window measures need an exact match.
- **Materialization blockers:** `parameters` in the metric view, row filters, column masks or ABAC policies on the source, and invoker-dependent expressions (`current_user()`, `is_member()`).
- Set `rely.at_most_one_match: true` only on genuine many-to-one joins; it is not validated, and on a fan-out join it gives wrong `SUM` and `COUNT` results.
