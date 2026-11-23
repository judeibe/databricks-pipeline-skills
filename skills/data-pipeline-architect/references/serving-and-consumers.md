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

- **Metric views only fit measures that re-aggregate** (additive sums and counts, per-cell distincts, ratios divided last). Non-additive or selection-dependent measures belong in a governed SQL function. Keep the dimensions the metric view needs as gold columns.
- **Keep the grain fine enough** for every dashboard filter and Genie question; aggregate further at query time rather than recovering lost dimensions.
- **Lakebase synced tables** require change data feed on the source (`delta.enableChangeDataFeed = true`) and a regular UC catalog for the pipeline storage. They are read-only from the Postgres side except for reads, indexes and drop.
- **Iceberg readers** need compatible library versions, and UniForm requires deletion vectors to be disabled. Check `databricks-iceberg` before promising external access.
- Downstream ML, vector search and agent skills are consumers of your gold tables, not part of the pipeline design. Keep their requirements (point-in-time keys, text columns, access) in the design doc's consumer section.
