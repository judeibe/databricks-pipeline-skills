# Lineage, Reproducibility and Governance Review (Databricks)

## Purpose

Check that any table, report or model can be traced to the exact data it used, that the derivation is recorded, and that sensitive data is classified and access-controlled. On Databricks this rests on Delta table versions, Unity Catalog lineage and metadata, and fine-grained access, not on external versioning tools.

Load `databricks-unity-catalog` for commands (`references/1-access-control.md`, `4-fine-grained-access.md`, `5-system-tables.md`). Design guidance: `data-pipeline-architect` `references/governance-and-observability.md` if installed.

## When to use

- A model or report must be reproducible and its input data pinned.
- A new dataset, catalog, schema or grant is introduced.
- Nobody can say where a column came from, or who owns a table.
- A table may hold PII and its handling needs review.
- Data files or extracts are committed to git or copied around.

## Procedure

### 1. Find how data is referenced and versioned

```bash
# Mutable references versus pinned ones
grep -rnE "VERSION AS OF|TIMESTAMP AS OF|DESCRIBE HISTORY|RESTORE TABLE|table_changes|readChangeFeed|startingVersion" . --include='*.py' --include='*.sql' --include='*.ipynb'
# Hardcoded locations instead of UC three-part names and volumes
grep -rnE "dbfs:/|/mnt/|s3://|abfss://|gs://|/Workspace/Shared" . --include='*.py' --include='*.sql' --include='*.yml' --include='*.ipynb'
# Data files tracked by git
git ls-files | grep -iE '\.(csv|parquet|pkl|npz|avro|orc)$' | head -20
```

Code and models that must be reproducible should record a table's full name plus a Delta version or timestamp (for example as run params or tags), not just a mutable name that changes underneath them.

### 2. Check the reproducibility horizon

Time travel only reaches back as far as retained history. `VACUUM` and the log and deleted-file retention table properties set that horizon. If a model or audit needs to reproduce data from months ago, confirm retention covers it or that a snapshot or clone is kept. Flag a short retention on a table that models train on.

### 3. Check lineage

Lineage is captured by Unity Catalog for table and column reads and writes (`system.access.table_lineage` and the column lineage table). Confirm pipelines read and write UC tables with three-part names so lineage is recorded; reads through `/mnt`, DBFS paths or external files break the trail. Query lineage in a SQL warehouse with a date filter; do not run experimental CLI queries against large system tables.

### 4. Check documentation and ownership

A "dataset card" on Databricks is Unity Catalog metadata: table and column `COMMENT`s, tags, owner, and pipeline or job tags (team, environment, product). Confirm gold tables have a comment, a named owner and an owner-group grant. Source, refresh cadence and limitations belong in the comment or linked doc.

### 5. Check PII and access

```bash
grep -rniE "email|phone|ssn|first_name|last_name|address|dob|national_id|credit_card|ip_address" . --include='*.py' --include='*.sql' --include='*.yml' --include='*.ipynb'
grep -rnE "GRANT |REVOKE |ROW FILTER|MASK |SET TAGS|ALTER .* OWNER" . --include='*.py' --include='*.sql' --include='*.yml'
```

For each PII column confirm: tagged or classified, masked or filtered where not needed in the clear (column masks and row filters are UC functions; row filters and masks on pipeline datasets are Public Preview), grants go to groups at the narrowest securable, and there is a retention or deletion path. A row filter on a source table forces a full refresh of downstream materialized views, so plan for that.

### 6. Check identities and secrets

Production jobs and pipelines run as a service principal (`run_as`). A table-update trigger needs `SELECT` for the job identity on the watched tables. Zerobus writers need `MODIFY` and `SELECT` on the target table. Secrets come from secret scopes, UC connections or service credentials, never from code or YAML values.

## Concrete checks

- [ ] No data files in git; landing data lives in managed tables or volumes.
- [ ] Tables are referenced by UC three-part names; no `/mnt` or DBFS roots for governed data.
- [ ] Reproducible consumers (models, reports) record table name and Delta version or timestamp.
- [ ] Retention (`VACUUM`, log and deleted-file retention) covers the reproducibility need.
- [ ] Lineage is intact end to end; no step bypasses UC.
- [ ] Gold tables carry comments, tags and an owner.
- [ ] PII columns are tagged and masked or filtered; access is by group, least privilege.
- [ ] Retention or deletion exists for sensitive data.
- [ ] Production runs as a service principal with only the grants it needs.
- [ ] No secret values in code, YAML or notebook outputs.

## Anti-patterns

- Reading `latest/` paths or a mutable table name for a model that must be reproducible.
- Pipelines that write to external paths and register nothing in Unity Catalog.
- Grants to individual users, or `ALL PRIVILEGES` on a catalog.
- PII copied into ad-hoc extracts and notebook outputs.
- Copying a redacted table instead of using a mask or row filter.
- Assuming `RESTORE` or time travel works after `VACUUM` removed the files.

## Required output

1. **Versioning and reproducibility**: how each consumed table is pinned, and whether retention allows replay.
2. **Lineage map**: source to consumer, noting any break.
3. **Documentation and ownership**: comments, tags, owner.
4. **PII and access**: columns, protection, grants, retention.
5. **Findings table**: `severity | file:line | gap | risk | fix | skill`.
6. **Next safe action**.

## Safety

- Never print raw PII or secret values; name the column or variable.
- Do not run `GRANT`, `REVOKE`, `VACUUM`, `RESTORE`, or change tags, masks, filters or retention during review.
- Do not change access scope or ownership without approval.
