# Notebooks and Code Hygiene Review (Databricks)

## Purpose

Check that notebook and source code is reproducible, testable, parameterised, free of secrets, and runs unchanged as a bundle-managed job on serverless compute. Databricks guidance is to keep business logic in importable modules and SQL files, and use notebooks for exploration, visualisation and thin orchestration.

Load `databricks-jobs` (notebook tasks, parameters), `databricks-serverless` / `databricks-serverless-migration` (compatibility), and `databricks-setup-local` or `databricks-execution-compute` (running and testing code).

## When to use

- A PR adds or changes a notebook, a source module, or a job's task code.
- A notebook works interactively but fails as a scheduled task.
- Notebook diffs are unreviewable, or logic is copied across notebooks.
- A secret or token may be in code or output.

## Procedure

### 1. Inventory notebooks in both formats

```bash
find . -name '*.ipynb' -not -path '*/.ipynb_checkpoints/*' -not -path '*/.databricks/*'
grep -rl "^# Databricks notebook source" . --include='*.py' --include='*.sql'
# Committed outputs and execution counts in .ipynb
grep -lE '"output_type"|"execution_count": [0-9]' $(find . -name '*.ipynb' -not -path '*/.ipynb_checkpoints/*') 2>/dev/null
```

Note which format the repo uses and whether outputs are stripped. Source-format notebooks (`# Databricks notebook source`, `# COMMAND ----------`, `# MAGIC` cells) diff cleanly. `.ipynb` is fine when outputs and execution counts are stripped before commit.

### 2. Check what the notebook carries

- Substantial logic (data loaders, transforms, model code) should live in `.py` modules or `.sql` files that jobs and pipelines reference and `pytest` can import, not copied between notebooks.
- Notebook tasks should read parameters through widgets or job `parameters` (`{{job.parameters.name}}`), and pass values between tasks with task values (`{{tasks.<task_key>.values.<name>}}`), not edited constants.
- A task's result should come back with `dbutils.notebook.exit(json.dumps({...}))` when a caller needs it.

### 3. Check compatibility with serverless and jobs

```bash
grep -rnE "sparkContext|\.rdd\b|\.cache\(\)|\.persist\(|dbfs:/|/mnt/|spark\.sparkContext|init_script|%scala|%r\b" . --include='*.py' --include='*.sql' --include='*.ipynb'
```

Flag RDD and `sparkContext` use, `.cache()` / `.persist()`, DBFS mounts instead of `/Volumes/...`, two-part table names instead of three-part Unity Catalog names, init scripts, and unpinned dependencies. R and Scala cells are valid classic-compute blockers; name them.

### 4. Scan for secrets and hardcoded environment

```bash
grep -rniE "api[_-]?key|secret|token|password|bearer |dapi[0-9a-f]{20,}|aws_access|sk-[a-z0-9]{10,}" . --include='*.py' --include='*.sql' --include='*.yml' --include='*.ipynb'
grep -rnE "FROM [a-z_]+\.[a-z_]+\b|USE CATALOG [a-z_]+|saveAsTable\(\"[a-z_]+\." . --include='*.py' --include='*.sql' --include='*.ipynb' | head
```

Credentials come from secret scopes (`dbutils.secrets.get`), UC connections or service credentials. A match in a cell source or an output is a leak: redact it and recommend rotation. Catalog and schema come from parameters or bundle variables, not literals, so dev, test and prod can differ.

### 5. Check tests and gates

- Unit tests exist for module logic and run in CI (`pytest`, optionally with Databricks Connect against serverless).
- `databricks bundle validate --strict` runs in CI; a non-production deploy catches resource-mapping errors.
- Integration checks run in staging with completion and data assertions (row counts, schema).
- Pipeline code is tested in a development target with small data that includes bad records.
- Code lives in a bundle-managed path, never `/Workspace/Shared`; builds (wheels, JARs) are uploaded to volumes in CI, not committed.

## Concrete checks

- [ ] Logic lives in modules or SQL files; notebooks are thin.
- [ ] Notebooks run top to bottom as a job task with parameters supplied, no manual cells.
- [ ] Parameters, task values and `dbutils.notebook.exit` are used instead of edited constants.
- [ ] `.ipynb` outputs and execution counts are stripped before commit, or the repo uses source-format notebooks.
- [ ] No secrets in source, outputs or YAML; scopes or UC connections are used.
- [ ] No hardcoded catalog, schema, workspace URL or user path.
- [ ] Serverless-compatible code, or a named classic blocker.
- [ ] Dependencies are pinned in the job `environments` spec or project file.
- [ ] Tests run in CI, with `bundle validate --strict` as a gate.
- [ ] Seeds are set where analysis results must repeat.

## Anti-patterns

- Hidden kernel state: a cell depends on a variable from a deleted cell.
- Copy-pasted loaders across notebooks.
- Constants edited by hand before each run instead of parameters.
- Full outputs and images committed, burying real changes.
- Tokens pasted into a cell for a quick test, then committed.
- Absolute personal workspace paths (`/Users/<name>/...`) in job definitions.
- Interactive-only helpers (`display`, `%run` chains) that break as job tasks.

## Required output

1. **Reproducibility verdict** per notebook: runs as a job task with parameters.
2. **Structure findings**: logic in notebooks, duplication, test coverage.
3. **Compatibility findings**: serverless and Unity Catalog issues.
4. **Secret scan**: locations only, values redacted, rotation flagged.
5. **Findings table**: `severity | notebook:cell or file:line | issue | risk | fix | skill`.
6. **Next safe action**.

## Safety

- Never print or commit a discovered secret; redact it and flag the credential for rotation.
- Do not execute notebooks during review; they may write to shared tables.
- Strip outputs before sharing excerpts; they may hold data.
- Do not rewrite history or force-push to remove a leaked secret without approval.
