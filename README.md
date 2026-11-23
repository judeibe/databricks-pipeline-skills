# databricks-pipeline-skills

[![skills.sh](https://skills.sh/b/judeibe/databricks-pipeline-skills)](https://skills.sh/judeibe/databricks-pipeline-skills)

Agent skills for designing and reviewing data pipelines on Databricks.

| Skill | Use it to |
| --- | --- |
| [`data-pipeline-architect`](skills/data-pipeline-architect/SKILL.md) | Design pipelines end to end: Lakeflow Spark Declarative Pipelines, Auto Loader, Lakeflow Connect, Zerobus, Lakeflow Jobs, bundles and Unity Catalog. Covers idempotency, backfills, CDC, schema evolution and data quality gates. |
| [`data-pipeline-review`](skills/data-pipeline-review/SKILL.md) | Audit existing Databricks data and ML pipelines: duplicates, gaps, stale data, retry and backfill safety, permissions, lineage, MLflow training and model serving. |

The two are a pair: use the architect to design, the review skill to audit what exists.

## Install

Install with the [skills CLI](https://www.skills.sh/docs):

```bash
# both skills
npx skills add judeibe/databricks-pipeline-skills

# one skill
npx skills add judeibe/databricks-pipeline-skills --skill data-pipeline-architect

# list what the repo offers without installing
npx skills add judeibe/databricks-pipeline-skills --list
```

## Databricks product skills

These skills hold the architecture decisions and route to the official Databricks product skills (`databricks-pipelines`, `databricks-jobs`, `databricks-dabs` and others) for exact API and CLI syntax. Install those with the [Databricks CLI](https://docs.databricks.com/aws/en/agent-skills):

```bash
# all detected coding agents
databricks aitools install

# one agent
databricks aitools install --agents claude-code

# this project only, instead of globally
databricks aitools install --scope project

# keep them current
databricks aitools list
databricks aitools update
```

If you installed the deprecated AI Dev Kit skills, uninstall them first to avoid conflicts. Run `databricks aitools install --help` for the remaining flags, such as `--skills-only` and `--skills`.

## Layout

```
skills/
  data-pipeline-architect/
    SKILL.md
    references/  templates/  examples/
  data-pipeline-review/
    SKILL.md
    references/  templates/
```

## License

MIT
