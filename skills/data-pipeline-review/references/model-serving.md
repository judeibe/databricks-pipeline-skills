# Model Scoring and Serving Review (Databricks)

## Purpose

Check that predictions come from a known, pinned model version through the right consumption path, can be rolled back, and are monitored. On Databricks a registered Unity Catalog model is consumed either as **batch scoring** over Delta or through a **Model Serving endpoint**; the review starts by checking the choice fits the latency requirement.

Load `databricks-model-serving` for endpoint operations and `databricks-ml-training` for batch scoring and registration. For Unity Gateway (model services, MCP services) use `databricks-unity-gateway`.

## When to use

- A PR adds or changes batch scoring, an endpoint, or model-loading code.
- A model behaves differently in production from offline evaluation.
- There is no clear way to tell or change which version is live.
- Drift, latency or error monitoring is missing.

## Procedure

### 1. Identify the consumption path

```bash
grep -rnE "spark_udf|score_batch|pyfunc\.load_model|load_model|get_deploy_client|serving-endpoints|serving_endpoint|update_endpoint|create_endpoint|update-config|models:/" . --include='*.py' --include='*.sql' --include='*.yml' --include='*.ipynb'
```

| Need | Expected path |
|---|---|
| Dashboards, daily or hourly scores, results read by apps or Genie | Batch: `mlflow.pyfunc.spark_udf` (or `fe.score_batch` for feature-store models) into a gold predictions table, written with `MERGE` for history or a deliberate overwrite |
| Score on a user action, sub-second | Model Serving endpoint |

A real-time endpoint for a use case that only reads a dashboard is a cost finding. A batch path for a per-request need is a latency finding.

### 2. Check version pinning

Models load as `models:/<catalog>.<schema>.<model>@<alias>` (for example `@prod`), never `@latest`, stage names, or an unrecorded version. For endpoints, the served entity names a version and the response or logs let you tell which one answered.

### 3. Check rollout and swap

- A version swap changes **both** the alias and the endpoint config (`update_endpoint` or `serving-endpoints update-config`). Alias-only does not change what the endpoint serves; config-only leaves the alias stale.
- Readiness is judged on `state.ready == READY` **and** `state.config_update == NOT_UPDATING`. A check on `ready` alone passes while the old version still serves.
- Risky changes use `traffic_config` routes (canary or A/B) before moving to 100%. The route's `served_model_name` is `<model>-<version>`; a mismatched string silently does not match.
- Rollback is documented and is a repeat of the swap with the previous version.

### 4. Check input handling and parity

- A model signature and input example are logged, so bad shapes are rejected rather than scored.
- Preprocessing is part of the logged model (pipeline or custom pyfunc), not re-implemented in serving code. Feature-store models use the registered feature lineage.
- Endpoint dependencies that need other Databricks resources (UC functions, vector indexes, other endpoints) are passed as `resources=[...]` at log time; otherwise the first query returns PERMISSION_DENIED.
- `env_manager` matches the runtime: `local` only when scoring and training share a runtime; otherwise `virtualenv` or `uv`.
- Custom models use Models from Code; no untrusted pickle is deserialized.

### 5. Check monitoring and access

- Request and response logging is enabled on the endpoint (inference tables through AI Gateway). Confirm the current feature name in the workspace.
- A data profiling monitor of the inference-log type (or equivalent) watches input drift, prediction distribution and model quality, with alerts.
- Latency, throughput and error rates are watched (`serving-endpoints export-metrics`, logs).
- Rate limits use the correct gateway: legacy AI Gateway settings on the endpoint belong to Model Serving; a Unity Gateway request belongs to `databricks-unity-gateway`.
- Endpoint permissions are explicit (`CAN_QUERY` for consumers); apps declare the endpoint as a resource.

## Concrete checks

- [ ] Batch versus real-time matches the freshness and latency need.
- [ ] Model loads by alias; no `@latest`, no stage, no raw version in scoring code.
- [ ] Swap changes alias and endpoint config together; readiness checks both state fields.
- [ ] Canary or traffic split exists for risky changes; rollback is one documented step.
- [ ] Signature and input example logged; preprocessing is inside the model.
- [ ] Passthrough `resources` declared where the model calls other Databricks assets.
- [ ] Batch scores land in a gold table with an idempotent write.
- [ ] Request and response logging and a drift monitor are on, with alerts.
- [ ] Latency and error monitoring exist; permissions are explicit.

## Anti-patterns

- Serving code that re-implements training transforms.
- Moving only the alias and assuming the endpoint updated.
- Declaring the endpoint ready after `state.ready` alone.
- Scoring row by row in a loop instead of `spark_udf` over a DataFrame.
- Scoring history overwritten when consumers need it; use `MERGE` for incremental scoring with history (overwrite is fine for "latest score per entity" because the commit is atomic).
- No prediction logging, so drift appears only when business metrics drop.
- Looking for a deployed endpoint in the Serving UI under "Owned by me" when jobs deploy as a service principal.

## Required output

1. **Consumption path**: batch or endpoint and its fit to the requirement.
2. **Versioning and rollback**: pin, swap procedure, readiness check.
3. **Skew and input audit**: signature, preprocessing parity, passthrough resources.
4. **Monitoring**: drift, quality, latency, errors, alerts.
5. **Findings table**: `severity | file:line | issue | risk | fix | skill`.
6. **Next safe action**.

## Safety

- Do not create, update or delete endpoints, change traffic, or move aliases during review.
- Never deserialize an untrusted model file.
- Treat inference inputs and outputs as sensitive; redact values and name fields.
- Do not disable monitoring or alerts to quiet noise without sign-off.
