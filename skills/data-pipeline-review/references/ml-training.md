# ML Training Review (Databricks)

## Purpose

Check that a training pipeline is reproducible, free of data leakage, tracked in MLflow, and registered in Unity Catalog in a way that serving can trust. The leakage and splitting rules are platform-neutral and stay. The tooling is Databricks: train on Databricks compute, track with MLflow, register to the Unity Catalog registry with aliases, and use Unity Catalog Feature Engineering where training and serving must share feature logic.

Load `databricks-ml-training` (and `databricks-mlflow-evaluation` for evaluation gates).

## When to use

- A PR adds or changes training code, feature engineering, or a tuning sweep.
- Offline metrics look too good, or production performs far worse than offline.
- Runs are not tracked, or a registered model cannot be tied to its data and code.
- Training runs locally or by hand instead of as a job.

## Procedure

### 1. Locate training and registry code

```bash
grep -rnE "\.fit\(|train_test_split|TimeSeriesSplit|GroupKFold|optuna|autolog|start_run|log_model|register_model|registered_model_name|set_registry_uri|set_registered_model_alias|transition_model_version_stage|FeatureLookup|create_training_set|fe\.log_model|score_batch" . --include='*.py' --include='*.ipynb'
```

### 2. Check where and how training runs

Training should run as a Databricks job or notebook task (serverless unless a blocker is named), not in a local process, because the data, tracking server and registry are in the workspace. Confirm the run is submitted through a bundle job or a one-time run, with dependencies pinned in the job `environments` spec, and that the notebook returns structured results with `dbutils.notebook.exit`.

### 3. Check the split and leakage boundary

- The split happens before any `fit`. Scalers, imputers, encoders and resamplers fit on the training fold only (a pipeline object makes this hold).
- Oversampling happens inside the training fold.
- The test set is used once, not for early stopping or selection.
- Time-ordered data uses a chronological split and features exclude anything unavailable at prediction time.
- Where features come from tables with timestamps, use point-in-time joins (`FeatureLookup` with a timeseries column) so labels never see future feature values.
- Seeds are set for Python, NumPy and the framework, and `random_state` is passed to splits.

### 4. Check tracking and registration

- `mlflow.set_registry_uri("databricks-uc")` is set before logging; otherwise the model lands in the deprecated workspace registry.
- The experiment path is under a folder that exists (`set_experiment` does not create parents).
- Autolog runs without `registered_model_name` during hyperparameter trials; the **best** run is registered once. A model version per trial is a finding.
- The run logs params, metrics, the code commit, and the input table name with its Delta version.
- The model signature and input example are logged.
- Versions are promoted with movable aliases (`@prod`, `@challenger`) through `set_registered_model_alias`. Stage transitions are deprecated in Unity Catalog.
- Evaluation gates promotion: the candidate is evaluated and compared to the current `@prod` before the alias moves.

### 5. Check features and skew

If the same features are computed in training and at scoring, prefer Unity Catalog Feature Engineering (`FeatureLookup`, `fe.log_model`, `fe.score_batch`) so lineage and logic are shared. Do not demand it where plain tables suffice. Declarative Feature Views are Public Preview; confirm before recommending. Models trained with `fe.log_model(training_set=...)` must be scored with `fe.score_batch`, not a plain `spark_udf`.

### 6. Check checkpoints and artifacts

Long runs checkpoint model, optimizer and epoch to durable storage (a Unity Catalog volume or MLflow artifacts), and a resume path exists. Artifacts are loaded from trusted sources; custom models use the Models from Code pattern rather than unpickling untrusted files.

## Concrete checks

- [ ] Training runs on Databricks compute through a job or one-time run; dependencies are pinned.
- [ ] Split precedes every `fit`; preprocessing is fit on training data only.
- [ ] No target leakage; time-ordered data splits chronologically; point-in-time joins for timestamped features.
- [ ] Seeds are set and splits are seeded.
- [ ] Registry URI is `databricks-uc`; the experiment folder exists.
- [ ] One registered version per intended candidate, not per trial.
- [ ] Runs log params, metrics, code commit, and input table version.
- [ ] Signature and input example are logged.
- [ ] Promotion uses aliases and an evaluation gate; no stage transitions.
- [ ] Feature-store-backed models are scored with `fe.score_batch`.
- [ ] Long runs are resumable.

## Anti-patterns

- `fit_transform` on the whole dataset before splitting; SMOTE before the split.
- Test set used for tuning or early stopping.
- Random shuffle on time-series data.
- Training in a local Python process against exported data.
- `transition_model_version_stage` or a hardcoded version number as the promotion mechanism.
- `registered_model_name` inside an Optuna objective, so each trial registers.
- Training data identified only by a mutable table name.
- Unpinned dependencies, so a rerun builds a different model.

## Required output

1. **Reproducibility verdict**: seeds, environment pinning, data version, code commit.
2. **Split and leakage audit**: `file:line` findings, preprocessing fit location, temporal handling.
3. **Registry and promotion**: UC registry use, aliases, evaluation gate.
4. **Features and skew**: shared logic and scoring path.
5. **Findings table**: `severity | file:line | issue | impact on metric honesty | fix | skill`.
6. **Next safe action**.

## Safety

- Do not retrain, register, or move an alias during review.
- Do not start long or GPU runs without approval.
- Never alter recorded metrics or delete runs, versions or checkpoints without sign-off.
- Redact sample training data; name columns instead.
