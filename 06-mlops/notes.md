# 06 - MLOps: from experiment to repeatable scoring

A model that only works in a notebook is not done. The goal of this stage is a single saved artifact
that takes raw rows and returns scores, plus a few checks around it.

Scripts: `01_missing_values.py`, `02_encoding_and_scaling.py`, `03_save_load_pipeline.py`,
`04_batch_scoring.py` (shared definition in `churn_pipeline.py`)

## Missing values

Measure first (`sum(isNull)` per column). In the churn sample 68 customers lack tenure, 31 lack a
monthly charge and 30 lack a contract. Then choose:

| approach | when | cost |
|---|---|---|
| `dropna()` | few rows lost, missing at random | 1,500 -> 1,376 rows here |
| `fillna(constant)` | a sensible default exists (`"unknown"`) | invents values; 0 months of tenure is a lie |
| `Imputer(strategy="mean"/"median")` | numeric columns | needs doubles; learns the value from training data |
| missing-indicator column | missingness may carry signal | extra feature; here tenure-missing customers churn less |

Median is safer than mean with outliers. For categoricals, an explicit `"unknown"` category or the mode.
The imputation value is *learned*, so it belongs inside the pipeline where it is saved and reused.

## Categorical encoding and scaling

`StringIndexer(handleInvalid=...)` decides what happens to a value not seen in training:

| setting | behavior |
|---|---|
| `error` (default) | job fails at scoring time |
| `skip` | the row disappears silently |
| `keep` | gets an extra index (`k`), never fails |

For production scoring use `keep`, and remember that the extra bucket becomes the all-zeros vector after
one-hot with `dropLast`. Indexers can also order alphabetically (`stringOrderType`) for a stable mapping.

Scalers: `StandardScaler` (mean 0, std 1; `withMean=True` makes sparse vectors dense), `MinMaxScaler`
(0..1), `MaxAbsScaler` (keeps sparsity), `Normalizer` (per row, not per column). Fit them on training data
only: scaled test values can fall outside 0..1. Linear models and anything distance-based need scaling;
trees do not. `SQLTransformer` lets arbitrary SQL column logic live in the pipeline as a stage.

## Pipelines as the deployable unit

`churn_pipeline.py` builds:

```
 SQLTransformer  cast numerics to double, fill missing categoricals with "unknown"
      -> Imputer (median)
      -> StringIndexer x3 (handleInvalid=keep) -> OneHotEncoder
      -> VectorAssembler -> StandardScaler -> LogisticRegression
```

The fitted `PipelineModel` scores raw data, nulls and all (AUC about 0.80).

## Saving and loading

```
 model.write().overwrite().save(path)     PipelineModel.load(path)
 path/
   metadata/part-00000                    JSON: class, Spark version, params
   stages/0_SQLTransformer_.../           one folder per stage, with its learned data
   stages/8_LogisticRegression_.../
```

- A loaded model reproduces predictions exactly; the script asserts it.
- Saving to an existing path fails unless you call `.overwrite()`.
- An unfitted `Pipeline` can be saved as a recipe.
- Saved models are tied to the Spark version family that wrote them. Record the version (the metadata
  does) and pin it in requirements.

## Batch scoring

`04_batch_scoring.py` follows the shape of a real nightly job:

1. load the model, 2. validate the input columns before scoring, 3. score, 4. write to a batch-keyed
folder with overwrite so a re-run is idempotent, 5. sanity-check the output.

Two awkward rows (an unseen contract type, an almost entirely empty record) go through without special code.
The monitoring check compares the mean predicted probability with the training churn rate (0.198 vs 0.202 here).
Hard 0/1 predictions come out lower because the model rarely crosses 0.5, so compare probabilities, not labels.
Real monitoring would also track input distributions and, once labels arrive, live metrics.
