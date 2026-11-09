# 05 - Machine learning with Spark ML

`pyspark.ml` is the DataFrame-based ML library. Every algorithm wants one `features` column holding a
vector and one numeric `label` column. Everything else is plumbing to get there.

Scripts: `01_feature_engineering.py`, `02_regression_pipeline.py`, `03_classification.py`, `04_tuning.py`

## Vocabulary

```
 Transformer : DataFrame -> DataFrame          (VectorAssembler, fitted models)
 Estimator   : DataFrame -> fit() -> Transformer   (StringIndexer, LinearRegression)
 Pipeline    : a list of stages; fit() returns a PipelineModel
```

An estimator *learns* something from data (category order, a mean, coefficients); the resulting
transformer applies it. That learned state is the reason to fit on training data only.

## Feature engineering

- `StringIndexer` maps strings to 0..k-1 by frequency (most common = 0). `labels` shows the mapping.
- `OneHotEncoder` turns an index into a sparse indicator vector. By default it drops the last
  category (`dropLast`) because it is implied by the others, so 4 neighborhoods become 3 columns.
- `VectorAssembler` concatenates numeric and vector columns into `features`.
- `StandardScaler`, `Bucketizer`, and `Correlation.corr` help with scale and redundancy. In the sample,
  sqft and bedrooms correlate at 0.82, which matters for interpreting linear coefficients.
- Sparse vectors print as `(size, [indices], [values])`; dense as `[v1, v2, ...]`.

## Train/test split and baselines

`df.randomSplit([0.8, 0.2], seed=42)`. Set the seed or the split changes every run. Split before
fitting anything. Always compute a dumb baseline first (predict the mean, or the majority class):
accuracy of 0.81 means nothing when 81% of customers do not churn.

## Regression

`LinearRegression`, `RandomForestRegressor`, ... with `RegressionEvaluator` metrics `rmse`, `mae`, `r2`.
On the synthetic houses data the baseline RMSE is about 114k; linear regression gets about 24k (R2 0.96).
The random forest does worse here because the data really is close to linear. The model that wins
is whichever matches the structure, and a simple model is easier to explain. Fitted linear models expose
`coefficients`, `intercept`, and `summary`. Forests expose `featureImportances`.

Gotcha: `StringIndexer(handleInvalid="keep")` adds an extra category even when training data has none.
After one-hot encoding that is a column of all zeros, which makes the least-squares system singular and
Spark logs an optimizer error. Use `keep` when scoring unseen values; it was not needed here.

## Classification

`LogisticRegression`, `RandomForestClassifier`, ... Output columns: `rawPrediction`, `probability`
(vector of per-class probabilities), `prediction`.

- `BinaryClassificationEvaluator` -> `areaUnderROC` / `areaUnderPR` (uses raw scores, threshold free).
- `MulticlassClassificationEvaluator` -> `accuracy`, `f1`, `weightedPrecision`, `weightedRecall`.
- A confusion matrix is one `groupBy("label").pivot("prediction").count()`.
- With about 20% positives, both models score 0.83 accuracy but recall on churners is only about 27%.
  Accuracy hides that. Move the decision threshold or weight classes if recall matters.

## Tuning

```
 ParamGridBuilder -> every combination
 CrossValidator       k folds x candidates fits + 1 refit on all training data   (robust, k times the cost)
 TrainValidationSplit candidates fits + 1 refit, one split                       (cheap, noisier)
```

Both take an estimator (usually a whole `Pipeline`), a list of param maps, and an evaluator.
`avgMetrics` / `validationMetrics` align with the grid; `bestModel` is the refit winner. Evaluate the
winner once on the held-out test set, never on the data used for selection. `parallelism=2` fits
candidates concurrently. Search cost multiplies fast, so keep grids small and start coarse.
In the sample the regularization grid barely matters (AUC 0.80-0.82 for most candidates), a useful
sign that the model is not the bottleneck.
