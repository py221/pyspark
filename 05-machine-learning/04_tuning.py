"""Hyper-parameter search with CrossValidator and TrainValidationSplit.

Run from the repo root:  python 05-machine-learning/04_tuning.py
"""
import time  # noqa: E402
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, get_spark, output_path  # noqa: E402

from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.classification import LogisticRegression  # noqa: E402
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator  # noqa: E402
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler  # noqa: E402
from pyspark.ml.regression import RandomForestRegressor  # noqa: E402
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, TrainValidationSplit  # noqa: E402

spark = get_spark("tuning")

# ---------- Classification: CrossValidator ----------
churn = (spark.read.csv(data_path("churn.csv"), header=True, inferSchema=True).dropna()
         .withColumnRenamed("churned", "label"))
train, test = churn.randomSplit([0.8, 0.2], seed=5)

prep = [
    StringIndexer(inputCol="contract", outputCol="contract_idx", handleInvalid="keep"),
    OneHotEncoder(inputCols=["contract_idx"], outputCols=["contract_vec"]),
    VectorAssembler(inputCols=["tenure_months", "monthly_charge", "support_calls", "contract_vec"],
                    outputCol="features"),
]
lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=50)
pipeline = Pipeline(stages=prep + [lr])

# The grid: every combination is tried -> 3 x 2 = 6 candidates
grid = (ParamGridBuilder()
        .addGrid(lr.regParam, [0.0, 0.01, 0.1])
        .addGrid(lr.elasticNetParam, [0.0, 0.5])
        .build())

evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
cv = CrossValidator(estimator=pipeline, estimatorParamMaps=grid, evaluator=evaluator,
                    numFolds=3, seed=1, parallelism=2)
t0 = time.time()
cv_model = cv.fit(train)                    # 6 candidates x 3 folds + 1 final refit
print(f"CrossValidator fit in {time.time() - t0:.1f}s (fits: {len(grid)} x 3 folds + 1 refit)")

print("mean AUC per candidate:")
for params, metric in zip(grid, cv_model.avgMetrics):
    desc = {p.name: v for p, v in params.items()}
    print(f"  {metric:.4f}  {desc}")

best_lr = cv_model.bestModel.stages[-1]
print("best regParam:", best_lr.getRegParam(), "| elasticNetParam:", best_lr.getElasticNetParam())
print("held-out AUC with best model:", round(evaluator.evaluate(cv_model.transform(test)), 4))

# ---------- Regression: TrainValidationSplit (one split, cheaper) ----------
houses = spark.read.csv(data_path("houses.csv"), header=True, inferSchema=True).dropna()
h_train, h_test = houses.randomSplit([0.8, 0.2], seed=9)
rf = RandomForestRegressor(featuresCol="features", labelCol="price", seed=2)
h_pipe = Pipeline(stages=[
    StringIndexer(inputCol="neighborhood", outputCol="hood_idx"),
    OneHotEncoder(inputCols=["hood_idx"], outputCols=["hood_vec"]),
    VectorAssembler(inputCols=["sqft", "bedrooms", "bathrooms", "age_years", "has_garage", "hood_vec"],
                    outputCol="features"),
    rf])
h_grid = (ParamGridBuilder().addGrid(rf.maxDepth, [3, 6]).addGrid(rf.numTrees, [10, 40]).build())
rmse = RegressionEvaluator(labelCol="price", metricName="rmse")
tvs = TrainValidationSplit(estimator=h_pipe, estimatorParamMaps=h_grid, evaluator=rmse,
                           trainRatio=0.8, seed=1)
tvs_model = tvs.fit(h_train)
print("\nvalidation RMSE per candidate:")
for params, metric in zip(h_grid, tvs_model.validationMetrics):
    print(f"  {metric:,.0f}  {{{', '.join(f'{p.name}={v}' for p, v in params.items())}}}")
print(f"held-out RMSE with best model: {rmse.evaluate(tvs_model.transform(h_test)):,.0f}")

spark.stop()
