"""A full regression workflow: split, Pipeline, fit, evaluate against a baseline.

Run from the repo root:  python 05-machine-learning/02_regression_pipeline.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, get_spark, output_path  # noqa: E402

from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.evaluation import RegressionEvaluator  # noqa: E402
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler  # noqa: E402
from pyspark.ml.regression import LinearRegression, RandomForestRegressor  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

spark = get_spark("regression")
houses = spark.read.csv(data_path("houses.csv"), header=True, inferSchema=True).dropna()

# Split BEFORE fitting anything; the seed makes the split repeatable.
train, test = houses.randomSplit([0.8, 0.2], seed=42)
print("train rows:", train.count(), " test rows:", test.count())

# --- Baseline: always predict the training mean. Any model must beat this. ---
mean_price = train.agg(F.avg("price")).first()[0]
baseline = test.withColumn("prediction", F.lit(mean_price))
rmse = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="rmse")
r2 = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="r2")
print(f"baseline RMSE: {rmse.evaluate(baseline):,.0f}")

# --- Pipeline = indexer -> encoder -> assembler -> estimator ---
# Wrapping the steps means test data goes through the SAME learned transformations.
stages = [
    # Note: handleInvalid="keep" would add an always-zero "unseen" column after one-hot
    # encoding; an ordinary least-squares fit then sees a singular matrix and Spark logs
    # an optimizer error. Keep the default here, use "keep" when scoring unseen data (stage 06).
    StringIndexer(inputCol="neighborhood", outputCol="hood_idx"),
    OneHotEncoder(inputCols=["hood_idx"], outputCols=["hood_vec"]),
    VectorAssembler(inputCols=["sqft", "bedrooms", "bathrooms", "age_years", "has_garage", "hood_vec"],
                    outputCol="features"),
]
lr_pipeline = Pipeline(stages=stages + [LinearRegression(featuresCol="features", labelCol="price")])
lr_model = lr_pipeline.fit(train)          # -> PipelineModel
pred = lr_model.transform(test)
pred.select("house_id", "price", F.round("prediction").alias("prediction")).show(5)
print(f"linear regression  RMSE: {rmse.evaluate(pred):,.0f}   R2: {r2.evaluate(pred):.3f}")

# Inspect the fitted last stage
lin = lr_model.stages[-1]
print("intercept:", round(lin.intercept), "| train R2:", round(lin.summary.r2, 3))
for name, coef in zip(["sqft", "bedrooms", "bathrooms", "age_years", "has_garage"], lin.coefficients):
    print(f"  {name:<11}{coef:>10.1f}")

# --- Swap the estimator, keep everything else ---
rf_pipeline = Pipeline(stages=stages + [RandomForestRegressor(
    featuresCol="features", labelCol="price", numTrees=40, maxDepth=6, seed=1)])
rf_model = rf_pipeline.fit(train)
rf_pred = rf_model.transform(test)
print(f"random forest      RMSE: {rmse.evaluate(rf_pred):,.0f}   R2: {r2.evaluate(rf_pred):.3f}")
imp = rf_model.stages[-1].featureImportances
print("sqft importance:", round(imp[0], 3), "| bedrooms:", round(imp[1], 3))

# --- Residuals: look at where the model is wrong, not just the single number ---
(pred.withColumn("error", F.col("prediction") - F.col("price"))
 .select(F.round(F.avg("error")).alias("mean_error"), F.round(F.stddev("error")).alias("std_error"))
 .show())

spark.stop()
