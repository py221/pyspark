"""Batch scoring with a saved model: validate input, score, write results, sanity-check.

Run from the repo root:  python 06-mlops/04_batch_scoring.py
"""
import os  # noqa: E402
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, get_spark, output_path  # noqa: E402

from pyspark.ml import PipelineModel  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from churn_pipeline import RAW_COLUMNS, build_pipeline  # noqa: E402

spark = get_spark("batch-scoring")
model_path = output_path("06", "churn_model")

# Train and save if the previous script has not been run yet.
if not os.path.exists(model_path):
    data = (spark.read.csv(data_path("churn.csv"), header=True, inferSchema=True)
            .withColumnRenamed("churned", "label"))
    build_pipeline().fit(data).write().overwrite().save(model_path)

model = PipelineModel.load(model_path)

# --- A "new batch": real rows plus the awkward cases production sends ---
batch = (spark.read.csv(data_path("churn.csv"), header=True, inferSchema=True)
         .drop("churned").limit(200))
awkward = spark.createDataFrame(
    [("N-001", 3, 79.0, "three_year", "card", 8, "north"),      # contract value never seen
     ("N-002", None, None, None, "wallet", 0, "south")],        # everything optional is missing
    "customer_id string, tenure_months int, monthly_charge double, contract string, "
    "payment_method string, support_calls int, region string")
batch = batch.unionByName(awkward)


# --- 1. Validate the input contract before scoring ---
def check_schema(df, required):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"input is missing columns: {missing}")


check_schema(batch, RAW_COLUMNS + ["customer_id"])
print("input rows:", batch.count(), "- schema ok")

# --- 2. Score: the pipeline imputes, encodes and scales on its own ---
scored = (model.transform(batch)
          .withColumn("churn_probability", F.udf(lambda v: float(v[1]), "double")("probability"))
          .select("customer_id", "contract", "churn_probability", F.col("prediction").cast("int").alias("predicted_churn"))
          .withColumn("scored_at", F.current_timestamp()))
scored.filter(F.col("customer_id").startswith("N-")).show(truncate=False)

# --- 3. Write the output (idempotent: overwrite the batch's folder) ---
out = output_path("06", "scores", "batch=2026-03-01")
scored.write.mode("overwrite").parquet(out)
print("scored rows written:", spark.read.parquet(out).count())

# --- 4. Cheap monitoring: has the score distribution shifted? ---
stats = scored.agg(F.avg("predicted_churn").alias("predicted_rate"),
                   F.avg("churn_probability").alias("mean_prob")).first()
training_rate = (spark.read.csv(data_path("churn.csv"), header=True, inferSchema=True)
                 .agg(F.avg("churned")).first()[0])
print(f"mean churn probability {stats.mean_prob:.3f} vs training label rate {training_rate:.3f}"
      f" (hard-label rate {stats.predicted_rate:.3f} runs lower: the model rarely crosses 0.5)")
if abs(stats.mean_prob - training_rate) > 0.10:
    print("ALERT: score distribution far from training distribution - investigate inputs")

(scored.withColumn("bucket", F.floor(F.col("churn_probability") * 5) / 5)
 .groupBy("bucket").count().orderBy("bucket").show())

spark.stop()
