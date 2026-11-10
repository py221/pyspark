"""Finding and handling missing values: drop, fill, impute, flag.

Run from the repo root:  python 06-mlops/01_missing_values.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, get_spark, output_path  # noqa: E402

from pyspark.ml.feature import Imputer  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

spark = get_spark("missing-values")
churn = spark.read.csv(data_path("churn.csv"), header=True, inferSchema=True)
n = churn.count()

# --- 1. Measure first: nulls per column ---
counts = churn.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in churn.columns])
counts.show()
print("rows:", n)

# --- 2. Option A: drop rows (cheap, but loses data and may bias the sample) ---
print("dropna() any null      :", churn.dropna().count(), "rows left")
print("dropna(thresh=7)       :", churn.dropna(thresh=7).count(), "rows left (keep rows with >=7 non-null)")
print("dropna(subset=[label]) :", churn.dropna(subset=["churned"]).count())

# --- 3. Option B: fill with a constant ---
filled = churn.fillna({"contract": "unknown", "tenure_months": 0})
print("after fillna, null contracts:", filled.filter("contract IS NULL").count())
print("   but 0 months tenure is now a made-up value for", churn.filter("tenure_months IS NULL").count(), "customers")

# --- 4. Option C: impute from the data (mean / median / mode) ---
cols = ["tenure_months", "monthly_charge"]
as_double = churn.select("*", *[F.col(c).cast("double").alias(c + "_d") for c in cols])
for strategy in ["mean", "median"]:
    imp = Imputer(strategy=strategy, inputCols=[c + "_d" for c in cols],
                  outputCols=[c + "_" + strategy for c in cols])
    model = imp.fit(as_double)
    print(f"{strategy:>6} imputer learned:", model.surrogateDF.first().asDict())

# --- 5. Keep the information that a value WAS missing ---
flagged = (as_double
           .withColumn("tenure_missing", F.col("tenure_months").isNull().cast("int"))
           .withColumn("charge_missing", F.col("monthly_charge").isNull().cast("int")))
# Is missingness itself related to the label? If so the flag is a useful feature.
flagged.groupBy("tenure_missing").agg(F.count("*").alias("rows"),
                                      F.round(F.avg("churned"), 3).alias("churn_rate")).show()

# --- 6. Mode for a categorical column ---
mode = (churn.filter("contract IS NOT NULL").groupBy("contract").count()
        .orderBy(F.desc("count")).first()["contract"])
print("most common contract:", mode)

# Rule of thumb: learn imputation values from TRAINING data only, and store them inside the
# pipeline (see 03_save_load_pipeline.py) so scoring uses the same values.
spark.stop()
