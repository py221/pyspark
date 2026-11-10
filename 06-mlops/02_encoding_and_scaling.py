"""Categorical encoding behaviour (including unseen values) and the scalers.

Run from the repo root:  python 06-mlops/02_encoding_and_scaling.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, get_spark, output_path  # noqa: E402

from pyspark.ml.feature import (MaxAbsScaler, MinMaxScaler, Normalizer, OneHotEncoder,  # noqa: E402
                                SQLTransformer, StandardScaler, StringIndexer, VectorAssembler)
from pyspark.sql import functions as F  # noqa: E402

spark = get_spark("encoding-scaling")

train = spark.createDataFrame([("red",), ("blue",), ("red",), ("green",), ("red",), ("blue",)], ["colour"])
new = spark.createDataFrame([("red",), ("purple",)], ["colour"])      # 'purple' never seen in training

# --- 1. StringIndexer: frequency order and handling of unseen labels ---
idx = StringIndexer(inputCol="colour", outputCol="idx").fit(train)
print("labels (index order):", idx.labels)
# (with "error" Spark logs the failing task before the exception reaches Python)
for mode in ["error", "skip", "keep"]:
    m = StringIndexer(inputCol="colour", outputCol="idx", handleInvalid=mode).fit(train)
    try:
        out = m.transform(new).collect()
        print(f"handleInvalid={mode:<5}:", [(r.colour, r.idx) for r in out])
    except Exception as e:
        print(f"handleInvalid={mode:<5}: raised {type(e).__name__}")

# Other orderings are available (useful when you want a deterministic mapping)
alpha = StringIndexer(inputCol="colour", outputCol="idx", stringOrderType="alphabetAsc").fit(train)
print("alphabetical labels:", alpha.labels)

# --- 2. One-hot encoding sparse vectors ---
indexed = StringIndexer(inputCol="colour", outputCol="idx", handleInvalid="keep").fit(train).transform(train)
oh = OneHotEncoder(inputCols=["idx"], outputCols=["vec"], dropLast=False).fit(indexed)
oh.transform(indexed).distinct().show(truncate=False)

# --- 3. Scalers: compare what each one does to the same column ---
churn = spark.read.csv(data_path("churn.csv"), header=True, inferSchema=True).dropna()
vec = VectorAssembler(inputCols=["tenure_months", "monthly_charge"], outputCol="x").transform(churn)

scalers = {
    "standard (mean 0, std 1)": StandardScaler(inputCol="x", outputCol="s", withMean=True, withStd=True),
    "min-max  (0..1)": MinMaxScaler(inputCol="x", outputCol="s"),
    "max-abs  (-1..1)": MaxAbsScaler(inputCol="x", outputCol="s"),
}
for name, sc in scalers.items():
    out = sc.fit(vec).transform(vec)
    row = out.select("x", "s").first()
    print(f"{name:<26} {row.x.toArray().round(1)} -> {row.s.toArray().round(3)}")

# Normalizer works per ROW (unit length), not per column
nrm = Normalizer(inputCol="x", outputCol="s", p=2.0).transform(vec).select("x", "s").first()
print(f"{'row normalizer (L2)':<26} {nrm.x.toArray().round(1)} -> {nrm.s.toArray().round(3)}")

# --- 4. Fit scalers on TRAIN only, then apply to test ---
tr, te = vec.randomSplit([0.8, 0.2], seed=3)
fitted = MinMaxScaler(inputCol="x", outputCol="s").fit(tr)
print("\nmin/max learned from train:", fitted.originalMin.toArray(), fitted.originalMax.toArray())
out = fitted.transform(te).select("s").rdd.map(lambda r: r.s.toArray().max()).max()
print("largest scaled value in test:", round(out, 3), "(can exceed 1.0 if test has outliers)")

# --- 5. Arbitrary column transformations as a pipeline stage ---
sql = SQLTransformer(statement="""
  SELECT *, monthly_charge * tenure_months AS lifetime_value,
            CASE WHEN tenure_months < 12 THEN 'new' ELSE 'established' END AS tenure_band
  FROM __THIS__""")
sql.transform(churn).select("customer_id", "lifetime_value", "tenure_band").show(3)

spark.stop()
