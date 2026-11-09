"""Turning columns into the single vector column that Spark ML estimators need.

Run from the repo root:  python 05-machine-learning/01_feature_engineering.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, get_spark, output_path  # noqa: E402

from pyspark.ml.feature import (Bucketizer, OneHotEncoder, StandardScaler,  # noqa: E402
                                StringIndexer, VectorAssembler)
from pyspark.ml.stat import Correlation  # noqa: E402

spark = get_spark("feature-engineering")
houses = (spark.read.csv(data_path("houses.csv"), header=True, inferSchema=True)
          .dropna())          # missing values are handled properly in stage 06
print("rows after dropna:", houses.count())
houses.show(3)

# --- 1. Categorical -> index -> one-hot ---
# StringIndexer maps strings to 0..k-1 ordered by frequency (most common = 0).
indexer = StringIndexer(inputCol="neighborhood", outputCol="hood_idx")
indexer_model = indexer.fit(houses)                     # learns the mapping from data
print("label order (index = position):", indexer_model.labels)
indexed = indexer_model.transform(houses)

# An index implies an order (2 > 1) that categories do not have; one-hot removes it.
# Spark drops the last category by default (dropLast) to avoid redundant columns.
encoder = OneHotEncoder(inputCols=["hood_idx"], outputCols=["hood_vec"])
encoded = encoder.fit(indexed).transform(indexed)
encoded.select("neighborhood", "hood_idx", "hood_vec").distinct().show(truncate=False)

# --- 2. Assemble everything numeric into ONE vector column ---
assembler = VectorAssembler(
    inputCols=["sqft", "bedrooms", "bathrooms", "age_years", "has_garage", "hood_vec"],
    outputCol="features")
assembled = assembler.transform(encoded)
assembled.select("features", "price").show(3, truncate=False)
# Vectors print as (size,[indices],[values]) when sparse and [v1,v2,...] when dense.

# --- 3. Scaling: put columns on comparable ranges ---
scaler = StandardScaler(inputCol="features", outputCol="scaled", withMean=True, withStd=True)
scaled = scaler.fit(assembled).transform(assembled)
print("first scaled vector:", scaled.first()["scaled"])

# --- 4. Bucketizing a continuous column ---
bucketizer = Bucketizer(splits=[0, 10, 30, 50, float("inf")], inputCol="age_years", outputCol="age_band")
bucketizer.transform(houses).groupBy("age_band").count().orderBy("age_band").show()

# --- 5. Correlation between features helps decide what to keep ---
corr = Correlation.corr(assembled.select("features"), "features").head()[0].toArray()
# feature order: sqft, bedrooms, bathrooms, age, garage, then 3 one-hot columns
print("corr(sqft, bedrooms)  = %.2f" % corr[0][1])
print("corr(sqft, bathrooms) = %.2f" % corr[0][2])
print("corr(sqft, age)       = %.2f" % corr[0][3])

# --- Features and the label: Spark ML wants (features: Vector, label: Double) ---
print("column types:", dict(assembled.select("features", "price").dtypes))

spark.stop()
