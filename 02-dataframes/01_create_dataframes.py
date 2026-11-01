"""Creating DataFrames: from Python objects, from files with options, and from RDDs.

Run from the repo root:  python 02-dataframes/01_create_dataframes.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, get_spark, output_path  # noqa: E402

from pyspark.sql import Row  # noqa: E402
from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType,  # noqa: E402
                               StructField, StructType)

spark = get_spark("create-dataframes")

# --- 1. From Python objects ---
df = spark.createDataFrame([("keyboard", 45.0), ("monitor", 210.0)], ["product", "price"])
df.printSchema()
df.show()

rows = spark.createDataFrame([Row(name="a", n=1), Row(name="b", n=2)])
print("Row objects infer names from keywords:", rows.columns)

# --- 2. From a CSV file: three levels of control ---
orders_csv = data_path("orders.csv")

# (a) defaults: everything is a string and the header is data
raw = spark.read.csv(orders_csv)
print("\nno options   :", raw.columns[:3], "...", raw.first()[:3])

# (b) header + inferSchema: convenient, but costs an extra pass over the data
inferred = spark.read.csv(orders_csv, header=True, inferSchema=True)
print("inferSchema  :", dict(inferred.dtypes))

# (c) explicit schema: fastest, and the schema is documented in code
orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType()),
    StructField("order_date", DateType()),
    StructField("product", StringType()),
    StructField("category", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("region", StringType()),
    StructField("discount", DoubleType()),
])
orders = (spark.read.format("csv")
          .option("header", True)
          .option("mode", "PERMISSIVE")
          .schema(orders_schema)
          .load(orders_csv))
orders.printSchema()
orders.show(3)

# --- 3. A tab-separated file with a custom delimiter and compact date format ---
readings_schema = "sensor_id STRING, reported STRING, site STRING, status STRING, reading DOUBLE, note STRING"
readings = (spark.read.option("sep", "\t").option("header", False)
            .schema(readings_schema).csv(data_path("sensor_readings.tsv")))
print("short lines are padded with nulls, not dropped:")
readings.filter("site IS NULL").show(3)

# --- 4. RDD <-> DataFrame ---
rdd = readings.rdd                       # an RDD of Row objects
print("df.rdd element type:", type(rdd.first()).__name__)
back = spark.createDataFrame(rdd.map(lambda r: (r.sensor_id, r.reading)), ["sensor_id", "reading"])
back.show(3)

# --- 5. Quick profiling ---
orders.describe("quantity", "unit_price").show()
print("rows:", orders.count(), " null discounts:", orders.filter("discount IS NULL").count())

spark.stop()
