"""Writing DataFrames: parquet, save modes, partitioning, reading back.

Run from the repo root:  python 02-dataframes/04_write_parquet.py
"""
import os  # noqa: E402
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, get_spark, output_path  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

spark = get_spark("write-parquet")
orders = spark.read.csv(data_path("orders.csv"), header=True, inferSchema=True)


def tree(path, depth=2):
    """Print the directory layout Spark produced."""
    base = os.path.basename(path.rstrip("/"))
    print(base + "/")
    for root, dirs, files in os.walk(path):
        level = root[len(path):].count(os.sep)
        if level >= depth:
            dirs[:] = []
            continue
        dirs.sort()
        for f in [x for x in sorted(files) if not x.startswith(".")][:3]:
            print("  " * (level + 1) + f)
        for d in dirs[:4]:
            print("  " * (level + 1) + d + "/")


# --- A plain write produces a DIRECTORY of part files, not one file ---
out = output_path("02", "orders_parquet")
orders.write.mode("overwrite").parquet(out)
tree(out)

# --- Parquet stores the schema, so no inferSchema on the way back in ---
back = spark.read.parquet(out)
back.printSchema()
assert back.count() == orders.count()

# --- Save modes ---
#   error / errorifexists (default) : fail if path exists
#   overwrite                       : replace everything
#   append                          : add new files next to old ones
#   ignore                          : do nothing if path exists
orders.limit(10).write.mode("append").parquet(out)
print("rows after append:", spark.read.parquet(out).count())

# --- One file or many? coalesce(1) is fine for tiny output, a bottleneck for big ---
single = output_path("02", "orders_single")
orders.coalesce(1).write.mode("overwrite").option("compression", "gzip").parquet(single)
print("part files:", [f for f in os.listdir(single) if f.endswith(".parquet")])

# --- Other formats from the same DataFrame ---
orders.limit(100).write.mode("overwrite").json(output_path("02", "orders_json"))
orders.limit(100).write.mode("overwrite").option("header", True).csv(output_path("02", "orders_csv"))

# --- Column pruning: parquet is columnar, so selecting few columns reads few bytes ---
back.select("order_id", "quantity").explain()
print("(look for ReadSchema with only two columns)")

# --- Quick aggregate written back out ---
(back.groupBy("region").agg(F.count("*").alias("n"))
 .write.mode("overwrite").parquet(output_path("02", "region_counts")))
spark.read.parquet(output_path("02", "region_counts")).orderBy("region").show()

spark.stop()
