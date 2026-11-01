"""Window functions: rankings, running totals, previous-row comparisons.

Run from the repo root:  python 02-dataframes/03_window_functions.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, get_spark, output_path  # noqa: E402

from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

spark = get_spark("window-functions")
orders = (spark.read.csv(data_path("orders.csv"), header=True, inferSchema=True)
          .withColumn("revenue", F.col("quantity") * F.col("unit_price")))

# --- Ranking inside a group: top 2 products per category by total revenue ---
per_product = orders.groupBy("category", "product").agg(F.round(F.sum("revenue"), 2).alias("revenue"))
w_rank = Window.partitionBy("category").orderBy(F.desc("revenue"))
(per_product
 .withColumn("rank", F.rank().over(w_rank))
 .withColumn("row_number", F.row_number().over(w_rank))
 .withColumn("dense_rank", F.dense_rank().over(w_rank))
 .filter("rank <= 2")
 .orderBy("category", "rank").show())

# --- Daily revenue, running total, 7-day moving average, day-over-day change ---
daily = (orders.groupBy("order_date").agg(F.sum("revenue").alias("revenue"))
         .withColumn("revenue", F.round("revenue", 2)))
w_time = Window.orderBy("order_date")                                   # whole table, ordered
w_7d = Window.orderBy("order_date").rowsBetween(-6, Window.currentRow)  # this row and 6 before
w_run = Window.orderBy("order_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

(daily
 .withColumn("prev_day", F.lag("revenue", 1).over(w_time))
 .withColumn("change", F.round(F.col("revenue") - F.col("prev_day"), 2))
 .withColumn("running_total", F.round(F.sum("revenue").over(w_run), 2))
 .withColumn("avg_7d", F.round(F.avg("revenue").over(w_7d), 2))
 .orderBy("order_date").show(10))

# --- Share of total with a window that has no ordering ---
w_all = Window.partitionBy("region")
(orders.groupBy("region", "category").agg(F.sum("revenue").alias("rev"))
 .withColumn("pct_of_region", F.round(100 * F.col("rev") / F.sum("rev").over(w_all), 1))
 .orderBy("region", F.desc("pct_of_region")).show(8))

# --- Same thing in SQL ---
orders.createOrReplaceTempView("o")
spark.sql("""
  SELECT customer_id, order_date, revenue,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS nth_order
  FROM o ORDER BY customer_id, nth_order
""").show(5)

spark.stop()
