"""Querying with the DataFrame API and with SQL, plus joins.

Run from the repo root:  python 02-dataframes/02_query_and_sql.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, get_spark, output_path  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

spark = get_spark("query-and-sql")
orders = spark.read.csv(data_path("orders.csv"), header=True, inferSchema=True)
customers = spark.read.csv(data_path("customers.csv"), header=True, inferSchema=True)

# --- Column expressions ---
enriched = (orders
            .withColumn("discount", F.coalesce("discount", F.lit(0.0)))
            .withColumn("revenue", F.round(F.col("quantity") * F.col("unit_price")
                                           * (1 - F.col("discount")), 2))
            .withColumn("month", F.date_format("order_date", "yyyy-MM")))
enriched.select("order_id", "product", "revenue", "month").show(5)

# --- Filter, group, aggregate, sort ---
(enriched.filter((F.col("region") == "north") & (F.col("quantity") >= 2))
 .groupBy("category")
 .agg(F.count("*").alias("orders"), F.sum("revenue").alias("revenue"),
      F.avg("unit_price").alias("avg_price"))
 .orderBy(F.desc("revenue"))
 .show())

# --- The same question in SQL via a temp view ---
enriched.createOrReplaceTempView("orders_enriched")
spark.sql("""
    SELECT month, category, ROUND(SUM(revenue), 2) AS revenue
    FROM orders_enriched
    GROUP BY month, category
    HAVING SUM(revenue) > 5000
    ORDER BY month, revenue DESC
""").show(8)

# SQL and the API compile to the same plan; explain() lets you look at it.
enriched.filter("revenue > 500").groupBy("region").count().explain()

# --- Joins ---
print("inner join:", orders.join(customers, "customer_id").count())
print("left join :", orders.join(customers, "customer_id", "left").count())

orphans = orders.join(customers, "customer_id", "left_anti")      # orders with no customer row
print("left_anti (orders whose customer is unknown):", orphans.count(),
      sorted(r[0] for r in orphans.select("customer_id").distinct().collect()))

with_segment = orders.join(F.broadcast(customers), "customer_id")  # small table -> broadcast hint
(with_segment.groupBy("segment")
 .agg(F.count("*").alias("orders"), F.round(F.avg("quantity"), 2).alias("avg_qty"))
 .orderBy("segment").show())

# --- Dedupe and distinct counts ---
print("distinct customers who ordered:", orders.select("customer_id").distinct().count())
print("distinct (region, category) combos:", orders.dropDuplicates(["region", "category"]).count())

spark.stop()
