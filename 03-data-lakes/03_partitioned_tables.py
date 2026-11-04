"""Partitioned data lake layout, partition pruning, and tables in the catalog.

Run from the repo root:  python 03-data-lakes/03_partitioned_tables.py
"""
import os  # noqa: E402
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, get_spark, output_path  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

spark = get_spark("partitioned-tables")

# Start from the cleaned logs if script 01 ran, otherwise re-derive quickly.
clean = output_path("03", "logs_clean")
if os.path.exists(clean):
    logs = spark.read.parquet(clean)
else:
    LOG_RE = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) [^"]*" (\d{3}) (\S+)$'
    raw = spark.read.text(data_path("access_log.txt"))
    logs = (raw.filter(F.col("value").rlike(LOG_RE)).select(
        F.regexp_extract("value", LOG_RE, 1).alias("host"),
        F.to_timestamp(F.regexp_extract("value", LOG_RE, 2), "dd/MMM/yyyy:HH:mm:ss Z").alias("ts"),
        F.regexp_extract("value", LOG_RE, 4).alias("path"),
        F.regexp_extract("value", LOG_RE, 5).cast("int").alias("status")))
    logs = logs.withColumn("date", F.to_date("ts"))

print("days of data:", logs.select("date").distinct().count(), "| rows:", logs.count())

# --- Write partitioned by a LOW-cardinality column ---
lake = output_path("03", "logs_by_date")
logs.write.mode("overwrite").partitionBy("date").parquet(lake)
parts = sorted(d for d in os.listdir(lake) if d.startswith("date="))
print(len(parts), "partition folders, e.g.:", parts[:3])

# --- Partition pruning: the filter on 'date' skips whole directories ---
one_day = spark.read.parquet(lake).filter(F.col("date") == "2026-03-03")
one_day.explain()          # look at PartitionFilters: [... (date = 2026-03-03)]
print("rows on 2026-03-03:", one_day.count())

# A filter on a NON-partition column cannot skip folders (only row groups, via pushdown)
spark.read.parquet(lake).filter("status = 500").explain()

# The partition column lives in the folder name, not in the files; Spark adds it back
print("columns when reading the root:", spark.read.parquet(lake).columns)
print("columns when reading one folder directly:",
      spark.read.parquet(os.path.join(lake, parts[0])).columns)

# --- Two-level partitioning and the small-files trap ---
two_level = output_path("03", "logs_by_date_status")
logs.write.mode("overwrite").partitionBy("date", "status").parquet(two_level)
n_files = sum(1 for _, _, fs in os.walk(two_level) for f in fs if f.endswith(".parquet"))
print("files with partitionBy(date, status):", n_files)

# repartition on the partition column first -> one writer per partition value
compact = output_path("03", "logs_compact")
logs.repartition("date").write.mode("overwrite").partitionBy("date").parquet(compact)
n_files2 = sum(1 for _, _, fs in os.walk(compact) for f in fs if f.endswith(".parquet"))
print("files after repartition('date') first:", n_files2)

# --- Tables in the catalog (metadata + data) ---
spark.sql("DROP TABLE IF EXISTS web_logs")
logs.write.mode("overwrite").partitionBy("date").format("parquet").saveAsTable("web_logs")
spark.sql("SHOW TABLES").show()
spark.sql("SHOW PARTITIONS web_logs").show(3)
spark.sql("DESCRIBE TABLE EXTENDED web_logs").filter(
    F.col("col_name").isin("Type", "Provider", "Location")).show(truncate=False)

spark.sql("""
  SELECT date, COUNT(*) AS hits, SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) AS errors
  FROM web_logs WHERE date >= '2026-03-04' GROUP BY date ORDER BY date
""").show()

spark.sql("DROP TABLE web_logs")
spark.stop()
