"""Parsing semi-structured text: regex extraction, casting, timestamps.

Run from the repo root:  python 03-data-lakes/01_parse_web_logs.py
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, get_spark, output_path  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

spark = get_spark("parse-web-logs")
raw = spark.read.text(data_path("access_log.txt"))      # one column: value
print("raw lines:", raw.count())
raw.show(2, truncate=False)

# --- Regex with capture groups; regexp_extract returns '' when nothing matches ---
LOG_RE = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) [^"]*" (\d{3}) (\S+)$'
parsed = raw.select(
    F.regexp_extract("value", LOG_RE, 1).alias("host"),
    F.regexp_extract("value", LOG_RE, 2).alias("ts_raw"),
    F.regexp_extract("value", LOG_RE, 3).alias("method"),
    F.regexp_extract("value", LOG_RE, 4).alias("path"),
    F.regexp_extract("value", LOG_RE, 5).cast("int").alias("status"),
    F.regexp_extract("value", LOG_RE, 6).alias("size_raw"),
)

# Lines that failed to match show up as empty strings: find and drop them.
bad = parsed.filter(F.col("host") == "")
print("unparseable lines:", bad.count())
good = parsed.filter(F.col("host") != "")

# '-' means "no body": turn it into a real null before casting
good = good.withColumn("size", F.when(F.col("size_raw") == "-", None)
                       .otherwise(F.col("size_raw")).cast("long")).drop("size_raw")

# --- Timestamps: parse with an explicit pattern ---
# The log writes '01/Mar/2026:00:05:05 +0000'. Spark 3 is strict about patterns;
# if a pattern fails you get nulls (or an error) rather than a guess.
logs = good.withColumn("ts", F.to_timestamp("ts_raw", "dd/MMM/yyyy:HH:mm:ss Z")).drop("ts_raw")
logs.printSchema()
logs.show(3, truncate=False)
print("null timestamps:", logs.filter("ts IS NULL").count())

# --- Derived time columns ---
logs = (logs.withColumn("date", F.to_date("ts"))
        .withColumn("hour", F.hour("ts"))
        .withColumn("dow", F.date_format("ts", "E")))

print("requests per hour of day:")
logs.groupBy("hour").count().orderBy("hour").show(6)

print("error rate by path (status >= 400):")
(logs.groupBy("path")
 .agg(F.count("*").alias("hits"),
      F.round(F.avg((F.col("status") >= 400).cast("int")), 3).alias("error_rate"))
 .orderBy(F.desc("error_rate")).show(5))

# fixed-width time buckets using the window() function
(logs.groupBy(F.window("ts", "6 hours")).agg(F.count("*").alias("hits"))
 .orderBy("window").show(4, truncate=False))

# --- Same parse through the RDD API, then back to a DataFrame ---
pat = re.compile(r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) [^"]*" (\d{3}) (\S+)$')


def to_row(line):
    m = pat.match(line)
    if not m:
        return None
    host, ts, method, path, status, size = m.groups()
    return (host, method, path, int(status), None if size == "-" else int(size))


rdd_rows = spark.sparkContext.textFile(data_path("access_log.txt")).map(to_row).filter(lambda r: r)
via_rdd = spark.createDataFrame(rdd_rows, "host string, method string, path string, status int, size long")
print("rdd route rows:", via_rdd.count(), "| dataframe route rows:", logs.count())

# Persist the cleaned data for the next script
logs.write.mode("overwrite").parquet(output_path("03", "logs_clean"))

spark.stop()
