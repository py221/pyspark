"""Nested JSON: schema inference, dotted access, arrays, explode.

Run from the repo root:  python 03-data-lakes/02_nested_json.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, get_spark, output_path  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

spark = get_spark("nested-json")

# JSON Lines (one object per line) is the default shape Spark expects.
events = spark.read.json(data_path("events.jsonl"))
events.printSchema()            # structs, arrays of structs, arrays of strings

# --- Reach into structs with dots; missing structs yield nulls, not errors ---
events.select("event_id", "user.id", "user.country", "device.os", "device.model").show(5)
print("rows without a device struct:", events.filter(F.col("device").isNull()).count())

# --- Arrays: size, contains, element access ---
events.select(
    "event_id",
    F.size("items").alias("n_items"),
    F.array_contains("user.tags", "vip").alias("is_vip"),
    F.col("items")[0]["sku"].alias("first_sku"),
).show(5)

# --- explode: one output row per array element ---
# Rows with an empty array DISAPPEAR with explode; explode_outer keeps them with null.
lines = events.select("event_id", "type", F.explode("items").alias("item"))
print("events:", events.count(), "| exploded item rows:", lines.count(),
      "| with explode_outer:", events.select(F.explode_outer("items")).count())

flat = lines.select("event_id", "type", "item.sku", "item.qty", "item.price")
flat.show(5)

(flat.filter("type = 'purchase'")
 .groupBy("sku")
 .agg(F.sum("qty").alias("units"), F.round(F.sum(F.col("qty") * F.col("price")), 2).alias("revenue"))
 .orderBy(F.desc("revenue")).show(5))

# posexplode also gives the position inside the array
events.select("event_id", F.posexplode("user.tags").alias("pos", "tag")).show(5)

# --- Timestamps arrive as strings; cast them ---
by_day = (events.withColumn("ts", F.to_timestamp("ts"))
          .groupBy(F.to_date("ts").alias("day")).count().orderBy("day"))
by_day.show()

# --- Aggregating back into arrays ---
(events.groupBy("user.id")
 .agg(F.collect_set("type").alias("event_types"), F.count("*").alias("n"))
 .orderBy(F.desc("n")).show(3, truncate=False))

# --- Going the other way: struct -> JSON string -> struct ---
as_json = events.select("event_id", F.to_json(F.struct("user", "device")).alias("payload"))
as_json.show(2, truncate=False)
schema = events.select("user", "device").schema
restored = as_json.select("event_id", F.from_json("payload", schema).alias("p")).select("event_id", "p.user.id")
restored.show(2)

# --- An explicit schema skips inference and is the safer choice in production ---
ddl = ("event_id STRING, ts STRING, type STRING, "
       "user STRUCT<id: STRING, country: STRING, tags: ARRAY<STRING>>, "
       "items ARRAY<STRUCT<sku: STRING, qty: INT, price: DOUBLE>>")
typed = spark.read.schema(ddl).json(data_path("events.jsonl"))
typed.printSchema()                       # 'device' is not in the schema, so it is ignored

# --- Writing JSON back out; a pretty-printed multi-line file needs multiLine=true ---
typed.limit(50).write.mode("overwrite").json(output_path("03", "events_json"))
spark.createDataFrame([(1, "a")], "id INT, v STRING").coalesce(1).write.mode("overwrite").json(
    output_path("03", "tiny_json"))

spark.stop()
