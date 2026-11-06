"""Reading a stream from a folder of files, and inspecting what the engine does.

The folder data/stream_source holds 5 small JSON files. A streaming query treats
each file as newly arrived data. `trigger(availableNow=True)` processes everything
currently in the folder and then stops, which keeps these scripts finite.

Run from the repo root:  python 04-structured-streaming/01_file_source_stream.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, fresh_dir, get_spark, output_path  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

spark = get_spark("file-source-stream")

# Streaming file sources REQUIRE a schema (no inference by default).
schema = "event_time TIMESTAMP, page STRING, user_id STRING, latency_ms INT"

events = (spark.readStream
          .schema(schema)
          .option("maxFilesPerTrigger", 1)          # one file per micro-batch
          .json(data_path("stream_source")))

print("is streaming:", events.isStreaming)
events.printSchema()

# A streaming DataFrame supports the usual transformations...
slow = events.filter(F.col("latency_ms") > 500).select("event_time", "page", "latency_ms")

# ...but you cannot .show() or .count() it. You start a query that writes to a sink.
query = (slow.writeStream
         .format("memory")                 # sink: an in-memory table (good for learning/tests)
         .queryName("slow_events")
         .outputMode("append")
         .option("checkpointLocation", fresh_dir("04", "ckpt_01"))
         .trigger(availableNow=True)
         .start())
query.awaitTermination()

print("query is active:", query.isActive)
print("micro-batches run:", len(query.recentProgress))
for p in query.recentProgress:
    print(f"  batch {p['batchId']}: read {p['numInputRows']} rows")

# The memory sink is queryable with plain SQL.
spark.sql("SELECT page, COUNT(*) AS n, MAX(latency_ms) AS worst FROM slow_events GROUP BY page ORDER BY n DESC").show()

# --- Same source, written as files (parquet) with a checkpoint ---
out = output_path("04", "slow_parquet")
q2 = (slow.writeStream.format("parquet")
      .option("path", out)
      .option("checkpointLocation", fresh_dir("04", "ckpt_01b"))
      .outputMode("append")
      .trigger(availableNow=True)
      .start())
q2.awaitTermination()
print("rows written to parquet:", spark.read.parquet(out).count())

# --- Batch and stream agree: the same logic over the same files ---
batch_total = spark.read.schema(schema).json(data_path("stream_source")).filter("latency_ms > 500").count()
print("batch count of the same filter:", batch_total)

spark.stop()
