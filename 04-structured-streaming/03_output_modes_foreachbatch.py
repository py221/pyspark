"""Output modes side by side, then foreachBatch for custom sinks.

Run from the repo root:  python 04-structured-streaming/03_output_modes_foreachbatch.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, fresh_dir, get_spark, output_path  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

spark = get_spark("foreachbatch")
schema = "event_time TIMESTAMP, page STRING, user_id STRING, latency_ms INT"


def source():
    return (spark.readStream.schema(schema).option("maxFilesPerTrigger", 1)
            .json(data_path("stream_source")))


# --- Output modes ---
#   append   : only NEW rows (works for queries with no aggregation, or closed windows)
#   update   : only rows that CHANGED in this batch
#   complete : the entire result table, every batch (aggregations only)
per_page = source().groupBy("page").agg(F.count("*").alias("hits"))
q = (per_page.writeStream.format("memory").queryName("per_page_complete")
     .outputMode("complete").option("checkpointLocation", fresh_dir("04", "ckpt_03a"))
     .trigger(availableNow=True).start())
q.awaitTermination()
spark.table("per_page_complete").orderBy("page").show()

# Trying append mode on an un-windowed aggregation is rejected up front:
try:
    (per_page.writeStream.format("memory").queryName("bad").outputMode("append")
     .option("checkpointLocation", fresh_dir("04", "ckpt_03b")).start())
except Exception as e:                                  # AnalysisException
    print("append on a plain aggregation fails:", type(e).__name__)

# --- foreachBatch: run ANY batch code on each micro-batch ---
# Each micro-batch arrives as an ordinary DataFrame, so every batch API works:
# JDBC writes, MERGE-style upserts, writing to two sinks, calling a model, ...
archive = output_path("04", "archive")
totals_path = output_path("04", "running_totals")
log = []


def handle(batch_df, batch_id):
    n = batch_df.count()
    log.append((batch_id, n))
    # (1) archive the raw rows, tagged with the batch id
    (batch_df.withColumn("batch_id", F.lit(batch_id))
     .write.mode("append").parquet(archive))
    # (2) maintain running totals per page by merging this batch into stored totals
    new = batch_df.groupBy("page").agg(F.count("*").alias("hits"))
    try:
        old = spark.read.parquet(totals_path)
        merged = (old.unionByName(new).groupBy("page").agg(F.sum("hits").alias("hits")))
    except Exception:                                    # first batch: nothing stored yet
        merged = new
    # write to a temp location then swap, because we are reading from totals_path
    tmp = totals_path + "_tmp"
    merged.write.mode("overwrite").parquet(tmp)
    spark.read.parquet(tmp).write.mode("overwrite").parquet(totals_path)


fresh_dir("04", "archive")
fresh_dir("04", "running_totals")
q = (source().writeStream.foreachBatch(handle)
     .option("checkpointLocation", fresh_dir("04", "ckpt_03c"))
     .trigger(availableNow=True).start())
q.awaitTermination()

print("batches handled:", log)
print("archive rows:", spark.read.parquet(archive).count())
spark.read.parquet(totals_path).orderBy("page").show()
print("batch ids in archive:", sorted(r[0] for r in spark.read.parquet(archive).select("batch_id").distinct().collect()))

# foreachBatch gives at-least-once delivery: after a crash a batch can be replayed with the
# same batch_id, so make writes idempotent (overwrite by batch_id, or MERGE on a key).

spark.stop()
