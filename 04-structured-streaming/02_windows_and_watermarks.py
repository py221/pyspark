"""Event-time windows, watermarks, and what append vs update mode emit.

Run from the repo root:  python 04-structured-streaming/02_windows_and_watermarks.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, fresh_dir, get_spark, output_path  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

spark = get_spark("windows-watermarks")
schema = "event_time TIMESTAMP, page STRING, user_id STRING, latency_ms INT"


def source():
    return (spark.readStream.schema(schema).option("maxFilesPerTrigger", 1)
            .json(data_path("stream_source")))


total_events = spark.read.schema(schema).json(data_path("stream_source")).count()
print("events in the source folder:", total_events)


def run(name, df, mode, ckpt):
    q = (df.writeStream.format("memory").queryName(name).outputMode(mode)
         .option("checkpointLocation", fresh_dir("04", ckpt))
         .trigger(availableNow=True).start())
    q.awaitTermination()
    return spark.table(name)


# --- 1. Windowed count WITHOUT a watermark: state for every window is kept forever ---
no_wm = source().groupBy(F.window("event_time", "2 minutes"), "page").count()
res = run("no_wm", no_wm, "complete", "ckpt_02a")
print("\nno watermark, complete mode: windows x pages =", res.count(),
      "| events counted =", res.agg(F.sum("count")).first()[0])

# --- 2. With a watermark: Spark may drop state and late events ---
#   watermark = (max event_time seen so far) - delay
#   a window is finalised once the watermark passes its end
with_wm = (source().withWatermark("event_time", "5 minutes")
           .groupBy(F.window("event_time", "2 minutes"), "page").count())

upd = run("wm_update", with_wm, "update", "ckpt_02b")
print("\nwatermark + update mode rows:", upd.count(),
      "| events counted =", upd.agg(F.sum("count")).first()[0])
print("(update mode re-emits a window each time its count changes, so the sink holds partial"
      " counts too: summing them overcounts. Read the LATEST row per window instead.)")

app = run("wm_append", with_wm, "append", "ckpt_02c")
print("\nwatermark + append mode rows:", app.count(),
      "| events counted =", app.agg(F.sum("count")).first()[0])
print("append only emits windows the watermark has closed, so the newest windows"
      " are still waiting in state when the run ends")
app.orderBy("window").show(5, truncate=False)

# --- 3. Sliding windows: 4-minute windows that advance every 2 minutes ---
sliding = source().groupBy(F.window("event_time", "4 minutes", "2 minutes")).count()
sl = run("sliding", sliding, "complete", "ckpt_02d")
sl.orderBy("window").show(3, truncate=False)

# --- 4. Deduplicate by key within the watermark (state is bounded by the delay) ---
dedup = (source().withWatermark("event_time", "5 minutes")
         .dropDuplicates(["user_id", "event_time"]).select("user_id", "page"))
d = run("dedup", dedup, "append", "ckpt_02e")
print("rows after dropDuplicates:", d.count())

spark.stop()
