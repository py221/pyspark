# 04 - Structured Streaming

Structured Streaming treats a stream as a table that keeps growing. You write the same DataFrame
code as in batch; the engine runs it incrementally on each new slice of data (a micro-batch).

Scripts: `01_file_source_stream.py`, `02_windows_and_watermarks.py`, `03_output_modes_foreachbatch.py`

```
 files arrive ---> [ source ] ---> transformations ---> [ sink ]
                       |                                   ^
                       +---- checkpoint (offsets, state) --+
```

## Reading and starting a stream

- `spark.readStream.schema(...).json(folder)` watches a folder. File sources need an explicit schema.
- `maxFilesPerTrigger=1` makes each micro-batch consume one file, which makes batches easy to see.
- A streaming DataFrame has `isStreaming == True`. You cannot `show()` or `count()` it; you call
  `writeStream...start()`, which returns a `StreamingQuery`.
- `trigger(availableNow=True)` processes everything currently available, then stops. It makes scripts
  and tests finish. `processingTime="10 seconds"` is the always-on version.
- `query.awaitTermination()` blocks; `query.recentProgress` has rows read per batch; `query.stop()` ends it.
- The `memory` sink (with `queryName`) exposes results as a table for SQL while learning. Real sinks
  are parquet files, Kafka, or `foreachBatch`.
- **Checkpoint** (`checkpointLocation`) records which input was processed plus any aggregation state.
  It is what makes restarts resume instead of reprocess. Delete it only when you want a fresh start.

## Event time, windows and watermarks

Aggregate by when the event *happened*, not when it arrived: `groupBy(window("event_time", "2 minutes"))`.
Sliding windows add a slide: `window(col, "4 minutes", "2 minutes")`.

Late data is the catch. Spark keeps state for every open window, and without a bound that grows forever.

```
 watermark = max(event_time seen) - allowed delay      (here: 5 minutes)
 window is final once watermark > window end  ->  its state is dropped,
                                                  later events for it are ignored
```

- `withWatermark("event_time", "5 minutes")` must be set on the same column used in the window and
  before the aggregation.
- In the sample run, complete mode with no watermark counted all 300 events. Append mode with a
  watermark emitted only 267: the rest sit in windows the watermark has not closed yet, or were late.
- `dropDuplicates(["user_id", "event_time"])` with a watermark deduplicates with bounded state.

## Output modes

| mode | emits | works with |
|---|---|---|
| `append` | only new final rows | non-aggregating queries; aggregations only with a watermark (after windows close) |
| `update` | rows changed in this batch | aggregations |
| `complete` | the whole result table every batch | aggregations (state must fit) |

An un-windowed aggregation in `append` mode is rejected up front with an `AnalysisException`.
Update mode on the memory sink keeps partial counts from earlier batches, so summing the sink
overcounts: read the latest row per window.

## foreachBatch

`writeStream.foreachBatch(fn)` calls `fn(batch_df, batch_id)` for each micro-batch with an ordinary
DataFrame, so any batch API works: write to two places, upsert into a table, call a JDBC sink.
The script archives raw rows and maintains running per-page totals.

Delivery is at-least-once. After a crash a batch can be replayed with the same `batch_id`, so make writes
idempotent (overwrite by batch id or merge on a key).

Gotchas
- Re-running a script with an old checkpoint and different query logic fails or behaves oddly. The
  scripts delete their checkpoints first.
- Not every batch operation is allowed on a stream (no `distinct`-style global ops without state, limited joins).
