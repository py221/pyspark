# 03 - Data lakes

A data lake is just files in cheap storage, read by whatever engine you like. The work in Spark is
turning raw, messy files into clean typed tables and laying them out so queries read as little as
possible.

Scripts: `01_parse_web_logs.py`, `02_nested_json.py`, `03_partitioned_tables.py`

## Parsing text with regular expressions

Read a log as one string column (`spark.read.text`), then carve it up with `regexp_extract`:

```
 10.0.0.37 - - [01/Mar/2026:00:05:05 +0000] "GET /cart HTTP/1.1" 200 75307
 (  host  )     (        ts            )     (m) (path)         (st) (size)
   group 1            group 2               3    4              5     6
```

- `regexp_extract(col, pattern, n)` returns `""` when the pattern does not match, not null. Filter
  those out to find malformed lines (5 of 2,005 in the sample).
- A literal `-` for "no value" must become a real null before casting to a number.
- The same parse works with Python's `re` inside an RDD `map`, then `createDataFrame`. Built-in
  column functions are faster because they stay inside the JVM.

## Timestamps

`to_timestamp(col, "dd/MMM/yyyy:HH:mm:ss Z")` parses with an explicit pattern. Spark 3 is strict about
pattern letters; a pattern that does not fit gives errors or nulls rather than a guess. Setting
`spark.sql.legacy.timeParserPolicy=LEGACY` restores the lenient Spark 2 parser, which is a stopgap, not a fix.
Always check `isNull()` counts after parsing. The session time zone (set to UTC here) affects how
timestamps are displayed and converted. Useful follow-ups: `to_date`, `hour`, `date_format(ts, "E")`,
and `window(ts, "6 hours")` for time buckets.

## Nested JSON

Spark reads JSON Lines (one object per line) by default; a pretty-printed multi-line document needs
`option("multiLine", True)`. Inference scans the data, so production code should pass a schema.

```
 root
  |-- user: struct          user.id, user.country        (dot access)
  |-- device: struct        may be absent on some rows -> nulls
  |-- items: array<struct>  explode() -> one row per item
```

- Struct fields: `col("user.id")`. A missing struct gives null, not an error.
- Arrays: `size`, `array_contains`, `col("items")[0]`, `collect_set` / `collect_list` to build arrays.
- `explode(arr)` drops rows whose array is empty; `explode_outer` keeps them with null. In the sample,
  890 item rows versus 994 with `explode_outer`.
- `posexplode` also returns the index. `to_json` / `from_json` convert between structs and strings.
- Fields in the data that are not in your schema are silently ignored.

## Partitioned tables and pruning

`df.write.partitionBy("date").parquet(path)` writes one folder per value:

```
 logs_by_date/
   date=2026-03-01/part-....parquet
   date=2026-03-02/part-....parquet
```

- Filtering on the partition column (`date = '2026-03-03'`) skips whole folders: look for
  `PartitionFilters` in `explain()`. Filtering on any other column cannot skip folders.
- The partition value lives in the folder name, not the files. Read the root and the column comes back;
  read one folder directly and it is gone.
- Partition on low-cardinality columns that queries filter on (dates, region). Partitioning by a
  high-cardinality column such as user id creates a flood of tiny files.
- Small-files trap: each write task emits a file per partition value it holds. `partitionBy(date, status)`
  produced 25 files for 2,000 rows; `repartition("date")` first gave 5.
- `saveAsTable` registers the data in the catalog (managed table under the warehouse dir) so you can
  query it by name, `SHOW PARTITIONS`, and `DESCRIBE TABLE EXTENDED`.
