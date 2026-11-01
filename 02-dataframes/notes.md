# 02 - DataFrames

A DataFrame is a distributed table: named, typed columns on top of an RDD of rows. Because Spark
knows the schema, the Catalyst optimizer can reorder, prune and push down work, which a plain RDD
of Python objects can never allow. The same plan runs whether you write the API or SQL.

Scripts: `01_create_dataframes.py`, `02_query_and_sql.py`, `03_window_functions.py`, `04_write_parquet.py`

## Creating DataFrames

- From Python objects: `spark.createDataFrame(rows, schema)`. `Row(name=..., n=...)` infers names.
- From files: `spark.read.csv/json/parquet(...)` or `spark.read.format("csv").option(...).load(path)`.
- From an RDD: `spark.createDataFrame(rdd, schema)`; back again with `df.rdd`.

CSV reading has three levels:

```
 no options      -> every column string, header row is data (_c0, _c1 ...)
 header+infer    -> convenient, but an extra pass over the data to guess types
 explicit schema -> fastest, self-documenting, no surprises   <- preferred
```

Options worth remembering: `header`, `inferSchema`, `sep` (tab-separated files need `"\t"`),
`dateFormat`, and `mode` (`PERMISSIVE` keeps bad rows with nulls, `DROPMALFORMED` drops them,
`FAILFAST` raises). A line with too few fields is padded with nulls rather than dropped.

Schemas can be a `StructType` or a DDL string like `"id INT, name STRING"`.

## Querying

- Column expressions: `withColumn`, `select`, `filter`, `F.when/otherwise`, `F.coalesce`.
- Aggregation: `groupBy(...).agg(F.count("*"), F.sum(...), F.avg(...))`.
- SQL: `df.createOrReplaceTempView("t")` then `spark.sql("SELECT ... FROM t")`. A temp view is only
  a name for a query plan; nothing is stored.
- `df.explain()` prints the physical plan. Look for `Exchange` (a shuffle) and `Filter`/`PushedFilters`.

## Joins

| type | keeps |
|---|---|
| `inner` (default) | matching rows only |
| `left` / `right` / `outer` | all rows from one/both sides, nulls for no match |
| `left_semi` | left rows that have a match (no right columns) |
| `left_anti` | left rows that have NO match: great for finding orphans |

In the sample data 62 orders reference customers that are not in the customer table; `left_anti`
finds them in one line. Wrap a small table in `F.broadcast(...)` to ship it to every executor and
avoid shuffling the big side.

Gotcha: joining on a column *name* (`on="customer_id"`) keeps one copy of the column; joining on an
expression (`a.id == b.id`) keeps both and later references become ambiguous.

## Window functions

A window computes a value per row using related rows, without collapsing them like `groupBy`.

```
 Window.partitionBy("category").orderBy(desc("revenue"))
   category  product   revenue   rank
   furniture desk       79,286     1
   furniture chair      40,660     2
```

- Ranking: `row_number` (always unique), `rank` (ties share a rank, then a gap), `dense_rank` (no gap).
- Offsets: `lag(col, n)` / `lead(col, n)` for previous/next row.
- Frames: `rowsBetween(-6, currentRow)` is a 7-row moving window; `unboundedPreceding` to `currentRow`
  gives a running total.
- No `orderBy` means the aggregate covers the whole partition (handy for percent-of-total).

Gotcha: a window with `orderBy` but no `partitionBy` pulls all rows into one partition. Fine here,
a bottleneck at scale.

## Writing parquet

`df.write.parquet(path)` creates a *directory* of part files (one per partition) plus `_SUCCESS`.

- Save modes: `error` (default), `overwrite`, `append`, `ignore`.
- Parquet is columnar and stores the schema, so reading needs no inference, and selecting two columns
  reads two columns (`ReadSchema` in the plan shows it).
- `coalesce(1)` gives one file; fine for tiny output, a bottleneck for big.
- Compression is snappy by default; `option("compression", "gzip")` trades CPU for size.
