# 01 - RDDs

An RDD (resilient distributed dataset) is Spark's original abstraction: an immutable collection
split into partitions, plus the *recipe* (lineage) for rebuilding any partition. Most day-to-day
work uses DataFrames now, but RDDs explain everything underneath them: partitioning, shuffles,
laziness, and why some operations are expensive.

Scripts: `01_context_and_laziness.py`, `02_pair_rdds.py`, `03_reduce_vs_group.py`

## The SparkContext

`spark.sparkContext` (usually `sc`) is the handle to the engine. In local mode the "cluster" is
threads in one JVM: `local[2]` means two worker threads. `sc.defaultParallelism` tells you how many
partitions you get when you do not ask for a number.

## Creating RDDs

| from | call | notes |
|---|---|---|
| a Python list | `sc.parallelize(lst, n)` | can hold mixed types; fine for demos |
| a number range | `sc.range(0, N)` | generated on the executors, no list in the driver |
| a text file | `sc.textFile(path, minPartitions)` | one element per line |
| a DataFrame | `df.rdd` | elements are `Row` objects |

`rdd.getNumPartitions()` and `rdd.glom().map(len).collect()` show how the data is chopped up.

## Transformations vs actions

```
 transformations (lazy)              actions (trigger work)
 ----------------------              ----------------------
 map, filter, flatMap                collect, take, first
 distinct, sample                    count, reduce, fold, aggregate
 reduceByKey, join, sortBy           takeOrdered, top, countByKey
        |                                   |
        v                                   v
   builds a plan  ------------------>  runs the plan
```

Transformations return a new RDD instantly and do no work. Only an action makes Spark walk the
lineage and run tasks. The script proves it with an accumulator: after `map` + `filter` it reads 0,
after `take(5)` only a handful of rows were touched (Spark stops early), after `count()` all of them.

Why it matters: Spark can pipeline `map -> filter -> map` into one pass, skip work that is never
needed, and rebuild a lost partition from lineage instead of replicating everything.

Gotchas
- `collect()` pulls everything into the driver. Use `take(n)` while exploring.
- An RDD used by two actions is computed twice unless you `cache()` / `persist()` it.
- Lambdas run in Python worker processes; anything they capture is pickled and shipped. Updating a
  captured Python variable inside `map` does nothing visible on the driver. Use an accumulator.
- Real text data is dirty. Split lines, then `filter(len(fields) >= expected)`. In the sample file
  547 of 600 lines survive that check.

## Pair RDDs

An RDD whose elements are `(key, value)` tuples unlocks the per-key operations.

```
 ("200",1) ("404",1) ("200",1)              keys()     -> "200" "404" "200"
        |                                    values()   -> 1 1 1
        v  reduceByKey(add)                  mapValues  -> touch values only
   ("200",2) ("404",1)                       sortByKey  -> order by key
```

Joins on pair RDDs: `join` (inner), `leftOuterJoin`, `rightOuterJoin`, `fullOuterJoin` (missing side
is `None`), `cogroup` (groups both sides per key without flattening), `subtractByKey` (anti-join).
`flatMapValues` expands each value into several pairs. `collectAsMap()` silently keeps only one
value per key, so duplicate keys vanish. `lookup(k)` returns all values for a key.

## reduceByKey vs groupByKey

Both group by key, but they move different amounts of data.

```
 partition 1: (a,1)(a,1)(b,1)        partition 2: (a,1)(b,1)(b,1)

 reduceByKey:  combine locally first      groupByKey: ship every pair
   p1 -> (a,2)(b,1)   p2 -> (a,1)(b,2)      p1 -> (a,1)(a,1)(b,1)
            \ shuffle: 4 records /                 \ shuffle: 6 records /
```

On the sample log, `groupByKey` shuffles 1,784 records and `reduceByKey` 68 for the same answer. With
real data and few distinct keys the gap is huge. A key with millions of values also has to fit in
memory on one executor under `groupByKey`.

Rule: prefer `reduceByKey`, `aggregateByKey` (when the result type differs, e.g. `(sum, count)` for an
average) or `combineByKey`. Reach for `groupByKey` only when you truly need all values together.
