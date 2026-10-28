"""RDD basics: SparkContext, creating RDDs, lazy evaluation, transformations vs actions.

Run from the repo root:  python 01-rdds/01_context_and_laziness.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, get_spark  # noqa: E402

spark = get_spark("rdd-basics")
sc = spark.sparkContext

# --- 1. The SparkContext is the handle to the cluster (here: this machine) ---
print("app name           :", sc.appName)
print("master             :", sc.master)
print("default parallelism:", sc.defaultParallelism)
print("python version     :", sc.pythonVer)

# --- 2. Three ways to create an RDD ---
from_list = sc.parallelize(["a", "b", "c", 1, 2, 3, {"k": 4}])   # RDDs can hold mixed types
print("\nmixed types:", from_list.map(lambda x: type(x).__name__).collect())

numbers = sc.range(0, 1_000_000)             # like range(), but distributed
print("range partitions:", numbers.getNumPartitions())

lines = sc.textFile(data_path("sensor_readings.tsv"), minPartitions=4)
print("text file lines:", lines.count(), "in", lines.getNumPartitions(), "partitions")

# glom() shows how elements are grouped into partitions
print("partition sizes:", sc.parallelize(range(10), 3).glom().map(len).collect())

# --- 3. Transformations are lazy: nothing runs until an action ---
touched = sc.accumulator(0)


def tag(x):
    touched.add(1)
    return x * 2


doubled = numbers.map(tag)          # returns instantly, no work done
evens = doubled.filter(lambda x: x % 4 == 0)
print("\nafter map+filter, rows touched so far:", touched.value)   # 0

print("first 5 evens:", evens.take(5))                             # action!
print("rows touched after take(5):", touched.value)                # small, not 1,000,000

evens.count()                                                      # action: full pass
print("rows touched after count():", touched.value)

# The recipe Spark remembered (lineage), printed as text:
print("\nlineage:\n" + evens.toDebugString().decode())

# --- 4. A tour of common transformations and actions ---
words = sc.parallelize(["spark makes big data small", "big data needs big memory"])
print("\nmap     :", words.map(lambda s: s.split()).collect())
print("flatMap :", words.flatMap(lambda s: s.split()).collect())
print("distinct:", sorted(words.flatMap(lambda s: s.split()).distinct().collect()))

nums = sc.parallelize([5, 3, 8, 1, 9, 2], 2)
print("reduce       :", nums.reduce(lambda a, b: a + b))
print("fold         :", nums.fold(0, lambda a, b: a + b))
print("takeOrdered  :", nums.takeOrdered(3))
print("top          :", nums.top(3))
print("aggregate    :", nums.aggregate((0, 0), lambda acc, x: (acc[0] + x, acc[1] + 1),
                                         lambda a, b: (a[0] + b[0], a[1] + b[1])))
print("sample ~50%  :", nums.sample(False, 0.5, seed=1).collect())

# --- 5. Cleaning a messy text file the RDD way ---
parsed = lines.map(lambda l: l.split("\t"))
good = parsed.filter(lambda f: len(f) >= 6)
print(f"\nvalid records: {good.count()} of {parsed.count()}")
print("a good record:", good.first())

# Reusing an RDD in several actions? cache it so lineage isn't recomputed.
good.cache()
good.count()
print("storage level:", good.getStorageLevel())

spark.stop()
