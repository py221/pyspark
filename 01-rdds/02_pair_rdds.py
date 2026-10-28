"""Pair RDDs: (key, value) tuples, per-key operations and the join family.

Run from the repo root:  python 01-rdds/02_pair_rdds.py
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, get_spark  # noqa: E402

spark = get_spark("pair-rdds")
sc = spark.sparkContext

# Build a pair RDD from the web log: (status, 1)
LOG = re.compile(r'^(\S+) \S+ \S+ \[[^\]]+\] "(\S+) (\S+) [^"]*" (\d{3}) (\S+)$')


def parse(line):
    m = LOG.match(line)
    return m.groups() if m else None


records = sc.textFile(data_path("access_log.txt")).map(parse).filter(lambda r: r is not None)
by_status = records.map(lambda r: (r[3], 1))

print("keys   :", sorted(by_status.keys().distinct().collect()))
print("counts :", sorted(by_status.reduceByKey(lambda a, b: a + b).collect()))
print("countByKey (action, returns dict):", dict(by_status.countByKey()))

# --- Per-key transformations ---
pairs = sc.parallelize([(1, 2), (3, 4), (3, 6)])
print("\nmapValues      :", pairs.mapValues(lambda v: v * 10).collect())
print("flatMapValues  :", pairs.flatMapValues(lambda v: range(v, v + 3)).collect())
print("sortByKey desc :", pairs.sortByKey(ascending=False).collect())
print("groupByKey     :", [(k, list(v)) for k, v in pairs.groupByKey().collect()])
print("collectAsMap   :", pairs.collectAsMap(), "   <- duplicate keys collapse!")
print("lookup(3)      :", pairs.lookup(3))

# --- Operations on two pair RDDs ---
other = sc.parallelize([(3, 9), (5, 0)])
print("\njoin           :", sorted(pairs.join(other).collect()))
print("leftOuterJoin  :", sorted(pairs.leftOuterJoin(other).collect()))
print("rightOuterJoin :", sorted(pairs.rightOuterJoin(other).collect()))
print("fullOuterJoin  :", sorted(pairs.fullOuterJoin(other).collect(), key=str))
print("subtractByKey  :", pairs.subtractByKey(other).collect())
print("cogroup        :", sorted((k, (list(a), list(b)))
                                  for k, (a, b) in pairs.cogroup(other).collect()))

# --- A realistic one: bytes served per path, biggest first ---
bytes_by_path = (
    records.filter(lambda r: r[4].isdigit())
    .map(lambda r: (r[2], int(r[4])))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda kv: kv[1], ascending=False)
)
print("\ntop paths by bytes:")
for path, total in bytes_by_path.take(5):
    print(f"  {path:<16}{total:>12,}")

spark.stop()
