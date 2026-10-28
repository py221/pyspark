"""reduceByKey vs groupByKey, plus aggregateByKey for non-trivial aggregates.

Run from the repo root:  python 01-rdds/03_reduce_vs_group.py
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, get_spark  # noqa: E402

spark = get_spark("reduce-vs-group")
sc = spark.sparkContext

LOG = re.compile(r'^(\S+) \S+ \S+ \[[^\]]+\] "(\S+) (\S+) [^"]*" (\d{3}) (\d+)$')


def parse(line):
    m = LOG.match(line)
    return (m.group(1), int(m.group(5))) if m else None


# (host, bytes) for lines that have a numeric size
host_bytes = sc.textFile(data_path("access_log.txt")).map(parse).filter(bool).cache()
print("pairs:", host_bytes.count(), " distinct hosts:", host_bytes.keys().distinct().count())

# Same answer, different amount of data shuffled across the network.
via_reduce = host_bytes.reduceByKey(lambda a, b: a + b)
via_group = host_bytes.groupByKey().mapValues(sum)

assert dict(via_reduce.collect()) == dict(via_group.collect())
print("both approaches agree on", via_reduce.count(), "hosts")

# The lineages look alike, so measure the difference instead: how many records
# would each approach have to ship through the shuffle?
#   groupByKey  -> every (key, value) pair
#   reduceByKey -> at most one pair per key per partition (after the local combine)
shipped_group = host_bytes.count()
shipped_reduce = host_bytes.mapPartitions(lambda it: [len({k for k, _ in it})]).sum()
print(f"records shuffled by groupByKey : {shipped_group}")
print(f"records shuffled by reduceByKey: {shipped_reduce}")
print("(with big data and few keys the gap is orders of magnitude)")

# Average needs two numbers per key (sum, count) -> aggregateByKey
zero = (0, 0)
seq = lambda acc, v: (acc[0] + v, acc[1] + 1)          # noqa: E731  fold a value into a partition's acc
comb = lambda a, b: (a[0] + b[0], a[1] + b[1])         # noqa: E731  merge accs from two partitions
avg_bytes = host_bytes.aggregateByKey(zero, seq, comb).mapValues(lambda t: t[0] / t[1])
print("avg bytes per request, 3 hosts:", [(h, round(a)) for h, a in avg_bytes.take(3)])

# combineByKey is the general form that the others are built from
combined = host_bytes.combineByKey(
    lambda v: [v],                       # first value seen for a key in a partition
    lambda acc, v: acc + [v],            # another value, same partition
    lambda a, b: a + b,                  # merge partitions
)
print("combineByKey keeps raw values (like groupByKey):", len(combined.first()[1]), "values for one host")

spark.stop()
