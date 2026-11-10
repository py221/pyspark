"""Fast checks that the building blocks behave as the notes claim."""
import sys
from pathlib import Path

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import Imputer, StringIndexer, VectorAssembler
from pyspark.sql import Window
from pyspark.sql import functions as F

from common.spark import data_path

ROOT = Path(__file__).resolve().parent.parent


def test_make_data_outputs_exist():
    for name in ["access_log.txt", "orders.csv", "customers.csv", "sensor_readings.tsv",
                 "events.jsonl", "houses.csv", "churn.csv"]:
        assert (ROOT / "data" / name).stat().st_size > 0
    assert len(list((ROOT / "data" / "stream_source").glob("*.json"))) == 5


def test_rdd_word_count(spark):
    sc = spark.sparkContext
    words = sc.parallelize(["a b a", "b a"]).flatMap(str.split)
    counts = dict(words.map(lambda w: (w, 1)).reduceByKey(lambda x, y: x + y).collect())
    assert counts == {"a": 3, "b": 2}


def test_transformations_are_lazy(spark):
    sc = spark.sparkContext
    seen = sc.accumulator(0)

    def bump(x):
        seen.add(1)
        return x

    rdd = sc.parallelize(range(100), 2).map(bump)
    assert seen.value == 0          # nothing has run yet
    rdd.count()
    assert seen.value == 100


def test_reduce_and_group_agree(spark):
    pairs = spark.sparkContext.parallelize([("x", 1), ("y", 2), ("x", 3)])
    assert dict(pairs.reduceByKey(lambda a, b: a + b).collect()) == \
        dict(pairs.groupByKey().mapValues(sum).collect())


def test_dataframe_aggregate_and_window(spark):
    orders = spark.read.csv(data_path("orders.csv"), header=True, inferSchema=True)
    assert orders.count() == 1200
    w = Window.partitionBy("category").orderBy(F.desc("quantity"), "order_id")
    ranked = orders.withColumn("rn", F.row_number().over(w))
    assert ranked.filter("rn = 1").count() == orders.select("category").distinct().count()


def test_regex_parsing_finds_bad_lines(spark):
    pattern = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) [^"]*" (\d{3}) (\S+)$'
    raw = spark.read.text(data_path("access_log.txt"))
    bad = raw.filter(~F.col("value").rlike(pattern)).count()
    assert bad == 5


def test_explode_drops_empty_arrays(spark):
    events = spark.read.json(data_path("events.jsonl"))
    assert events.select(F.explode("items")).count() < events.select(F.explode_outer("items")).count()


def test_partitioned_write_and_pruning(spark, tmp_path):
    df = spark.createDataFrame([("a", 1), ("b", 2), ("a", 3)], ["k", "v"])
    out = str(tmp_path / "lake")
    df.write.partitionBy("k").parquet(out)
    assert sorted(p.name for p in Path(out).iterdir() if p.name.startswith("k=")) == ["k=a", "k=b"]
    plan = spark.read.parquet(out).filter("k = 'a'")._jdf.queryExecution().executedPlan().toString()
    assert "PartitionFilters: [isnotnull(k#" in plan


def test_streaming_available_now(spark, tmp_path):
    schema = "event_time TIMESTAMP, page STRING, user_id STRING, latency_ms INT"
    stream = spark.readStream.schema(schema).json(data_path("stream_source"))
    q = (stream.writeStream.format("memory").queryName("smoke_stream")
         .option("checkpointLocation", str(tmp_path / "ckpt"))
         .outputMode("append").trigger(availableNow=True).start())
    q.awaitTermination()
    assert spark.table("smoke_stream").count() == 300


def test_pipeline_fit_save_load(spark, tmp_path):
    df = spark.createDataFrame(
        [(1.0, 20.0, "a", 0.0), (2.0, None, "b", 1.0), (3.0, 40.0, "a", 1.0),
         (4.0, 50.0, "b", 0.0), (5.0, 60.0, "a", 1.0), (6.0, 10.0, "b", 0.0)],
        "x double, y double, c string, label double")
    pipe = Pipeline(stages=[
        Imputer(strategy="median", inputCols=["y"], outputCols=["y_imp"]),
        StringIndexer(inputCol="c", outputCol="c_idx", handleInvalid="keep"),
        VectorAssembler(inputCols=["x", "y_imp", "c_idx"], outputCol="features"),
        LogisticRegression(maxIter=10),
    ])
    model = pipe.fit(df)
    path = str(tmp_path / "m")
    model.write().overwrite().save(path)
    loaded = PipelineModel.load(path)
    a = [r.prediction for r in model.transform(df).orderBy("x").collect()]
    b = [r.prediction for r in loaded.transform(df).orderBy("x").collect()]
    assert a == b


def test_churn_pipeline_scores_raw_rows(spark):
    sys.path.insert(0, str(ROOT / "06-mlops"))
    from churn_pipeline import build_pipeline

    data = (spark.read.csv(data_path("churn.csv"), header=True, inferSchema=True)
            .withColumnRenamed("churned", "label"))
    model = build_pipeline(max_iter=10).fit(data)
    scored = model.transform(data.limit(20))
    assert scored.filter(F.col("probability").isNull()).count() == 0
