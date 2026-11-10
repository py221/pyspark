"""Persisting a fitted pipeline and loading it back (the unit of deployment).

Run from the repo root:  python 06-mlops/03_save_load_pipeline.py
"""
import os  # noqa: E402
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, get_spark, output_path  # noqa: E402

from pyspark.ml import PipelineModel  # noqa: E402
from pyspark.ml.evaluation import BinaryClassificationEvaluator  # noqa: E402
from pyspark.ml.pipeline import Pipeline  # noqa: E402

from churn_pipeline import build_pipeline  # noqa: E402  (sits next to this script)

spark = get_spark("save-load")
data = (spark.read.csv(data_path("churn.csv"), header=True, inferSchema=True)
        .withColumnRenamed("churned", "label"))
train, test = data.randomSplit([0.8, 0.2], seed=21)

pipeline = build_pipeline()
model = pipeline.fit(train)                      # raw rows in, nulls and all
auc = BinaryClassificationEvaluator(labelCol="label")
print("AUC on raw test data (nulls included):", round(auc.evaluate(model.transform(test)), 4))

# --- Save the fitted model: a folder of metadata + one folder per stage ---
path = output_path("06", "churn_model")
model.write().overwrite().save(path)


def show(root, depth=3):
    for dirpath, dirs, files in os.walk(root):
        level = dirpath[len(root):].count(os.sep)
        if level >= depth:
            dirs[:] = []
            continue
        dirs.sort()
        print("  " * level + os.path.basename(dirpath) + "/")
        for f in sorted(files):
            if not f.startswith((".", "_")):
                print("  " * (level + 1) + f)


show(path)

# Stage metadata is plain JSON - handy for auditing what was deployed
meta_dir = os.path.join(path, "stages")
first_stage = sorted(os.listdir(meta_dir))[0]
with open(os.path.join(meta_dir, first_stage, "metadata", "part-00000")) as f:
    print("\nfirst stage metadata:", f.read()[:200], "...")

# --- Load it back: no training data, no re-fitting ---
loaded = PipelineModel.load(path)
a = model.transform(test).select("customer_id", "probability").orderBy("customer_id").collect()
b = loaded.transform(test).select("customer_id", "probability").orderBy("customer_id").collect()
print("\nloaded model reproduces predictions exactly:", a == b)
print("stages:", [type(s).__name__ for s in loaded.stages])
print("learned median for tenure_months:", loaded.stages[1].surrogateDF.first()["tenure_months_num"])

# --- An UNFITTED pipeline can be saved too (the recipe without the learned state) ---
recipe = output_path("06", "churn_recipe")
pipeline.write().overwrite().save(recipe)
print("recipe reloaded with", len(Pipeline.load(recipe).getStages()), "stages")

# Overwrite protection: saving to an existing path without .overwrite() raises.
try:
    model.write().save(path)
except Exception as e:
    print("second save without overwrite:", type(e).__name__)

spark.stop()
