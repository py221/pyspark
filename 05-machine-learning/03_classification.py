"""Binary classification: logistic regression vs random forest, with proper metrics.

Run from the repo root:  python 05-machine-learning/03_classification.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.spark import data_path, get_spark, output_path  # noqa: E402

from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier  # noqa: E402
from pyspark.ml.evaluation import (BinaryClassificationEvaluator,  # noqa: E402
                                   MulticlassClassificationEvaluator)
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

spark = get_spark("classification")
churn = (spark.read.csv(data_path("churn.csv"), header=True, inferSchema=True)
         .dropna().withColumnRenamed("churned", "label"))
churn = churn.withColumn("label", F.col("label").cast("double"))

train, test = churn.randomSplit([0.75, 0.25], seed=11)
print("class balance (train):")
train.groupBy("label").count().show()

# Majority-class baseline: accuracy looks fine when classes are imbalanced, which is
# exactly why accuracy alone can mislead.
majority = train.groupBy("label").count().orderBy(F.desc("count")).first()["label"]
base_acc = test.filter(F.col("label") == majority).count() / test.count()
print(f"always predict {majority:.0f}: accuracy {base_acc:.3f}")

cats = ["contract", "payment_method", "region"]
stages = [StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep") for c in cats]
stages.append(OneHotEncoder(inputCols=[c + "_idx" for c in cats], outputCols=[c + "_vec" for c in cats]))
stages.append(VectorAssembler(
    inputCols=["tenure_months", "monthly_charge", "support_calls"] + [c + "_vec" for c in cats],
    outputCol="features"))

auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
acc = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")
f1 = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")

models = {
    "logistic regression": LogisticRegression(featuresCol="features", labelCol="label", maxIter=50),
    "random forest": RandomForestClassifier(featuresCol="features", labelCol="label",
                                            numTrees=60, maxDepth=6, seed=3),
}
fitted = {}
for name, est in models.items():
    model = Pipeline(stages=stages + [est]).fit(train)
    pred = model.transform(test)
    fitted[name] = pred
    print(f"{name:<20} AUC {auc.evaluate(pred):.3f}  accuracy {acc.evaluate(pred):.3f}  F1 {f1.evaluate(pred):.3f}")

# --- Confusion matrix for the logistic model ---
pred = fitted["logistic regression"]
pred.groupBy("label").pivot("prediction", [0.0, 1.0]).count().na.fill(0).orderBy("label").show()

# --- The output columns of a classifier ---
pred.select("label", "rawPrediction", "probability", "prediction").show(3, truncate=False)

# Changing the decision threshold trades precision for recall
tp = pred.filter("label = 1 AND prediction = 1").count()
fp = pred.filter("label = 0 AND prediction = 1").count()
fn = pred.filter("label = 1 AND prediction = 0").count()
print(f"precision {tp / max(tp + fp, 1):.3f}   recall {tp / max(tp + fn, 1):.3f}")

spark.stop()
