"""Shared pipeline definition for the stage-06 scripts.

Everything the model needs - imputing, encoding, scaling - lives INSIDE the
pipeline, so the saved artifact can score raw rows with no side code.
"""
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import (Imputer, OneHotEncoder, SQLTransformer, StandardScaler,
                                StringIndexer, VectorAssembler)

NUMERIC = ["tenure_months", "monthly_charge", "support_calls"]
CATEGORICAL = ["contract", "payment_method", "region"]
RAW_COLUMNS = NUMERIC + CATEGORICAL           # what a scoring input must contain


def build_pipeline(max_iter: int = 50) -> Pipeline:
    # 1. add a double copy of each numeric column (Imputer works on floating point)
    #    and a copy of each categorical column where missing becomes "unknown"
    num = ", ".join(f"CAST({c} AS DOUBLE) AS {c}_num" for c in NUMERIC)
    cat = ", ".join(f"COALESCE({c}, 'unknown') AS {c}_cat" for c in CATEGORICAL)
    cast_fill = SQLTransformer(statement=f"SELECT *, {num}, {cat} FROM __THIS__")
    # 2. numeric: median imputation (the median is learned from training data)
    imputer = Imputer(strategy="median", inputCols=[c + "_num" for c in NUMERIC],
                      outputCols=[c + "_imp" for c in NUMERIC])
    # 3. categorical: index (unseen values land in an extra bucket) then one-hot
    indexers = [StringIndexer(inputCol=c + "_cat", outputCol=c + "_idx", handleInvalid="keep")
                for c in CATEGORICAL]
    onehot = OneHotEncoder(inputCols=[c + "_idx" for c in CATEGORICAL],
                           outputCols=[c + "_vec" for c in CATEGORICAL])
    # 4. one vector, scaled
    assembler = VectorAssembler(inputCols=[c + "_imp" for c in NUMERIC] + [c + "_vec" for c in CATEGORICAL],
                                outputCol="raw_features")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=False, withStd=True)
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=max_iter)
    return Pipeline(stages=[cast_fill, imputer, *indexers, onehot, assembler, scaler, lr])
