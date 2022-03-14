# Databricks notebook source
# MAGIC %md 
# MAGIC ###Machine Learning: Spark ML
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to introduce you to Spark ML for use with some common Machine Learning tasks in Spark.

# COMMAND ----------

# MAGIC %md **NOTE** We are using the **pyspark.ml** and not the **pyspark.mllib** libraries to do this work.  Both libraries go by the common name of "Spark ML" and "Spark ML Lib" but pyspark.ml is based on Spark Dataframes while pyspark.mllib is based on Spark RDDs.  pyspark.mllib, based on RDDs, is considered a legacy capability and should not be used for new development.

# COMMAND ----------

# MAGIC %md Many Machine Learning algorithms are inherently single-threaded but not all.  In an effort to make leveraging the distributed capabilities of Spark easier for Data Scientists, Spark ML Lib (pyspark.ml) was created to apply ML capabilities to Spark DataFrames.  To see this in action, let's prepare our data as before but not extract a pandas DataFrame, leaving the data in a Spark DataFrame:

# COMMAND ----------

from pyspark.sql.functions import expr
from pyspark.sql.functions import min, count, sum
from pyspark.sql import functions as F
from pyspark.sql.types import *

import numpy as np
import pandas as pd

schema = StructType([
  StructField('InvoiceNo', IntegerType()),
  StructField('StockCode', StringType()),
  StructField('Description', StringType()),
  StructField('Quantity', IntegerType()),
  StructField('InvoiceDate', TimestampType()),
  StructField('UnitPrice', FloatType()),
  StructField('CustomerID', IntegerType()),
  StructField('Country', StringType())
  ])

# read data to Spark DataFrame
raw = spark.read.csv(
  'wasbs://downloads@smithbc.blob.core.windows.net/online_retailer_kaggle/data.csv', 
  header=True, 
  timestampFormat='M/d/yyyy H:mm',
  schema=schema
  ).withColumn('InvoiceDate', expr('cast(InvoiceDate as date)')) # deal with ugly date formatting

# calculate base measures
base = (raw
          .withColumn('Amount', raw.UnitPrice * raw.Quantity)
          .withColumn('DaysSince', expr("datediff('2011-12-31', InvoiceDate)"))
       )

# calculate raw RFM metrics
rfm = (
    base
      .groupBy('CustomerId')
      .agg(
        min(base.DaysSince).alias('Recency'),
        count(base.InvoiceNo).alias('Frequency'),
        sum(base.Amount).alias('Monetary')
        )
  )

# get quantiles for each measure
r_quantile = rfm.approxQuantile('Recency', np.linspace(0.1, 0.9, num=9).tolist(), 0)
f_quantile = rfm.approxQuantile('Frequency',np.linspace(0.1, 0.9, num=9).tolist(), 0)
m_quantile = rfm.approxQuantile('Monetary', np.linspace(0.1, 0.9, num=9).tolist(), 0)

scores = (
  rfm
  	.withColumn('r_score',
		F.when(rfm.Recency >= r_quantile[8], 1).
		when(rfm.Recency >= r_quantile[7] , 2).
		when(rfm.Recency >= r_quantile[6] , 3).
        when(rfm.Recency >= r_quantile[5] , 4).
        when(rfm.Recency >= r_quantile[4] , 5).
        when(rfm.Recency >= r_quantile[3] , 6).
        when(rfm.Recency >= r_quantile[2] , 7).
        when(rfm.Recency >= r_quantile[1] , 8).
        when(rfm.Recency >= r_quantile[0] , 9).
		otherwise(10)
		)
	.withColumn('f_score', 
		F.when(rfm.Frequency > f_quantile[8], 10).
		when(rfm.Frequency > f_quantile[7] , 9).
		when(rfm.Frequency > f_quantile[6] , 8).
        when(rfm.Frequency > f_quantile[5] , 7).
        when(rfm.Frequency > f_quantile[4] , 6).
        when(rfm.Frequency > f_quantile[3] , 5).
        when(rfm.Frequency > f_quantile[2] , 4).
        when(rfm.Frequency > f_quantile[1] , 3).
        when(rfm.Frequency > f_quantile[0] , 2).
		otherwise(1)
		)
	.withColumn('m_score', 
		F.when(rfm.Monetary > m_quantile[8], 10).
		when(rfm.Monetary > m_quantile[7] , 9).
		when(rfm.Monetary > m_quantile[6] , 8).
        when(rfm.Monetary > m_quantile[5] , 7).
        when(rfm.Monetary > m_quantile[4] , 6).
        when(rfm.Monetary > m_quantile[3] , 5).
        when(rfm.Monetary > m_quantile[2] , 4).
        when(rfm.Monetary > m_quantile[1] , 3).
        when(rfm.Monetary > m_quantile[0] , 2).
		otherwise(1)
        )
	)

display(scores)

# COMMAND ----------

# MAGIC %md To prepare our data for training, we must convert our features into a nupmy array.  This is done using a **VectorAssembler** which will create a new field in our Spark DataFrame employing a custom data type which aligns with what we are familiar with as a numpy array:

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(inputCols=['r_score', 'f_score', 'm_score'], outputCol='features')
vectorized = assembler.transform(scores)

display(vectorized)

# COMMAND ----------

# MAGIC %md Now we can train our model using the Spark ML Lib KMeans algorithm which understands how to work with the vectorized data now in our Spark DataFrame. Notice that we are using a value of k = 4 based on the hyperparameter work performed in the last lab:

# COMMAND ----------

from pyspark.ml.clustering import KMeans

kmeans = KMeans(
  featuresCol='features',
  predictionCol='prediction',
  k=4,
  maxIter=1000)

model = kmeans.fit(vectorized)

# COMMAND ----------

# MAGIC %md Using our model, we can make some cluster assignment predictions:

# COMMAND ----------

# Make predictions
predictions = model.transform(vectorized)
display(predictions)

# COMMAND ----------

# MAGIC %md We can now perform a quick evaluation using an evaluator provided by Spark ML Lib:

# COMMAND ----------

from pyspark.ml.evaluation import ClusteringEvaluator

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
print("Average silhouette: " + str(silhouette))

# COMMAND ----------

# MAGIC %md The Spark ML library employs a SciKit-Learn pattern for model fitting and prediction. As a result, the method calls applied to the Spark DataFrame should be relatively familiar to you given the work you've already done in SciKit-Learn.  This is purposefully part of the design of Spark ML, intended to allow Data Scientists to easily adopt Spark ML.
# MAGIC 
# MAGIC That said, Spark ML hasn't really taken off as a preferred platform for Machine Learning, largely because it's ability to keep up with the breadth of functionality available through SciKit-Learn and other Python ML libraries just isn't there.  In that regard, you should have knowledge of Spark ML as a Spark developer but it will not likely become your primary means of performing this kind of work.