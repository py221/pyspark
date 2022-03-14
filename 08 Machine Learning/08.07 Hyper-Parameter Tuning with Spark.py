# Databricks notebook source
# MAGIC %md 
# MAGIC ###Machine Learning: Hyper-Parameter Tuning with Spark
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to introduce you to how Spark can be used as means of performing distributed hyper-parameter tuning.

# COMMAND ----------

# MAGIC %md In our last lab, we used Spark to assemble a pandas DataFrame that we intended to take into our model training exercise. Let's recreate that pandas DataFrame now:

# COMMAND ----------

from pyspark.sql.functions import expr
from pyspark.sql.functions import min, count, sum
from pyspark.sql import functions as F

import pandas as pd
import numpy as np

from pyspark.sql.types import *

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

scores_df=scores.toPandas()

scores_df.head()

# COMMAND ----------

# MAGIC %md Using the k-means algorithm, we'll assign our data points to a k-number of clusters.  Ideally, our clusters have clear, non-overlapping boundaries and minimum distances between cluster members and their cluster centriods.  The silhouette measure is often used to assess the degree of overlap and distance from center within our clusters and the average silhouette measure for all the clusters generated is used to assess the strength of the overall clustering effort.  
# MAGIC 
# MAGIC The value we choose for k along with the number of iterations we take in determining cluster centers can have a big impact on this score.  As a result, we often evaluate our segmentation efforts across a range of k-values and set the number of iterations to a high number which ideally allows our clusters to arrive at a stable centeroid.  To perform this work in a timely manner, we can use Spark to partition the work across the workers of a cluster.
# MAGIC 
# MAGIC With this in mind, we will ask each worker in our cluster to train and score a model provided a specific value of k.  For a worker to perform this work, it will need access to the training dataset in its entirity.  To that end, let's replicate our pandas DataFrame across our workers so that each worker has its own complete copy of the DataFrame against which to operate:

# COMMAND ----------

inputs = scores.select('r_score', 'f_score', 'm_score').toPandas()
inputs_broadcast = sc.broadcast(inputs)

# COMMAND ----------

display(inputs_broadcast.value)

# COMMAND ----------

# MAGIC %md Against this dataset, we will now train and score various models.  To enable that work, we'll define a funtion.  That function will accept a value of k and then train a k-means model with that given value of k against the now replicated dataset and return a score for that model:

# COMMAND ----------

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

# train and score a model for a given value of k
def train_model( k ):
  # configure model to use provided k
  km = KMeans(
    n_clusters=k, 
    init='random',
    n_init=1000
    )
  # fit the model to the replicated dataset
  kmeans = km.fit_predict( inputs_broadcast.value )
  # score the model
  silhouette = silhouette_score( inputs_broadcast.value , kmeans)
  # return the value for k along with the score
  return (k, float(silhouette))

# COMMAND ----------

# MAGIC %md Now we can assemble the values of k we wish to evaluate.  We'll load these hyper-parameter values into a Spark RDD so that the values of k are replicated across the workers in our cluster:

# COMMAND ----------

rdd = sc.range(2, 21, step=1, numSlices=sc.defaultParallelism)

# COMMAND ----------

# MAGIC %md By applying our function to this RDD, we are now forcing the workers to evaluate each value for k independently across the workers in our cluster.  The work being performed is parallelized with the values of k in the RDD.  The RDD returned represents the consolidated output of this work:

# COMMAND ----------

out = rdd.map(train_model)

# COMMAND ----------

# MAGIC %md We can now take the RDD output of this process and flip it into a Spark DataFrame.  This will allow us to perform a quick visualization that should point us to an appropriate value of k for our model:

# COMMAND ----------

display( spark.createDataFrame( out, schema=['clusters','silhouette']) )

# COMMAND ----------

# MAGIC %md From the results, it looks like values of 2 and 4 produces a pretty reliable set of clusters.  Using this knowledge, let's retrain a model for that k=4 and assign each value in our dataset to a particular cluster:

# COMMAND ----------

km = KMeans(
  n_clusters=4, 
  init='random',
  n_init=1000
  )
clusters = km.fit_predict( inputs )

# COMMAND ----------

# MAGIC %md And now we have predict cluster assignment as before:

# COMMAND ----------

results = pd.concat(
  [scores_df, pd.DataFrame(clusters.reshape(-1,1), columns=['cluster'])],
  axis=1
  )

results.head()

# COMMAND ----------

# MAGIC %md Before taking off, it's important to remember that we pinned a copy of our dataset into the memory of each worker node in our cluster.  To free up resources, we should release that dataset as shown here:

# COMMAND ----------

inputs_broadcast.unpersist(blocking=True)

# COMMAND ----------

