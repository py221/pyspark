# Databricks notebook source
# MAGIC %md 
# MAGIC ###Machine Learning: Spark for Data Prep
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to introduce you to how Spark can be used to prepare data for machine learning with sklearn.

# COMMAND ----------

# MAGIC %md SciKit-Learn and many other Python libraries enabling statistical and ML work expect data to be submitted as either a pandas DataFrame or a numpy array.  This doesn't mean that we can't still use Spark to prepare our datawhen the distributed capabilities of a Spark DataFrame may be helpful.
# MAGIC 
# MAGIC To illustrate this, let's consider a scenario where we want to perform customer segmentation (clustering) using sklearn.cluster.KMeans. We will cluster our customers based on three commonly used marketing measures: recency, frequency & monetary spend. (This si a classic segmentation technique known as *RFM Segmentation*.) 
# MAGIC 
# MAGIC [The dataset](https://www.kaggle.com/carrie1/ecommerce-data) we will use for this, doesn't contain these exact data elements. Instead, we'll use Spark SQL to derive these features before performing our ML training with sklearn.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import expr

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

display(raw)

# COMMAND ----------

# MAGIC %md The dataset we are working with contains customer purchase history information across a couple years.  Using this data, we can calculate how recently a purchase was made (using the end of the data reporting period as our cutoff) and the amount spent for each purchase:
# MAGIC 
# MAGIC **NOTE** The dataset terminates on December 31, 2011.  We'll use that date as the current date for recency calculations.

# COMMAND ----------

from pyspark.sql.functions import expr

# calculate base measures from dataset
base = (raw
          .withColumn('Amount', raw.UnitPrice * raw.Quantity)
          .withColumn('DaysSince', expr("datediff('2011-12-31', InvoiceDate)"))
       )

display(base)

# COMMAND ----------

# MAGIC %md We can now calculate recency as the minimum number of days since a purchase was made, the frequency of purchases as a simple count of purchases, and the total amount spent during the reporting period for each unique customer:

# COMMAND ----------

from pyspark.sql.functions import min, count, sum

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
  
display(rfm)

# COMMAND ----------

# MAGIC %md With our raw recency, frequency and monetary value calculated, we now need to score each customer for membership in a 10% quantile.  To do this, we'll use the **approxQuantile()** method to calculate the cutoffs between the 0-10%, 10%-20%, *etc.* quantiles for our recency, frequency and monetary values. We'll then use those quantile cutoffs to score each customer a 1, 2, 3, ... up to 10 based on which band they fall into in each metric:
# MAGIC 
# MAGIC **NOTE** I'm not providing a lot of detailed explaination for these techniques.  At this point, you should be able to go online and research each SparkSQL method to better understand how they work.  This is commonly how you will learn new techniques as a practicing Data Scientist.

# COMMAND ----------

from pyspark.sql import functions as F
import numpy as np

# get quantiles for each measure
r_quantile = rfm.approxQuantile('Recency', np.linspace(0.1, 0.9, num=9).tolist(), 0)
r_quantile

# COMMAND ----------

from pyspark.sql import functions as F
import numpy as np

# get quantiles for each measure
r_quantile = rfm.approxQuantile('Recency', np.linspace(0.1, 0.9, num=9).tolist(), 0)
f_quantile = rfm.approxQuantile('Frequency',np.linspace(0.1, 0.9, num=9).tolist(), 0)
m_quantile = rfm.approxQuantile('Monetary', np.linspace(0.1, 0.9, num=9).tolist(), 0)

# score customers for rfm quantiles
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

# MAGIC %md Leveraging the distributed nature of Spark, we've manipulated our dataset to produce the set required for model training.  This dataset is significantly smaller than the one that was used to produce it so that we can safely take it to a pandas DataFrame using the Spark DataFrame's **toPandas()** method:

# COMMAND ----------

scores_df = scores.toPandas()
scores_df.head()

# COMMAND ----------

# MAGIC %md With our dataset prepared and loaded to a pandas DataFrame, we can perform our k-means clustering on the data.  We will tackle this in our next lab.