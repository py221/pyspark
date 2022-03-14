# Databricks notebook source
# MAGIC %md 
# MAGIC ###MLOps: Normalization
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to introduce you to how we can normalize continuous variables with SciKit-Learn.

# COMMAND ----------

# MAGIC %md In this lab, we will cluster customers based on their annual spend by department within a wholesale club chain.  The dataset and details on it can be found at: https://archive.ics.uci.edu/ml/datasets/Wholesale+customers
# MAGIC 
# MAGIC The fields in the data set are as follows:
# MAGIC 
# MAGIC 1. Fresh: annual spending (m.u.) on fresh products (Continuous)
# MAGIC 2. Milk: annual spending (m.u.) on milk products (Continuous)
# MAGIC 3. Grocery: annual spending (m.u.) on grocery products (Continuous)
# MAGIC 4. Frozen: annual spending (m.u.) on frozen products (Continuous)
# MAGIC 5. Detergents_Paper: annual spending (m.u.) on detergents and paper products (Continuous)
# MAGIC 6. Delicatessen: annual spending (m.u.) on and delicatessen products (Continuous)
# MAGIC 7. Channel: customers Channel - Horeca (Hotel/Restaurant/Cafe) or Retail channel (Nominal)
# MAGIC 8. Region: customers Region Lisnon, Oporto or Other (Nominal) 

# COMMAND ----------

spend = spark.read.csv(
  'wasbs://downloads@smithbc.blob.core.windows.net/wholesale_customers/', 
  header=True, 
  inferSchema=True
  )

spend_df = spend.toPandas()

# COMMAND ----------

# MAGIC %md Our dataset contains two categorical variables: Channel & Region.  We'll ignore these for now and explore transformations applicable to the annual spend totals associated with the different grocery store departments:

# COMMAND ----------

# generate statistics on the department spend values
spend_df.drop(['Channel','Region'], axis=1).describe()

# COMMAND ----------

# MAGIC %md Spending varies considerably between departments with the Delicassen having the lowest average spend and Fresh having the highest. If we were to train something like a regression model on these data, the differences in scale would not be a concern but if we were to train something like a k-means clustering model, these differences in scale would affect the "distance" calculations performed by the algorithm, assigning more wieght to those features with higher average values.  In a scenario like this, we need to scale our features so that each field receives equal consideration.
# MAGIC 
# MAGIC One common way to do this is to scale the values in a feature between the feature max and the feature min.  This ensures that all values in each feature sit between a range of 1 and 0:

# COMMAND ----------

import pandas as pd
from sklearn.preprocessing import MinMaxScaler

# instantiate the scaler
scaler = MinMaxScaler()

# train the scaler on the min and max values in each department field
scaler.fit( spend_df.drop(['Channel','Region'], axis=1) )

# scale the department field data between min (0) and max (1)
scaled = scaler.transform( spend_df.drop(['Channel','Region'], axis=1) )

scaled

# COMMAND ----------

# MAGIC %md To better see the impact of the MinMaxScaler, let's transform our array into a DataFrame and recalculate our basic statistics on the transformed data:

# COMMAND ----------

# get column names
column_names = spend_df.drop(['Channel','Region'],axis=1).columns

# convert the array into a dataframe
scaled_df = pd.DataFrame(scaled, columns=column_names)

# calculate statistics
scaled_df.describe()

# COMMAND ----------

# MAGIC %md Notice how each field now has a min of 0 and a max of 1.  The MinMaxScaler is ideal for making sure each field receives equal overall weight but it does tend to be a little over-reaching when there are significant outliers in a feature. In those situations, we commonly make use of the StandardScaler which scales based on the standard deviation of a feature:

# COMMAND ----------

from sklearn.preprocessing import StandardScaler

# instantiate the scaler
scaler = StandardScaler()

# learn the mean and standard deviations for each feature
scaler.fit( spend_df.drop(['Channel','Region'], axis=1) )

# scale to the number of standard deviations from mean
scaled = scaler.transform( spend_df.drop(['Channel','Region'], axis=1) )

scaled

# COMMAND ----------

column_names = spend_df.drop(['Channel','Region'],axis=1).columns

scaled_df = pd.DataFrame(scaled, columns=column_names)

scaled_df.describe()

# COMMAND ----------

# MAGIC %md The StandardScaler is a popular scalar that tells us where a value sits relative to the mean.  There's still a susceptibility to outliers that we need to consider but its less impactful on the overall dataset.  Still, if we know we have significant outliers, we may wish to use the RobustScaler which scales around a median and quantile values which minimize the impact of these anomalous values:

# COMMAND ----------

from sklearn.preprocessing import RobustScaler

scaler = RobustScaler()

scaler.fit( spend_df.drop(['Channel','Region'], axis=1) )

scaled = scaler.transform( spend_df.drop(['Channel','Region'], axis=1) )

scaled

# COMMAND ----------

scaled_df = pd.DataFrame(scaled, columns=spend_df.drop(['Channel','Region'],axis=1).columns)
scaled_df.describe()

# COMMAND ----------

# MAGIC %md The choice of which scaler to employ depends on your dataset and your model objectives but sklearn provides you with many options for this work.

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC Using the Melbourne Housing dataset, normalize the Landsize variable using the Robust scaler.

# COMMAND ----------

# make data available at /dbfs/tmp/melbourne/melb_data.csv
df = spark.read.csv(
  'wasbs://downloads@smithbc.blob.core.windows.net/melbourne_housing/melb_data.csv', 
  sep=',', 
  header=True,
  inferSchema=True
  ).toPandas()

# COMMAND ----------

#scenario 1 code here

# COMMAND ----------

# MAGIC %md ####Answers (scroll down)
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p>

# COMMAND ----------

import pandas as pd
import numpy as np

from sklearn.preprocessing import RobustScaler

scaler = RobustScaler()

scaler.fit( df[['Landsize']] )

scaled = scaler.transform( df[['Landsize']] )

scaled