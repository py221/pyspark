# Databricks notebook source
# MAGIC %md 
# MAGIC ###MLOps: Assembling Pipelines
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to introduce you to how data transformations can be combined with model training and prediction using pipelines.

# COMMAND ----------

# MAGIC %md In the last lab we looked at how data transformations can be assembled as ColumnTransformers to make the application of multiple transformations against a dataset much easier. When applied to a dataset, the ColumnTransformers produce an array of transformed data which can then be used as features with which to train a model.
# MAGIC 
# MAGIC The model produced by this expects any future data provided to it, such as when we use the model to make predictions, to be transformed in the same manner as the array that was used to train it. This means we now need a way to marry our ColumnTransformers with our model (or otherwise be able to re-create the ColumnTransformers with each use of the trained model).  To assist with this, SciKit-Learn provides a object which defines a sequence of steps representing an end-to-end pipeline through which data is transformed and supplied to our model in order to generate output in a consistent manner.
# MAGIC 
# MAGIC To explore this pipeline concept, let's return to the grocery spend data used in previous labs, transform its data, and use the transformed data to train a clustering model:

# COMMAND ----------

# install the most recent version of sklearn to avoid a problem with OHE
dbutils.library.installPyPI('scikit-learn', version='0.22.1')
dbutils.library.restartPython()

# COMMAND ----------

spend = spark.read.csv(
  'wasbs://downloads@smithbc.blob.core.windows.net/wholesale_customers/', 
  header=True, 
  inferSchema=True
  )

spend_df = spend.toPandas()

# COMMAND ----------

# MAGIC %md Notice as we assemble our ColumnTransformers that we are no longer referencing fields by name but instead by index.  We are moving towards model deployment, something we will explore in our next lab.  As we start to assemble models to be deployed to various applications, we need to consider that different applications may send our models data with field names that are beyond our control.  For this reason, we will begin simply referencing incoming data using positional values:

# COMMAND ----------

from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import RobustScaler
from sklearn.compose import ColumnTransformer

# define stages for ColumnTransformer
transformer = ColumnTransformer([
  ('ohe_encoder', OneHotEncoder( drop='first', sparse=False), [0, 1]), # apply OHE to channel and region fields
  ('robust_scaler', RobustScaler(), [2,3,4,5,6,7]) # apply robust scaler to all other fields
  ])

## apply transformations
#X = transformer.fit_transform( spend_df )

# COMMAND ----------

from sklearn.cluster import KMeans

# instantiate and configure clustering model
km = KMeans(
  n_clusters=4, 
  init='random',
  n_init=1000
  )


# COMMAND ----------

# MAGIC %md In the previous cell, we configured the KMeans algorithm to use a value of 4 for k.  If you'd like to explore how that value was derived, take a look at the hyper-parameter tuning lab and construct an evaluation of multiple values of k for the transformed dataset.  Based on my own testing, k=4 appears to be a good value to use with this dataset.
# MAGIC 
# MAGIC We now have our ColumnTransformer defined.  We also have our model instantiated and configured.  We must apply the ColumnTransformer to our incoming data first and then flow that transformed data to our model in order to train it.  This two-step process can be captured in an sklearn Pipeline object:

# COMMAND ----------

from sklearn.pipeline import Pipeline

# combine transformations with model as a pipeline
clf = Pipeline(
  steps=[
    ('transform', transformer),
    ('clustering', km)
    ]
  )

# COMMAND ----------

# MAGIC %md Notice the order in which the steps is defined determines the sequence in which the various steps are executed.  Let's now flow our raw data to the Pipeline object in order to train our model and then use that same raw dataset to create predictions with which we can calculate a model score:

# COMMAND ----------

from sklearn.metrics import silhouette_score

# fit the model
clf.fit(spend_df)

# generate a prediction
predict = clf.predict(spend_df)

# score the predictions
print( silhouette_score( spend_df, predict) )

# COMMAND ----------

# MAGIC %md The Pipeline object presents an interface that's consistent with that of our model and the other data transformation objects in Sci-Kit Learn.  We trigger a training run on the transforms and the model steps using the fit method.  We generate predictions using the predict method.  And we can employ the model just as we would a trained model for scoring.  This greatly simplifies the use of models that require feature engineering.
# MAGIC 
# MAGIC To see a more complex pipeline in action, let's assemble one for our Melbourne housing data:

# COMMAND ----------

# make data available at /dbfs/tmp/melbourne/melb_data.csv
df = spark.read.csv(
  'wasbs://downloads@smithbc.blob.core.windows.net/melbourne_housing/melb_data.csv', 
  sep=',', 
  header=True,
  inferSchema=True
  ).toPandas()

# COMMAND ----------

import pandas as pd
import numpy as np


# separate features from label column
features = df.drop('Price', axis=1)
labels = df['Price']

# COMMAND ----------

# MAGIC %md With the Melbourne data loaded into two DataFrames, one representing our features and the other representing our labels, let's assemble our pipeline including the two steps for feature transformation. Notice again that we are not using field names but instead using positional references for our fields:

# COMMAND ----------

from sklearn.impute import SimpleImputer
from sklearn.preprocessing import RobustScaler
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline

# define stages for missing value ColumnTransformer
missing_value_transformer = ColumnTransformer([
  (  'median_missing', 
      SimpleImputer(missing_values=np.NaN, strategy='median'), 
      [3,10,11,12,13,14,15]
  ),
  (  'most_frequent_missing', 
      SimpleImputer(missing_values=np.NaN, strategy='most_frequent'), 
      [4, 5]
  )
  ])

# define stages for encoding & scaling ColumnTransformer
encoding_scaling_transformer = ColumnTransformer([
  ('get my previously transformed stuff', 'passthrough', list(range(0,4))+[6]),
  ('ohe_encode', OneHotEncoder( drop='first', sparse=False), [7, 8]),
  ('normalize', RobustScaler(), [4, 5])
  ])

# instantiate and configure model
reg = LinearRegression()

# define pipeline
clf = Pipeline(steps=[
  ('missing_values', missing_value_transformer),
  ('encoding_scaling', encoding_scaling_transformer),
  ('regression', reg)
  ])

# COMMAND ----------

# MAGIC %md We now have a pipeline which applies two steps of data transformation before passing data to a linear regression model.  Let's now execute the pipeline to train and score our model:

# COMMAND ----------

# fit the model
clf.fit(features, labels)

# make predictions
predicted_prices = clf.predict(features)

# calculate score
print( clf.score(features, labels) )

# COMMAND ----------

# MAGIC %md While not a great score, our Pipeline object allows us to manage a series of transformations in combination with our model, easing our logic management.

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC Using the 1984 US House of Representatives voting records dataset used in previous labs, construct a pipeline to predict party membership.  The pipeline should:
# MAGIC 
# MAGIC 1. Replace missing votes on the South African legislation with a value of 1
# MAGIC 2. Replace all other missing votes with the most frequently occuring value for that item.
# MAGIC 3. Employ K-Nearest Neighbors to identify party affiliation with a k value of 6.
# MAGIC 4. All column references should be based on position and not column name.
# MAGIC 
# MAGIC HINT The remainder parameter on the column transformer allows you to specify a transformation for any features not explicitly addressed in other steps. You might consider using that to avoid having to identify each feature in the feature set.

# COMMAND ----------

df = (
  spark
    .read
    .csv(
      'wasbs://downloads@smithbc.blob.core.windows.net/party/house-votes-84.data',
      sep=',',
      header=True,
      inferSchema=True,
      nanValue='?'
      )
  ).toPandas()

df.head()

# COMMAND ----------

# scenario 1 code here

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

from sklearn.impute import SimpleImputer
from sklearn.neighbors import KNeighborsClassifier
from sklearn.compose import ColumnTransformer
from sklearn.neighbors import KNeighborsClassifier
from sklearn.pipeline import Pipeline


# separate features and labels
features = df.drop(['party'], axis=1)
labels = df['party']

# missing value imputer for southafrica field
southafrica_imputer = SimpleImputer(
  missing_values=np.NaN, 
  strategy='constant',
  fill_value=1
  )

# missing value imputer for all other fields
other_imputer = SimpleImputer(
  missing_values=np.NaN,
  strategy='most_frequent'
  )

# assemble column transformer
transformer = ColumnTransformer(
  [('south_africa', southafrica_imputer, [-1])],
  remainder = other_imputer # this is new, too!
  )

# configure k-nn model to use 6 nearest neighbors
knn = KNeighborsClassifier(n_neighbors=6)

# assemble pipeline
clf = Pipeline(steps=[
  ('transformations', transformer),
  ('knn', knn)
  ])

# fit and score model
clf.fit(features, labels)
clf.score(features, labels)