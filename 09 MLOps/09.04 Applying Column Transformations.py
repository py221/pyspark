# Databricks notebook source
# MAGIC %md 
# MAGIC ###MLOps: Applying Column Transformations
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to introduce you to how we can apply multiple column transformations to various features using SciKit-Learn.

# COMMAND ----------

# MAGIC %md Before starting this lab, let's ensure we have the correct version of SciKit-Learn installed:

# COMMAND ----------

# install the most recent version of sklearn to avoid a problem with OHE
dbutils.library.installPyPI('scikit-learn', version='0.22.1')
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md We've explored various column transformations.  Quite often, our datasets will require that one or more transformations are applied to a subset of features while other transformations are applied to others. In this lab, we will explore how multiple transformations can be applied to feature arrays in an efficient and easy to manage manner.
# MAGIC 
# MAGIC Let's start by revisiting the wholesale customer dataset used in the last lab:

# COMMAND ----------

spend = spark.read.csv(
  'wasbs://downloads@smithbc.blob.core.windows.net/wholesale_customers/', 
  header=True, 
  inferSchema=True
  )

spend_df = spend.toPandas()
spend_df

# COMMAND ----------

# MAGIC %md Notice this dataset consists of several continous variables that need to be scaled (normalized) and a couple of categorical variables that require one-hot encoding.
# MAGIC 
# MAGIC To do this, we will define scalers and encoders as we did before but apply them to specific features using a **ColumnTransformer**.  A ColumnTransformer acts as a sequencer for the application of various transformations and allows specific fields to be targeted for each:

# COMMAND ----------

from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import RobustScaler
from sklearn.compose import ColumnTransformer

# define stages for ColumnTransformer
transformer = ColumnTransformer([
  ('ohe_encoder', OneHotEncoder( drop='first', sparse=False), ['Region', 'Channel']), # apply OHE to region and channel fields
  ('robust_scaler', RobustScaler(), spend_df.columns[2:]) # apply robust scaler to all other fields
  ])

# apply transformations
X = transformer.fit_transform( spend_df )

# print feature array generated
X

# COMMAND ----------

# MAGIC %md Because so much is taking place in the application of a ColumnTransformer, it can be helpful to view the contents of the nparray in a bit more detail.  To facilitate this, let's convert the array into a DataFrame and inspect its first few rows:

# COMMAND ----------

import pandas as pd

pd.DataFrame(X).head()

# COMMAND ----------

# MAGIC %md While the fields do not have names, we can deduce the first field is a one-hot encoding of the two-value region field, the next two are a one-hot encoding of the three-value channel field, and the remaining fields are the scaled department spend values. It's important to note that while the two categorical fields in this dataset are pre-encoded as ordinals, within a ColumnTransformer, you do not need to convert categorical fields into integer ordinals before applying the OHE.  This is simply a perk of using a ColumnTransformer.

# COMMAND ----------

# MAGIC %md In previous labs, we've provided a *Try It Yourself* scenario within which you've transformed various fields in the Melbource housing dataset.  To further explore the ColumnTransformer, we'll use this same dataset in our formal lab:

# COMMAND ----------

# MAGIC %md Now, let's take a assemble a DataFrame and take a look at the data:

# COMMAND ----------

import pandas as pd
import numpy as np

# make data available at /dbfs/tmp/melbourne/melb_data.csv
df = spark.read.csv(
  'wasbs://downloads@smithbc.blob.core.windows.net/melbourne_housing/melb_data.csv', 
  sep=',', 
  header=True,
  inferSchema=True
  ).toPandas()

df.head()

# COMMAND ----------

# MAGIC %md To this dataset, we need to apply several transformations:
# MAGIC 1. Replace missing values with the median value in the Rooms, Bedroom2, Bathroom, Car, Landsize, BuildingArea and YearBuilt fields.
# MAGIC 2. Replace missing values with the most frequently occuring value in the Type and Method fields.
# MAGIC 3. Apply one-hot encoding to the Type and Method fields.
# MAGIC 4. Normalize the Landsize and BuildingArea fields using the Robust Scaler.
# MAGIC 
# MAGIC Let's now apply all the transforms in one shot:

# COMMAND ----------

from sklearn.impute import SimpleImputer
from sklearn.preprocessing import RobustScaler
from sklearn.preprocessing import OneHotEncoder

from sklearn.compose import ColumnTransformer

# define stages for ColumnTransformer
transformer = ColumnTransformer([
  (  'median_missing', 
      SimpleImputer(missing_values=np.NaN, strategy='median'), 
      ['Rooms','Bedroom2','Bathroom','Car','Landsize','BuildingArea','YearBuilt']
  ),
  ('most_frequent_missing', SimpleImputer(missing_values=np.NaN, strategy='most_frequent'), ['Type', 'Method']),
  ('ohe_encode', OneHotEncoder( drop='first', sparse=False), ['Type', 'Method']),
  ('normalize', RobustScaler(), ['Landsize', 'BuildingArea'])
  ])

# apply transformations
X = transformer.fit_transform( df )

pd.DataFrame(X).head()

# COMMAND ----------

# MAGIC %md Looking at the returned dataset, you should notice a couple problems.  First, our method and type fields are coming across as is and in combination with their OHE fields.  Second, the BuildingArea field had missing values replaced but the scaled version, *i.e.* the last field in the result set, has missing values in it.  What's going on?
# MAGIC 
# MAGIC It's important to note that the ColumnTransformer applies transformations to the fields in the incoming array or DataFrame.  Transformations should be thought of as being applied in no particular order and with no dependencies between them.  So when we need to apply multiple transformations, such as replacing missing values before performing one-hot encoding or replacing values before scaling, we need to define multiple ColumnTransformers, each working off the results of its predecessors.
# MAGIC 
# MAGIC Let's do this by starting with our missing value transformations:

# COMMAND ----------

# define stages for ColumnTransformer
missing_value_transformer = ColumnTransformer([
  (  'median_missing', 
      SimpleImputer(missing_values=np.NaN, strategy='median'), 
      ['Rooms','Bedroom2','Bathroom','Car','Landsize','BuildingArea','YearBuilt']
  ),
  ('most_frequent_missing', SimpleImputer(missing_values=np.NaN, strategy='most_frequent'), ['Type', 'Method'])
  ])

# apply transformations
X_01 = missing_value_transformer.fit_transform( df )

pd.DataFrame(X_01).head()

# COMMAND ----------

# MAGIC %md We can now use the results of this set of transformations to apply additional transformations. Notice that because we are workign with a nparray as our input, we must not identify fields by column index.  To help you keep track of the fields returned by a ColumnTransformer, notice that the fields in the resulting array are ordered by the order for the transforms and the order of the fields identified with each transform.  With that in mind, fields 4 and 5 are the transformed BuildingArea and YearBuilt fields and fields 6 and 7 are (more obviously given their string values) the Type and Method fields:

# COMMAND ----------

encoding_scaling_transformer = ColumnTransformer([
  ('ohe_encode', OneHotEncoder( drop='first', sparse=False), [7, 8]),
  ('normalize', RobustScaler(), [4, 5])
  ])

X_02 = encoding_scaling_transformer.fit_transform( X_01 )

pd.DataFrame(X_02).head()

# COMMAND ----------

# MAGIC %md But now we have more problems.  What happened to the Rooms, Bedroom2, Bathroom, Car and YearBuilt fields we transformed in the prior cell?  It turns out that the ColumnTransformer is configured to drop any fields to which a transformation is not applied.  We can override this by setting it's remainder arguement to 'passthrough' which we will do now to bring our previously transformed fields along for the ride:

# COMMAND ----------

encoding_scaling_transformer = ColumnTransformer(
  [
  ('ohe_encode', OneHotEncoder( drop='first', sparse=False), [7, 8]),
  ('normalize', RobustScaler(), [4, 5])
  ], 
  remainder='passthrough'
  )

X_02 = encoding_scaling_transformer.fit_transform( X_01 )

pd.DataFrame(X_02).head()

# COMMAND ----------

# MAGIC %md Alternatively, we can construct a 'passthrough' transform to identify the fields we wish to passthrough our ColumnTransformer.  This can be helpful when setting the remainder parameter to passsthrough would return many fields that would otherwise not be needed:

# COMMAND ----------

encoding_scaling_transformer = ColumnTransformer([
  ('get my previously transformed stuff', 'passthrough', list(range(0,4))+[6]),
  ('ohe_encode', OneHotEncoder( drop='first', sparse=False), [7, 8]),
  ('normalize', RobustScaler(), [4, 5])
  ])

X_02 = encoding_scaling_transformer.fit_transform( X_01 )

pd.DataFrame(X_02).head()

# COMMAND ----------

# MAGIC %md While not perfect, the ColumnTransformer is a step in the right direction for simplifying the application of multiple transformations to a dataset ahead of model training.  In our next lab, we will look at how we might sequence these transformations along with model training and prediction to build an end-to-end pipeline for the passing of untransformed data to a model for the purposes of generating a prediction.