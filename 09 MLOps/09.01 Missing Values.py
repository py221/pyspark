# Databricks notebook source
# MAGIC %md 
# MAGIC ###MLOps: Missing Values
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to review how to handle missing values leveraging SciKit-Learn.

# COMMAND ----------

# MAGIC %md Missing values are a common occurance.  Take a look at the 1984 US House of Representatives voting record data set to see an example of this where missing values are encoded in the text file by a question mark:

# COMMAND ----------

import pandas as pd
import numpy as np

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

# MAGIC %md To see how wide spread this is, let's count the number of NaN values relative to the records in the set:

# COMMAND ----------

# count the rows in the dataframe
print('Total Records\t{0}\n'.format(df.shape[0]))

# count the number of NaN values by field
print('Missing Values by Field:\n', df.isnull().sum())

# COMMAND ----------

# MAGIC %md Missing values are the most common data problem facing those of us doing Machine learning.  These values are problematic because make our observations from which we are learning incomplete and many algorithms will simply not accept them.  So to prepare our data for ML, we have to address the missing values.
# MAGIC 
# MAGIC One strategy for dealing with this is to simply drop records or fields containing missing values.  In situations where a field or row is almost completely made up of missing values, this can be a worthwhile strategy as there is little to learn from that data element. But when missing values are scattered throughout our observations, as they are in our voting record data set, this can lead to the loss of a lot of valueable information:

# COMMAND ----------

# rows and columns for full dataframe
print( df.shape )

# rows and columns when records with any NaN values dropped
print(df.dropna().shape)

# COMMAND ----------

# MAGIC %md In situations like this, a better strategy may be to keep our rows and fields intact and simply calculate, *i.e.* impute, replacement values for our missing values. The imputed value is often the mean or median value when we are dealing with a continuous, numerical feature, the most frequently occuring value for a categorical feature, or simply a constant value as determined based on our understanding of the data
# MAGIC 
# MAGIC To impute replacement values in sklearn, our first step is to extract our features to a numpy array, just as we might do in preparation for model training:

# COMMAND ----------

# display the features before replacement of missing values
X = df.drop('party', axis=1).values
X

# COMMAND ----------

# MAGIC %md Next, we define an imputer and "fit" it to our dataset.  "Fitting" in this sense allows the imputer to determine which values to use for imputation based on a strategy it's been configured to employ.  For example, we may use a **SimpleImputer** to replace missing values with the most frequently occuring values in a given field.  "Fitting" in this scenario is teaching the imputer which values in each field represents the most frequently occuring value:

# COMMAND ----------

from sklearn.impute import SimpleImputer

# configure the missing value imputer
impute = SimpleImputer(
  missing_values=np.NaN, 
  strategy='most_frequent'
  )

# read the input data to determine replacement values
impute.fit(X)

# COMMAND ----------

# MAGIC %md To see what the imputer has learned, we can call its **statistics_** method to return the most frequently occuring value for each field in the array:

# COMMAND ----------

impute.statistics_

# COMMAND ----------

# MAGIC %md Leveraging this information, we can now transform our feature array, replacing any missing values with the most frequently occuring value for the field within which it resides.  This leaves us with a cleansed feature set we can now take into our model training exercise:

# COMMAND ----------

X_cleansed = impute.transform(X)
X_cleansed

# COMMAND ----------

# MAGIC %md One of the more interesting features of the sklearn.impute.SimpleImputer class is that missing values can be identified by a string, *e.g.* '?', an integer, *e.g.* 0 or -1, or an explicit missing values, *e.g.* np.NaN. This gives you quite a bit of flexibility in tackling missing values.  That said, it does require that missing values be consistently identified between fields.  In addition, you may have situations where one strategy for imputation may work well with one column but a different strategy is needed for another.  
# MAGIC 
# MAGIC In both these situations, you may want to define multiple imputers to apply to different columns. This can be a bit tricky but our first step is to train our imputers on the appropriate fields in our feature array:

# COMMAND ----------

# missing value imputer for southafrica field
southafrica_impute = SimpleImputer(
  missing_values=np.NaN, 
  strategy='constant',
  fill_value=1
  )
# train this imputer on the last feature
southafrica_impute.fit(X[:,-1].reshape(-1,1)) # reshape from 1d array to 2d array

# missing value imputer for all other fields
other_impute = SimpleImputer(
  missing_values=np.NaN,
  strategy='most_frequent'
  )
# train this imputer on all but the last feature
other_impute.fit(X[:,:-1])

# COMMAND ----------

# MAGIC %md With our imputers trained, we will create an empty array with the same shape as our original feature array.  We will then then populate each of the fields in the empty array using our different imputers:

# COMMAND ----------

# create an empty array sized the same as our feature set
X_cleansed = np.empty(X.shape)

# set the last feature to be the last imputed field
X_cleansed[:,-1] = southafrica_impute.transform(
                  X[:,-1].reshape(-1,1)
                  ).reshape(-1)

# set all but the last features to be all but the last imputed field
X_cleansed[:, :-1] = other_impute.transform(X[:,:-1])

X_cleansed

# COMMAND ----------

# MAGIC %md With our cleansed feature array, we can now proceed.

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC Using the Melbourne Housing dataset, extract the features on which we will eventually train our model and replace missing values in the feature array using the median value for each field.  The dataset contains a header row and the fields on which you will train the model are:</p>
# MAGIC 
# MAGIC * Rooms
# MAGIC * Bedroom2
# MAGIC * Bathroom
# MAGIC * Car
# MAGIC * Landsize
# MAGIC * BuildingArea
# MAGIC * YearBuilt

# COMMAND ----------

df = spark.read.csv(
  'wasbs://downloads@smithbc.blob.core.windows.net/melbourne_housing/melb_data.csv', 
  sep=',', 
  header=True,
  inferSchema=True
  ).toPandas()

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

# scenario 1 code
import pandas as pd
import numpy as np

from sklearn.impute import SimpleImputer

# extract features
X = df[['Rooms','Bedroom2','Bathroom','Car','Landsize','BuildingArea','YearBuilt']].values

# configure the missing value imputer
impute = SimpleImputer(
  missing_values=np.NaN, 
  strategy='median'
  )
impute.fit(X)

# transform features with missing value imputation
X_cleansed = impute.transform(X)
X_cleansed