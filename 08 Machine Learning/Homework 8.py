# Databricks notebook source
# MAGIC %md ###General Instructions
# MAGIC In this assignment, you will need to complete the code samples where indicated to accomplish the given objectives. **Be sure to run all cells** and export this notebook as an HTML with results included.  Upload the exported HTML file to Canvas by the assignment deadline.

# COMMAND ----------

# MAGIC %md ####Assignment
# MAGIC Unlike previous exercises, you will not be provided any sample code from which to work.  You will be given some very high-level instructions and are expected to figure out a solution from there.

# COMMAND ----------

# MAGIC %md The UCI Machine Learning Repository makes available a popular dataset identifying various properties of three cultivars of Italian wine grapes: https://archive.ics.uci.edu/ml/datasets/Wine. These can be used to build a multi-class identifier with which measurements of these properties can be used to predict which cultivar is being observed.
# MAGIC 
# MAGIC We have not explictly addressed multi-class classification in our labs, but SciKit-Learn makes available multiple algorithms for building multi-class predictors. For this exercise, use sklearn.tree.DecisionTreeClassifier to build a multi-class predictor, identifying the grape cultivar based on the provided attributes.  To prevent overfitting, train on 70% of the provided data and test on the remaining 30%. No data transformations should be performed for this exercise. There are no missing values in the dataset and the dataset is well stratified across the three cultivars. Be sure to provide accuracy scores where indicated below.

# COMMAND ----------

df = spark.read.csv('wasbs://downloads@smithbc.blob.core.windows.net/wine/wine.csv', sep=',', header=True, inferSchema=True)
display(df)

# COMMAND ----------

# read the data to a pandas DataFrame and assemble feature and label arrays
import pandas as pd

grapes_df = df.toPandas()
grapes_df.head()

# COMMAND ----------

# split the data into training and test data sets
from sklearn.model_selection import train_test_split

y = grapes_df.cultivar
X = grapes_df.drop('cultivar', axis=1)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, stratify=y, random_state=444)

# COMMAND ----------

# train your model using the training data
from sklearn.tree import DecisionTreeClassifier

dt = DecisionTreeClassifier()
dt.fit(X_train, y_train)

# COMMAND ----------

# score your model using the test data
from sklearn.metrics import mean_squared_error as MSE

y_pred = dt.predict(X_test)

# eval 
mse_dt = MSE(y_pred, y_test)
rmse_dt = mse_dt**(1/2)

print(f'mean squared error: {mse_dt:.4f}\n'
      f'root mean squared error: {rmse_dt:.4f}\n'
      f'accuracy: {dt.score(X, y):.4f}')

# COMMAND ----------

