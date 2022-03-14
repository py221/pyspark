# Databricks notebook source
# MAGIC %md ###General Instructions
# MAGIC In this assignment, you will need to complete the code samples where indicated to accomplish the given objectives. **Be sure to run all cells** and export this notebook as an HTML with results included.  Upload the exported HTML file to Canvas by the assignment deadline.

# COMMAND ----------

# MAGIC %md ####Assignment
# MAGIC Unlike previous exercises, you will not be provided any sample code from which to work.  You will be given some very high-level instructions and are expected to figure out a solution from there.

# COMMAND ----------

# MAGIC %md Abalone are a popular shellfish. Pressure on the abalone population from the fishing industry have caused the species to go into decline.  Efforts have been underway for sometime to limit the harvest of abalone to fish above a certain age, but there is no way to accurately detect the age of an abalone without counting the layers of its shell, with each layer indicating 1.5 years of life, and counting the layers requires the harvesting of the animal.
# MAGIC 
# MAGIC Researchers from the University of Tasmania have compiled a [dataset](https://archive.ics.uci.edu/ml/datasets/Abalone) of physical characteristics, many of which can be measured without harming the animal, along with a count of rings for a large number of abalone harvested off the Australian coast.  Use these data, stored at **wasbs://downloads@smithbc.blob.core.windows.net/abalone/** for your convenience, to build a regression model to predict the number of rings (and therefore the age) of abalone based on the following characteristics:
# MAGIC 
# MAGIC * sex
# MAGIC * mm_length
# MAGIC * mm_diameter
# MAGIC * mm_height
# MAGIC * g_whole_weight
# MAGIC 
# MAGIC Replace any missing values for the last 4 of these characteristics with a median value.  Replace any missing values for sex with the most frequently occuring value. Handle sex as a categorical feature.  Build a linear regression model and package your data transformations with the model as a pipeline to aid in the conversion of your model into an application that could be deployed to aid fisherman collecting abalone.
# MAGIC 
# MAGIC Be sure to score your model for accuracy and use a 5-fold cross-validation to ensure you reduce the impact of random splits on your results.  Print the model score where indicated in the cells below.

# COMMAND ----------

# install the most recent version of sklearn to avoid a problem with OHE
dbutils.library.installPyPI('scikit-learn', version='0.22.1')
dbutils.library.restartPython()

# COMMAND ----------

# read the data to a pandas DataFrame and assemble feature and label arrays
abalone = spark.read.csv(
  'wasbs://downloads@smithbc.blob.core.windows.net/abalone/', 
  header=True, 
  inferSchema=True
  )

abalone_df = abalone.toPandas()

label = abalone_df.rings
feat = abalone_df.iloc[:,0:5]

# COMMAND ----------

list(range(1,5))

# COMMAND ----------

# assemble your model pipeline
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import RobustScaler
import numpy as np

# imputation
missing_val_imputer = ColumnTransformer([
  (   'most_freq',
      SimpleImputer(missing_values='I', strategy='most_frequent'),
      [0]
  ),
  ('median', SimpleImputer(missing_values=np.NaN, strategy='median'), list(range(1,5)))
  ])

# ohe & std
ohe_n_standardization = ColumnTransformer([
  ('ohe', OneHotEncoder(drop='first', sparse=False), [0]),
  ('stdardz', RobustScaler(), list(range(1,5)))
  ])

# lr model 
reg = LinearRegression()

# pipeline
lm = Pipeline(steps=[
  ('missing_val', missing_val_imputer),
  ('ohe_n_stdardz', ohe_n_standardization),
  ('reg', reg)
  ])

# COMMAND ----------

# train your model using a 5-fold cross-validation
from sklearn.model_selection import cross_val_score

cv_scores = cross_val_score(lm, feat, label, cv=5, scoring='r2', n_jobs=-1)
print(cv_scores)

# COMMAND ----------

# present your model score
print(np.mean(cv_scores))

# COMMAND ----------

