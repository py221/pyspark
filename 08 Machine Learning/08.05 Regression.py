# Databricks notebook source
# MAGIC %md 
# MAGIC ###Machine Learning: Regression
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to review how the SciKit-Learn pattern is implemented with other types of ML models, namely regression models.

# COMMAND ----------

# MAGIC %md Up to this point, we've examined the SciKit-Learn pattern for model development using classification algorithms.  But the beauty of SciKit-Learn is that these same patterns are applicable to other classes of models, such as regression models.
# MAGIC 
# MAGIC For this exercise, we will use the GapMinder dataset commonly explored by R users.  This data set provides life expectancy values by country and year along with potentially correlating values such as population size and GDP per capita:

# COMMAND ----------

# import numpy and pandas
import numpy as np
import pandas as pd

# read the dataset to pandas df
df = (
  spark
    .read
    .csv(
      'wasbs://downloads@smithbc.blob.core.windows.net/gapminder/gapminder.tsv',
      sep='\t',
      header=True,
      inferSchema=True
      )
  ).toPandas()

df.head()

# COMMAND ----------

# MAGIC %md Before jumping into our training exercise, we should examine how some variables may or may not correlate with one another:

# COMMAND ----------

df.corr()

# COMMAND ----------

# MAGIC %md We can visualize this with a simple heatmap:

# COMMAND ----------

import seaborn as sns

# visualize correlations in a heatmap
sns.heatmap(
  df.corr(), 
  square=True, 
  cmap='RdYlGn'
  )

# display the visual
display()

# COMMAND ----------

# MAGIC %md Let's now assemble our features and labels. Note that we are ignoring *continent* but will return to it in later labs:

# COMMAND ----------

X = df[['gdpPercap','pop','year']].values
y = df['lifeExp'].values

# COMMAND ----------

# MAGIC %md Now we can train a linear regression model to predict life expectancy.  Note that while we are training a different type of model, the basic pattern of instantiate/configure, train, and then score remains the same:

# COMMAND ----------

from sklearn.linear_model import LinearRegression
from sklearn.model_selection import cross_val_score

reg = LinearRegression()
cv_scores = cross_val_score(reg, X, y, cv=5)

print("Average 5-Fold CV Score: {}".format(np.mean(cv_scores)))

# COMMAND ----------

# MAGIC %md While not a great accuracy score, the point of this exercise was to show you how patterns established when training a classifier translate to the training of a regression model.

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC Using the same dataset as above, train a regression model using the DecisionTreeRegressor algorithm as found in sklearn.tree. Use a 10-fold cross-validation with this algorithm and display the score you arrive at.

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

from sklearn.tree import DecisionTreeRegressor
from sklearn.model_selection import cross_val_score

regressor = DecisionTreeRegressor()

score = np.mean(
    cross_val_score(regressor, X, y, cv=10)
    )

print(score)