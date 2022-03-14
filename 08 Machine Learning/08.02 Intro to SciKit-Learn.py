# Databricks notebook source
# MAGIC %md 
# MAGIC ###Machine Learning: An Introduction to SciKit-Learn
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to introduce you to how Machine Learning models are built using SciKit-Learn.

# COMMAND ----------

# MAGIC %md SciKit-Learn is a popular Python library for the development of Machine Learning models. In this lab, we will walk through a simple binary (two-class) classification exercise using this library so that you may get familiar with the basic patterns it employs in building just about any type of ML model.
# MAGIC 
# MAGIC For this exercise, we will make use of a [data set](https://archive.ics.uci.edu/ml/datasets/congressional+voting+records) which tracks the votes for the members of the 1984 US House of Representatives on 16 key pieces of legislation.  The party affiliation (Democrat or Republican) for each member is recorded with each member's votes, and our goal will be to learn to predict party affiliation based on voting patterns.
# MAGIC 
# MAGIC The dataset we are using resides in cloud storage and because the pandas library we will be using for data access and manipulation does not have a native understanding of cloud storage, we will use a quick Spark-hack to read the data to pandas: 

# COMMAND ----------

import pandas as pd
import numpy as np

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

df

# COMMAND ----------

# MAGIC %md Reading the data from cloud storage, we note that the file contains a header row, is comma-separated, and uses a question-mark '?' to denote a missing/nanValue.  Later in these labs, we'll look at how to deal with missing values, but to keep things as simple as possible right now, we will simply drop any record with a missing entry:

# COMMAND ----------

# remove rows with missing votes & reset index on the dataframe
df = df.dropna()

# reset index to be a range from 0 to n with no gaps
df = df.reset_index(drop=True)

# inspect the shape of the dataframe.  How many rows and columns do we have?
print( df.shape )

# COMMAND ----------

# MAGIC %md Let's now examine the remaining data in our DataFrame:

# COMMAND ----------

# view some of the data
df.head(10)

# COMMAND ----------

# MAGIC %md Before building a model, let's see how many Democrats and Republicans (who voted on all 16 of these key pieces of legislation) were in the House in 1984:

# COMMAND ----------

df.groupby('party')['party'].count()

# COMMAND ----------

# MAGIC %md Our dataset consists of 124 Democrats and 108 Republicans. Let's take a look at how members of these parties voted on a couple pieces of legislation:
# MAGIC 
# MAGIC **NOTE** Don't worry if you are unfamiliar with [matplotlib](https://matplotlib.org/) and [seaborn](https://seaborn.pydata.org/).  These are popular data visualization packages employed in Python (and well worth learning) but you could similarly generate a result set and visualize it using the charting capabilities built into this and most popular notebook systems.

# COMMAND ----------

# EDUCATION VOTE

# import plotting libraries
import matplotlib.pyplot as plt
import seaborn as sns

# instantiate a figure
fig = plt.figure()

# make figure a column chart counting occurances
sns.countplot(
  data=df, # occurances are from df dataframe
  x='education',  # count yes/no votes in education column
  hue='party', # break down counts by party (and color code by that as well)
  palette='RdBu', # use the Red-Blue palette
  hue_order=['republican', 'democrat'] # use republican for red and democrat for blue
  )

# relabel the 0 and 1 values on the x-axis as No & Yes
plt.xticks([0,1], ['No', 'Yes']) 

# fig.show() doesn't work in Databricks so just display the default figure
display()

# COMMAND ----------

# MISSLE VOTE

fig = plt.figure()

sns.countplot(
  data=df,
  x='missle',  # count yes/no votes in MISSILE column
  hue='party', 
  palette='RdBu',
  hue_order=['republican', 'democrat'] 
  )

plt.xticks([0,1], ['No', 'Yes']) 

display()

# COMMAND ----------

# MAGIC %md As is typical today, votes on key legislative issues break down pretty much along party lines though there are some Democrats who occassionally vote with Republicans and vice-versa.  If these party-aligned patterns hold up around the other legislative items in this dataset, we should be able to make some pretty reliable predictions.
# MAGIC 
# MAGIC To build our model, we will use a k-Nearest Neighbors (KNN) algortihm.  This is a classic algorithm for binary classification (not to be confused with the k-means algorithm for clustering and segmentation exercises).  KNN attempts to classify a point based on the value of its k-number of nearest neighbors.  The distance between neighbors will be calculated leveraging a simple distance calculation that looks at the combined difference between numerical features associated with each point in the dataset.
# MAGIC 
# MAGIC The value we set of k is configurable and in another lab we will explore how we might determine an ideal value for k.  But for now, we'll just set k to 6 so that we will make predictions of party affiliation based on the 6 closest members in the dataset.
# MAGIC 
# MAGIC Before training our model, we need to extract our features, *i.e.* the votes, and our label, *i.e.* party affiliation, into numpy arrays  The algorithms in SciKit-Learn and many other popular Python Machine Learning libraries expect features and labels as numpy arrays:

# COMMAND ----------

# extract data for fitting
X = df.drop('party', axis=1).values  # features
y = df['party'].values # labels

# COMMAND ----------

# MAGIC %md Notice that our features are extracted to a numpy array named *X* and our label is extracted to a numpy array named *y*.  While you have the flexibility to name your features and label arrays anything you'd like, the use of an upper-case *X* and a lower-case *y* are the norm in most SciKit-Learn documentation and public code samples.
# MAGIC 
# MAGIC Now let's instantiate and configure our [KNN model](https://scikit-learn.org/stable/modules/generated/sklearn.neighbors.KNeighborsClassifier.html#sklearn-neighbors-kneighborsclassifier):

# COMMAND ----------

from sklearn.neighbors import KNeighborsClassifier

# configure k-nn model to use 6 nearest neighbors
knn = KNeighborsClassifier(n_neighbors=6)

# COMMAND ----------

# MAGIC %md While we've elected to configure only the number of nearest neighbors (n_neighbors) the algorithsm will learn from, there are many other parameters we could manipulate to control the behavior of the algorithm:

# COMMAND ----------

knn.get_params()

# COMMAND ----------

# MAGIC %md At this point, the model's algorithm is configured but it hasn't learned anything from our data.  So we pass in our features and our label and ask it to learn from it, *i.e.* **fit** the model:

# COMMAND ----------

# fit the classifier to the data
knn.fit(X,y)

# COMMAND ----------

# MAGIC %md With a fitted model in place, we can pass back our voting records data and ask it to **predict** party affiliation for each member of the US House:

# COMMAND ----------

# make party predictions 
y_pred = knn.predict(X)

y_pred

# COMMAND ----------

# MAGIC %md The numpy array returned by **predict()** for a KNN model is a 0-dimension array.  We'll want to join this data with the voting records information and party affiliation data housed in our pandas DataFrame.  To do that, we need to convert the 0-dimension array to a 1-dimension array and then convert that to a DataFrame that we can **concat** to our original dataset:

# COMMAND ----------

# convert y_pred nparray to pandas df
y_pred = pd.DataFrame( 
  y_pred.reshape(-1,1), # convert array to a 1-d np matrix (the -1 tells reshape to not reshape the rows axis)
  columns=['predicted'] # provide a label for the 1 field in this 1-d matrix
  )

# concat the predictions with our original data along the column axis
pd.concat(
  [ y_pred, df], 
  axis=1
    ).head(10)

# COMMAND ----------

# MAGIC %md A quick examination of the data shows us that our model does a pretty good job of classifying members of the US House based on their voting history.  It's not perfect but from a visual examination we can see it does okay.  Still, let's go back to calculate just how *okay* it is:

# COMMAND ----------

print( 'Mean accuracy: {0}'.format( knn.score(X,y) ) )

# COMMAND ----------

# MAGIC %md Our model comes pre-built with a **score** method which calculates a key evaluation metric for our data.  Internally, this method is comparing an array equivalent to the one we produced as y_pred to the array y being passed to it.  Per the SciKit Learn documentation for the [score method of this algorithm](https://scikit-learn.org/stable/modules/generated/sklearn.neighbors.KNeighborsClassifier.html#sklearn.neighbors.KNeighborsClassifier.score), the value returned represents the mean accuracy of the model for this dataset.

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC Using the same dataset as above, build a party classification model using a LogisticRegression algorithm as found in sklearn.linear_model. Accept all the default parameter settings for this model.  How does this model score relative to your KNN model?

# COMMAND ----------

# Scenario 1 code here

# COMMAND ----------

# MAGIC %md ####Scenario 2
# MAGIC 
# MAGIC Repeat Scenario 1 but this time using a Multi-Layer Perceptron (MLPClassifier) from sklearn.neural_network.  How does this model score relative to your previous two attempts?

# COMMAND ----------

# Scenario 2 code here

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

# Scenario 1 Code
from sklearn.linear_model import LogisticRegression

lr = LogisticRegression()
lr.fit(X,y)

lr.score(X,y)

# COMMAND ----------

from sklearn.neural_network import MLPClassifier

mlp = MLPClassifier()
mlp.fit(X,y)

mlp.score(X,y)