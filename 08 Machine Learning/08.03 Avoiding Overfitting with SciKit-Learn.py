# Databricks notebook source
# MAGIC %md 
# MAGIC ###Machine Learning: Avoiding Overfitting with SciKit-Learn
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to introduce you to techniques for splitting data into training and testing sets using SciKit-Learn

# COMMAND ----------

# MAGIC %md In the last lab, you learned how to build a simple classification model using SciKit-Learn. The goal was to teach you the basic pattern of preparing your data as feature (X) and label (y) numpy arrays, instantiating a model, fitting the model and then evaluating the model.  
# MAGIC 
# MAGIC This pattern is fairly consistently implemented across the SciKit-Learn library.  This consistency is what makes SciKit-Learn the most popular of the ML libraries available to Data Scientists today. Many other Python libraries enabling machine learning immulate this same pattern to ease their adoption by those familiar with SciKit-Learn.  That said, we skipped over a critical step that every Data Scientist should perform when training a model: splitting your data into test and training datasets. And so in this lab, we will learn how to perform data splits and add this to our basic pattern.
# MAGIC 
# MAGIC I am going to assume that by this point in your development as a Data Scientist that you understand the need for withholding some of your data from the learning algorithm - [here's a refresher](https://machinelearningmastery.com/a-simple-intuition-for-overfitting/) if you need a quick review - and jump straight into a couple techniques for performing this work.  
# MAGIC 
# MAGIC To get us started, we will load the same dataset as used in the last lab and divide it into features (X) and labels (y):

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

# remove rows with missing votes & reset index on the dataframe
df = df.dropna().reset_index(drop=True)

# extract data for fitting
X = df.drop('party', axis=1).values  # features
y = df['party'].values # labels

# COMMAND ----------

# MAGIC %md Let's now split our data into training and testing datasets:

# COMMAND ----------

from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(
  X, y, 
  test_size = 0.2, 
  random_state=42, 
  stratify=y
  )

# COMMAND ----------

# MAGIC %md The **train_test_split** class accepts our X and y arrays.  It allows us to decide what ratio of our data (20% in this case) to send to test with the remainder being sent to our training set. A random seed for randomly selecting which values get sent to train and test is not typically set to a fixed value, but we've set it here to a seed of 42 so that we will have a consistent split between lab runs.  Again, this is typical of demonstrations but shouldn't be done in the real world.  The fact the random seed can affect our results is a topic we will comeback to shortly.
# MAGIC 
# MAGIC The last parameter specified in this cell indicates that we wish to stratify our data based on information in the labels (y) array.  Remember back to our last lab where we observed there were a few more Democrats than Republicans in our dataset.  While not a terribly imbalanced dataset, we still may wish to preserve the ratio of Democrats to Republicans between our test and training data (as much as is possible).  By configuring the stratify parameter this way, we've done just that.
# MAGIC 
# MAGIC The end-result of the call to train_test_split is that we've split our X and y numpy arrays into training and test instances which we can capture in a single line as shown above.
# MAGIC 
# MAGIC Let's now learn on our training dataset:

# COMMAND ----------

from sklearn.neighbors import KNeighborsClassifier

# instantiate k-nn learner to use 6 nearest neighbors
knn = KNeighborsClassifier(n_neighbors=6)

# fit on the training data
knn.fit(X_train, y_train)

# COMMAND ----------

# MAGIC %md With our model trained (fitted) on our training data, let's evaluate it against the data it did not see, *i.e.* our test data:

# COMMAND ----------

# evaluate on the test data
knn.score(X_test, y_test)

# COMMAND ----------

# MAGIC %md If you compare the score from the cell above from the model run in our last lab, you might notice that we scored a little higher with this particular split of our dataset.  The higher score is an artifact of where we split our data and in no way validates or invalidates the use of splits.  Our goal with splitting data into a testing and training datset is not to improve our accuracy but instead to improve the reliability of our model by avoiding the overfitting to our data.  By withholding information from our learner, we have a valid technique for testing it.
# MAGIC 
# MAGIC Still, where we split our data matters and that's concerning.  If we were to have split on another value, our accuracy score may have shifted up or down (and sometimes by a large amount).
# MAGIC 
# MAGIC To overcome this problem, what we need to do is split our data randomly, train, test, and do it over and over again using other randomly selected splits.  We can then take an average of our scores over these splits to get a better, more reliable estimate of accuracy.  This is called cross-validation:

# COMMAND ----------

from sklearn.model_selection import cross_val_score

# instantiate k-nn learner to use 6 nearest neighbors
knn = KNeighborsClassifier(n_neighbors=6)

# train and score the model 5 times
cv_scores = cross_val_score(knn, X, y, cv=5)

# COMMAND ----------

# MAGIC %md With cross-validation, we divide the dataset into some number of subsets called folds. In this case, we defined 5 folds.  The model is trained on all but one of these folds and then tested against the hold out fold.  This is repeated so that each fold has an opportunity to serve as the test set while each of the remaining folds serves as a part of the training set.
# MAGIC 
# MAGIC In our previous cell with 5 folds this would lead to 5 cycles of training and evaluation.  The **cross_val_score** object returns the score for each of these evaluation runs:

# COMMAND ----------

cv_scores

# COMMAND ----------

# MAGIC %md Averaging across these, we arrive at an average model accuracy:

# COMMAND ----------

print( np.mean(cv_scores) )

# COMMAND ----------

# MAGIC %md And we can see there's a bit of variablity in this number but not so much that we can't trust it:

# COMMAND ----------

print( np.std(cv_scores) )

# COMMAND ----------

# MAGIC %md So, what's the right value to use for the number of folds in a cross-validation?  In general you should look for a value that allows each fold to be statistically representative of the overall dataset.  This takes quite a bit of work to determine so that most Data Scientists simply set the number of folds to either 5 or 10.  
# MAGIC 
# MAGIC Another approach, called "leave-one-out" cross-validation, has you set the parameter to the number of records in the overall dataset.  In this scenario, each individual record serves as a hold-out against the larger set. In the past, this technique wasn't terribly popular given the computational overhead associated with it, but given some patterns around the parallelization of workloads that we will explore later in this class, this technique has become suddenly viable for Data Scientists.

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC Using the same dataset as above, build a party classification model using a LogisticRegression algorithm as found in sklearn.linear_model. Accept all the default parameter settings for this model.  Perform cross-validation with 5 splits.  How do your results compare to those found with the KNN model?

# COMMAND ----------

# Scenario 1 code here

# COMMAND ----------

# MAGIC %md ####Scenario 2
# MAGIC 
# MAGIC Using the same dataset as above, build a party classification model using a Multi-Layer Perceptron (MLPClassifier) from sklearn.neural_network. Accept all the default parameter settings for this model.  Train on a 70:30 train:test split with a random seed of 42.  How do your results compare to those found in Scenario 2 of the last lab?

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
from sklearn.model_selection import cross_val_score

lr = LogisticRegression()
cv_scores = cross_val_score(lr, X, y, cv=5)

print( np.mean(cv_scores) )
print( np.std(cv_scores) )

# COMMAND ----------

# Scenario 2 Code
from sklearn.neural_network import MLPClassifier
from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(
  X, y, 
  test_size = 0.3, 
  random_state=42, 
  stratify=y
  )

mlp = MLPClassifier()
mlp.fit(X_train, y_train)

mlp.score(X_test, y_test)