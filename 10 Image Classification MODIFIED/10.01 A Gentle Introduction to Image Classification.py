# Databricks notebook source
# MAGIC %md 
# MAGIC ###Images: Intro to Image Classification
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to introduce you to image classification through a simple exercise.

# COMMAND ----------

# MAGIC %md An image is a collection of pixels organized in a array. The position of pixel in the array corresponds to a column and row position in the image, and each pixel is assigned a value which controls how it is rendered to the computer screen.  When viewed together, these pixels form an image that we humans may then recognize.
# MAGIC 
# MAGIC A long-standing goal of Artificial Intelligence and Machine Learning is to bridge the gap between what we as humans see in an image and the 2-dimensional representation managed by computers.  One of the first major efforts in this space was undertaken by the US Census Bureau which commissioned a large number of handwritten digits, *i.e.* numbers from 0 to 9, with which it could attempt to train a computer algorithm to read numerical information provided on Census forms.  This collection of images, known as the [MNIST dataset](http://yann.lecun.com/exdb/mnist/), is very popularly used to introduce students to early and modern image classification techniques. We will begin our introduction to image classification with a focus on this dataset.
# MAGIC 
# MAGIC The MNIST dataset is encoded using a binary format known as IDX. This format is popular with large, dense image collections such as those encoded on DVDs.  
# MAGIC 
# MAGIC To make reading this dataset easier, we've converted the IDX format to CSV.  In the CSV file, each line represents on of 42,000 randomly selected images from the 70,000 image MNIST dataset. Each pixel of each image is rendered to a column named pixel0, pixel1, pixel2, etc.  As the images in this dataset are all grayscale, a single value for each pixel is recorded under each field.  Each image is standardized to a 28 pixel x 28 pixel size so that each line in our CSV will have 784 pixel columns.  A leading column named label will hold a value from 0 to 9, indicating the number the image is supposed to represent:

# COMMAND ----------

import pandas as pd
import numpy as np
from numpy.random import randint
import matplotlib.pyplot as plt


data = spark.read.csv('wasbs://downloads@smithbc.blob.core.windows.net/mnist_csv/train.csv', sep=',', header=True, inferSchema=True).toPandas()
#data = pd.read_csv('/dbfs/tmp/mnist_csv/train.csv')

data.head(5)

# COMMAND ----------

# MAGIC %md Notice that each pixel contains a value ranging from 0 to 255.  The MNIST images are grayscale images so that the value 0 represents black and 255 represents white. Values in between indicate the level of gray the pixel should be rendered with. If we grab a single image and re-organize its pixels into a proper 28 x 28 array, you should be able to make out the digit it represents: 
# MAGIC 
# MAGIC **NOTE** You can run the second cell repeatedly to see different, randomly selected digits in the dataset

# COMMAND ----------

# a simple function to render pixel values as an ASCII grid
def print_image_array(np_array):
  
  pad_length = 3
  
  for i in range(np_array.shape[0]):
    print(''.join( map(
                      lambda p: str(p).rjust(pad_length,'0')[:pad_length], 
                      np_array[i,:]
                      )
                  )
         )

# COMMAND ----------

# randomly select an image record from our dataframe
i = randint(0, data.shape[0])

# get the label for this image
label = data.iloc[i,0]
print('This image is a {0}'.format(label),'\n')

# reshape the pixels into a 28 x 28 structure
image_np = np.reshape(
      data.iloc[i, 1:].values,
      (28,28)
      )

# print the pixels in the array
print_image_array(image_np)

# print(image_np)

# COMMAND ----------

# MAGIC %md Trying to see an image in through raw numpy array is a bit challenging.  How about we use some matplot functionality to render the pixels in a more interpretable manner?:

# COMMAND ----------

# display the array as a grayscale image
plt.imshow( image_np, cmap='gray')

# force the display
display()

# COMMAND ----------

# MAGIC %md Now that we can more clearly see how this image represents a specific digit, let's consider how we might get a computer to recognize it. 
# MAGIC 
# MAGIC In supervised Machine Learning exercises, we train a model to identify a label based on a set of features.  If we treat each pixel as a feature, each of which, in the case of a grayscale image, can have a value between 0 and 255, we have a simple, crude basis for training a model.  Here, we will use a K-Nearest Neighbor model to identify an image based on it's three nearest neighbors:
# MAGIC 
# MAGIC **NOTE** We have hardcoded a k value of 3 based on some previous testing. If you were to perform hyper-parameter tuning on 1 through 10 nearest neighbors, the process would take over 45 minutes to complete on this platform (using Spark to parallelize the work). Just training one model as you will do below will take about 10-15 minutes to complete.

# COMMAND ----------

from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier

# separate features and labels
X = data.drop('label', axis=1).values
y = data['label'].values

# split the dataset
X_train, X_test, y_train, y_test = train_test_split(X,y,test_size=0.4, random_state=42)

# train a nearest neighbors model
knn = KNeighborsClassifier(n_neighbors=3)
knn.fit(X_train, y_train)

# score the model
score = knn.score(X_test, y_test)
print(score)

# COMMAND ----------

# MAGIC %md Using a simple KNN algorithm and treating each pixel as a feature, we are able to train a model to pretty reliably deteremine which digit is represented by each image. 

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC One of the first models attempted with the MNIST dataset was a multi-class Support Vector Machine classifier.  (In SciKit-Learn, this is implemented as the sklearn.svm.LinearSVC class.) Leaving all parameters to their defaults, does this model do better or worse than our KNN model in classifying images?  Does it take more time or less time to train?
# MAGIC 
# MAGIC NOTE An SVM was one of the first Machine Learning techniques applied to this problem by the US Postal Service.

# COMMAND ----------

# your scenario 1 code here
from sklearn.svm import LinearSVC

# COMMAND ----------

# MAGIC %md ####Scenario 2
# MAGIC 
# MAGIC There are many models available for classification.  One popular one is the Random Forest classifier (implemented in SciKit-Learn as the sklearn.ensemble.RandomForestClassifier).  Does this model perform better or worse than the KNN model and does it take longer or shorter to train?train?

# COMMAND ----------

# your scenario 2 code here
from sklearn.ensemble import RandomForestClassifier

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

# your scenario 1 code here
from sklearn.svm import LinearSVC

model = LinearSVC(dual=False)
model.fit(X_train, y_train)

print( model.score(X_test, y_test) )

# COMMAND ----------

# your scenario 2 code here
from sklearn.ensemble import RandomForestClassifier

model = RandomForestClassifier()
model.fit(X_train, y_train)

print( model.score(X_test, y_test) )

# COMMAND ----------

