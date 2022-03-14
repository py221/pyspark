# Databricks notebook source
# MAGIC %md **REMINDER** You must use a Databricks 6.4 ML cluster for this homework assignment

# COMMAND ----------

# MAGIC %md ###General Instructions
# MAGIC In this assignment, you will need to complete the code samples where indicated to accomplish the given objectives. **Be sure to run all cells** and export this notebook as an HTML with results included.  Upload the exported HTML file to Canvas by the assignment deadline.

# COMMAND ----------

# MAGIC %md ####Assignment
# MAGIC Unlike previous exercises, you will not be provided any sample code from which to work.  You will be given some very high-level instructions and are expected to figure out a solution from there.

# COMMAND ----------

# MAGIC %md The MNIST-Fashion dataset is a collection of 60,000 training and 10,000 test images identifying fashion products in 10 categories: 
# MAGIC 
# MAGIC | Label        | Description          |
# MAGIC | ------------- |:-------------:|
# MAGIC | 0	| T-shirt/top| 
# MAGIC | 1	| Trouser| 
# MAGIC | 2| 	Pullover| 
# MAGIC | 3	| Dress| 
# MAGIC | 4	| Coat| 
# MAGIC | 5	| Sandal| 
# MAGIC | 6	| Shirt| 
# MAGIC | 7	| Sneaker| 
# MAGIC | 8 | 	Bag| 
# MAGIC | 9	| Ankle boot| 
# MAGIC 
# MAGIC Build a CNN consisting of the following layers to classify images to a particular category:
# MAGIC 
# MAGIC 1. 2D convolutional layer leveraging 64 nodes and using a 3x3 filter
# MAGIC 2. MaxPooling layer with a 2x2 kernel
# MAGIC 3. Flattening layer 
# MAGIC 4. Dense layer consisting of 96 nodes
# MAGIC 5. Dense output layer with 10 nodes
# MAGIC 
# MAGIC Use standard activation functions with each convolutional and dense layer. Be sure to apply a 25% dropout after the pooling and first dense layer. 
# MAGIC 
# MAGIC Retrieve your data using the [built-in fashion_mnist dataset](https://keras.io/datasets/#fashion-mnist-database-of-fashion-articles) found within Keras.  Be sure to apply appropriate data transformations. 
# MAGIC 
# MAGIC Adjust the number of epochs to achieve a > 90% accuracy. Try both the Adadelta and the Stochastic Gradient Descent optimization algorithms (with a learning rate of 0.01 as your starting point) with a loss function of categorical_crossentropy when you compile. Stop whenever gives you the required result.
# MAGIC 
# MAGIC Do not use Horovod. Be sure to run the last cell of this assignment so that predictions are presented along side sample images.
# MAGIC 
# MAGIC HINT: Review the first lab on CNNs for the patterns to follow. You will want to go with a lower learning rate than used in that lab or your model can get stuck.  Ideally, it should take you between 10 and 20 epochs to reach the required accuracy. With the Stochastic Gradent Descent optimizer, you might try bumping up your momentum to 0.9 to speed up its learning.

# COMMAND ----------

import tensorflow as tf
import keras
from keras.models import Sequential
from keras.layers import Dense, Dropout, Flatten
from keras.layers import Conv2D, MaxPooling2D
from keras.optimizers import Adadelta

import matplotlib.pyplot as plt
import numpy as np

# COMMAND ----------

# retrieve dataset and apply transformations


# COMMAND ----------

# construct & compile CNN


# COMMAND ----------

# train the CNN


# COMMAND ----------

# score the CNN
score = model.evaluate(X_test, y_test, verbose=0)
print('Test accuracy:', score[1])

# COMMAND ----------

# visualize results
items = {
  0:'T-Shirt/Top',
  1:'Trouser',
  2:'Pullover', 
  3:'Dress',
  4:'Coat',
  5:'Sandal', 
  6:'Shirt',
  7:'Sneaker', 
  8:'Bag',
  9:'Ankle Boot'  
  }

plt.figure(figsize=(25,5))

for i, y in enumerate(y_test[0:5]):
  
  yhat = model.predict(X_test[i].reshape(1,28,28,1))
  
  s = plt.subplot(1,5,i+1)
  s.set_title('item = {0} (prob = {1:.2%})'.format( items[np.where(yhat == np.amax(yhat))[1][0]], np.amax(yhat)))
  plt.imshow(X_test[i][:,:,0], cmap='gray') 
  
display()