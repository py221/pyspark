# Databricks notebook source
# MAGIC %md 
# MAGIC ###Images: Convolutional Neural Networks
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to introduce you to CNNs through the construction of a simple image classification model.

# COMMAND ----------

# MAGIC %md Convolutional Neural Networks (CNNs) are a form of Deep Neural Network, specialized for the purposes of image classification and object detection. In a CNN, multiple hidden layers learn to apply convolutions (filters) and pooling to images in order to detect relevant features.  Learning takes place through the adjustment of the (initially randomized) weights applied to each input feature (pixel).  Through forward and backward propogation, the weights are adjusted and the model learns to detect features that are critical to the task at hand.
# MAGIC 
# MAGIC **NOTE** It is assumed you are familiar with the basics of neural networks.
# MAGIC 
# MAGIC The architecture of a CNN typically consists of multiple convolutional layers and pooling layers.  The number of layers, their sequencing, filter sizes, activation functions, etc. are largely based on patterns established by others in various demonstrations and competitions.  The question of "Why do we use this architecture for this CNN?" is often answered with a reference to work done by others who had the time and resources to explore various options and determined that given the state of the current arts that a given architecture provided the best predictive capability given the resources and time afforded them.  
# MAGIC 
# MAGIC With that in mind, we will explore CNNs through the construction of a simple deep neural network consisting of two convolutional layers followed by a single pooling layer because this is simple enough to produce some impressively good results on the limited computational resources we have available to us.  To get started, let's load the libraries we will make use of for this:

# COMMAND ----------

dbutils.library.installPyPI('keras')
dbutils.library.installPyPI('tensorflow')
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md In our last lab, we examined how to organize images to serve as inputs into the training of a CNN.  In order to limit network strain, we will tap into a set of the MNIST images that comes pre-installed with Keras:

# COMMAND ----------

import keras
from keras.models import Sequential
from keras.layers import Dense, Dropout, Flatten
from keras.layers import Conv2D, MaxPooling2D

import matplotlib.pyplot as plt

import numpy as np
import pandas as pd

# COMMAND ----------

(X_train, y_train), (X_test, y_test) = keras.datasets.mnist.load_data()

# COMMAND ----------

# MAGIC %md Let's take a look at how the data in our X and y datasets are organized:

# COMMAND ----------

print('The X_train dataset is organized as a {0} with a shape of {1}'.format(type(X_train), X_train.shape))
print('The X_test dataset is organized as a {0} with a shape of {1}'.format(type(X_test), X_test.shape))
print('The y_train dataset is organized as a {0} with a shape of {1}'.format(type(y_train), y_train.shape))
print('The y_test dataset is organized as a {0} with a shape of {1}'.format(type(y_test), y_test.shape))

# COMMAND ----------

# MAGIC %md If we grab the first five entries from each of the training datasets, we can see how the images and labels are organized:

# COMMAND ----------

plt.figure(figsize=(25,5))

for i, y in enumerate(y_train[0:5]):
  s = plt.subplot(1,5,i+1)
  s.set_title('figure = {0}'.format(y))
  plt.imshow(X_train[i], cmap='gray') 
  
display()

# COMMAND ----------

# MAGIC %md In preparation for training, we need to alter the shape and data type of our X datasets to align them with the expectations of Keras.  
# MAGIC 
# MAGIC In Keras, the arrays that make up our images must be organized as three-dimensional arrays, i.e. (rows, columns, channels).  As we are working with grayscale images, we will only have 1 channel:

# COMMAND ----------

# get shape of original images
img_rows, img_cols = X_train[0].shape

# restructure arrays for (rows, columns, channels)
X_train = X_train.reshape(X_train.shape[0], img_rows, img_cols, 1)
X_test = X_test.reshape(X_test.shape[0], img_rows, img_cols, 1)

# record new input shape for layers
input_shape = (img_rows, img_cols, 1)

# COMMAND ----------

# MAGIC %md But we aren't done with our data prep.  One of the standard techniques we apply when building a CNN is to convert the pixels to floating point values and scale them between 0 and 1.  Why do we do this?
# MAGIC 
# MAGIC Well, the answer is complicated but remember that our filters are multiplying pixel values. If we normalize our pixel values between 0 and 1, the multiplication keeps the output values between that same range.  By limiting the range of our output values, the convergence process, *i.e.* the movement of the model to a minimal error value through gradient descent, operates much faster:

# COMMAND ----------

X_train = X_train.astype('float32')
X_test = X_test.astype('float32')

X_train /= 255
X_test /= 255

# COMMAND ----------

# MAGIC %md Now our feature data, i.e. pixels, are ready to go.  But what about our labels?  Our labels are organized as a series. The range of values in the series are integers 0 through 9, corresponding to the digit the image represents.  These values, while numerical, actually represent classes that we wish to predict and as such we need to identify them as such.  We do this through the to_categorical method found in Kera's util library.  If you examine the output of this method, you should notice that the values now appear to be one-hot encoded (though we are preserving all fields):

# COMMAND ----------

y_train_cat = keras.utils.to_categorical(y_train, 10)
y_test_cat = keras.utils.to_categorical(y_test, 10)

# COMMAND ----------

# MAGIC %md The to_categorical method requires that our labels be integer values.  While not applicable in this example, we can explicitly convert string labels to integer values so that the predictions we make later can be mapped back to concepts such as "cat" or "dog" with relative ease of interpretation:

# COMMAND ----------

pd.DataFrame(y_test_cat).head()

# COMMAND ----------

# MAGIC %md With our data now prepared, let's begin to assemble our CNN. We start by instantiating the model as a sequential set of layers:

# COMMAND ----------

model = Sequential()

# COMMAND ----------

# MAGIC %md The initial layer is our input layer, but this is a given.  So we will start by defining our hidden layers.  We'll start with a convolutional layer that employs thirty-two 3x3 filters.  Why 32 filters and why a 3x3 filter(kernel) size?  Honestly, in the CNN space, we spend a lot of time studying what worked well for others and then following their patterns.  A small filter size of 3x3 makes sense given we have some relatively small images to work with.  But 32 filters?  That's just because that seems to be a popular starting point with MNIST.  If you'd like to run your model with fewer or more initial filters, give it a shot and see not only how the model performs but also how long it takes to achieve a result.  Experimentation is the key here.  
# MAGIC 
# MAGIC The activation function on this layer will be the rectified linear unit (ReLU) function that has become the most popular activation function for use with CNNs in the last few years. Like most activiation functions, this will produce a value between 0 and 1.  All negative values coming into the function are activated as a 0 while positive values receive a linearly increasing value from 0 to 1. For our purposes, it is fast to compute, leads to fast convergence, and gives consistently good results in CNNs: 

# COMMAND ----------

# Convolution Layer
model.add(
  Conv2D(32, 
         kernel_size=(3, 3),
         activation='relu',
         input_shape=input_shape)
         ) 

# COMMAND ----------

# MAGIC %md Next, let's add another convolational layer, this time with sixty-four 3x3 filters.  Notice the number of filters has increased from the thirty-two employed with the previous layer.  The current trend is to increase the number of filters as you add convolutional layers in a CNN, usually by doubling:

# COMMAND ----------

model.add(
  Conv2D(
    64, 
    (3, 3), 
    activation='relu'
    )
  )

# COMMAND ----------

# MAGIC %md Having one convolutional layer right after the other allows us to identify increasingly more complex features in our model.  A common pattern these days is to have two convolutional layers at the top of the CNN followed by a pooling layer that calculates a non-overlapping maximum:

# COMMAND ----------

# Pooling with stride (2, 2)
model.add(
  MaxPooling2D(
    pool_size=(2, 2)
    )
  )

# COMMAND ----------

# MAGIC %md To avoid overfitting, let's randomly drop out 25% of our neurons as we move from the pooling layer to our next layer. A dropout layer forces some number of neurons to produce a result of 0 on a given training pass.  This effectively forces the model to ignore the neurons selected for drop-out on that pass and allows the model to better generalize as opposed to getting stuck on a few specialized filters:

# COMMAND ----------

model.add(Dropout(0.25))

# COMMAND ----------

# MAGIC %md Now we add a flattening layer.  A flattening layer takes the 2-dimensional array that makes up our image and flattens it into a 1-dimensional structure.  This is required before the model is able to perform it's classification work. To help this make sense, think back to the first lab in this class when each pixel was used to form a feature in a row of data.  That's an example of a flattened image:

# COMMAND ----------

model.add(Flatten())

# COMMAND ----------

# MAGIC %md We now push our flattened image into a dense layer that produce 128 outputs.  Unlike the convolutional layers, the dense layer is just a regular neural network layer, no fancy convolutions or pooling here.  This works off our flattened dataset.
# MAGIC 
# MAGIC By why 128 neurons?  Again, we are following a pattern of increasing the outputs with each layer.  We started with 32 outputs (generated by 32 filters) then progressed to 64 outputs (generated by 64 filters), and now we've moved up to 128 outputs, a doubling with each layer.
# MAGIC 
# MAGIC Again to prevent overfitting, we drop-out some of our neurons during the training cycles:

# COMMAND ----------

model.add(Dense(128, activation='relu'))
model.add(Dropout(0.5))

# COMMAND ----------

# MAGIC %md We now need to take the combined input of the previous layer and predict to which of the 10 classes an incoming image should be assigned.  In order to make it so that the probabilities associated with each class total to 1, we will use the softmax activitation.  The class with which the output has the highest value is the one we will take as the assigned class, but this will allow us to also see the strength of that assignment relative to other classes:

# COMMAND ----------

# Apply Softmax for the classes 0 through 9 (10 classes total)
model.add(Dense(10, activation='softmax'))

# COMMAND ----------

# MAGIC %md With our CNN architected, let's now compile it in preparation for training. As we do this, we can specify options for our loss function and how loss is optimized as the model is trained.  Stochastic Gradient Descent (sgd) is one popular optimizer but we will use Adadelta which is a slightly more robust while still efficient gradient descent function:

# COMMAND ----------

# Loss function (crossentropy) and Optimizer (Adadelta)
model.compile(
  loss=keras.losses.categorical_crossentropy,
  optimizer=keras.optimizers.Adadelta(),
  metrics=['accuracy']
  )

# COMMAND ----------

# MAGIC %md Now that our model is compiled, we can train it.  We will do this 128 images at a time, making 8 passes over the complete set of images.  These settings give us good results while keeping processing times manageable.  Watch the output and how accuracy slowly improves as learning progresses:
# MAGIC 
# MAGIC **NOTE** This next step may take up to 20 minutes to complete.

# COMMAND ----------

model.fit(
      X_train,
      y_train_cat,
      batch_size=128,
      epochs=8,
      verbose=1,
      validation_data=(X_test, y_test_cat)
      )

# COMMAND ----------

# MAGIC %md Let's now see how our model performs:

# COMMAND ----------

# Evaluate our model
score = model.evaluate(X_test, y_test_cat, verbose=0)
print('Test loss:', score[0])
print('Test accuracy:', score[1])

# COMMAND ----------

# MAGIC %md And let's make a few predictions using it. First, let's take a look at what a prediction actually looks like:

# COMMAND ----------

model.predict(X_test[0].reshape(1,28,28,1))

# COMMAND ----------

# MAGIC %md As you can see, a probability for each category is provided.  We must chose the category with the highest probablity to be the one that's selected:

# COMMAND ----------

plt.figure(figsize=(25,5))

for i, y in enumerate(y_test_cat[0:5]):
  
  yhat = model.predict(X_test[i].reshape(1,28,28,1))
    
  s = plt.subplot(1,5,i+1)
  s.set_title('digit = {0} (prob = {1:.2%})'.format(np.where(yhat == np.amax(yhat))[1][0], np.amax(yhat)))
  plt.imshow(X_test[i][:,:,0], cmap='gray') 
  
display()