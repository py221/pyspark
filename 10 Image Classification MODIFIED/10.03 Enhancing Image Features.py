# Databricks notebook source
# MAGIC %md 
# MAGIC ###Images: Enhancing Image Features
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to introduce you to the concepts of convolution and pooling for image feature enhancement.

# COMMAND ----------

# MAGIC %md In the first lab, we constructed a classifier using the raw pixels of an image as the input features. This works reasonably well when our images represent fairly simple things that are fairly well differentiated from one another.  But as the complexity of images increases, the raw pixels may be too noisy for us to determine any useful patterns directly from them. In these situations, we may wish to enhance the image, applying filters to highlight specific shapes and patterns in the image.  This is tackled using a technique called convolution.
# MAGIC 
# MAGIC To assist us in exploring convolutions, let's load a popular library called Keras.  Keras provides a higher-level framework for building Deep Neural Networks (DNNs) on top of TensorFlow and other DNN libraries.  As image classification and object detection are popular applications for DNNs, Keras comes pre-built with quite a bit of functionality that will simplify our examination of convolutions:

# COMMAND ----------

dbutils.library.installPyPI('keras')
dbutils.library.installPyPI('tensorflow')
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md Now, let's load a simple image representing a digit. We will use the matplotlib.image library for this just to keep things simple:

# COMMAND ----------

# MAGIC %sh 
# MAGIC rm -R ./images 
# MAGIC mkdir images
# MAGIC cd images
# MAGIC wget https://smithbc.blob.core.windows.net/downloads/mnist/train/2/10009.png -O digit.png>/dev/null 2>&1
# MAGIC cd ..
# MAGIC ls -l -R ./images 

# COMMAND ----------

import matplotlib.image as mpimg
import matplotlib.pyplot as plt
import numpy as np

image_np = mpimg.imread('./images/digit.png')

plt.imshow(image_np, cmap='gray')
plt.show()

display()

# COMMAND ----------

# MAGIC %md Let's remember that the images that form an image are really nothing more than an array.  We can visualize the array for the image above as follows:

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

print_image_array(image_np)

# COMMAND ----------

# MAGIC %md Within the image, we might notice features such as horizontal lines, vertical lines, curves, *etc.* and we might wish to *extract* these from the image.  This is done by applying a filter to an image.  
# MAGIC 
# MAGIC A filter is itself another image, *i.e.* an array.  The filter image is significantly smaller than the image to which it is to be applied.  Pixels in the image are set to a maximum value to denote pixels that correspond to features we wish to detect and a minimum value to denote pixels we wish to ignore. For example, here are several 5 x 5 filter images that correspond with a horizontal line, a vertical line and a diagonal line:  

# COMMAND ----------

filter_vline = np.array([
  [  0.,   0., 255.,   0.,   0.],
  [  0.,   0., 255.,   0.,   0.],
  [  0.,   0., 255.,   0.,   0.],
  [  0.,   0., 255.,   0.,   0.],
  [  0.,   0., 255.,   0.,   0.]
  ])

filter_hline = np.array([
  [  0.,   0.,   0.,   0.,   0.],
  [  0.,   0.,   0.,   0.,   0.],
  [255., 255., 255., 255., 255.],
  [  0.,   0.,   0.,   0.,   0.],
  [  0.,   0.,   0.,   0.,   0.]
  ])

filter_diag = np.array([
  [  0.,   0.,   0.,   0., 255.],
  [  0.,   0.,   0., 255.,   0.],
  [  0.,   0., 255.,   0.,   0.],
  [  0., 255.,   0.,   0.,   0.],
  [255.,   0.,   0.,   0.,   0.]
  ])

plt.figure(figsize=(15,5))

s = plt.subplot(1,5,1)
s.set_title('vertical line')
plt.imshow(filter_vline, cmap='gray')

s = plt.subplot(1,5,2)
s.set_title('horizontal line')
plt.imshow(filter_hline, cmap='gray')

s = plt.subplot(1,5,3)
s.set_title('diagonal line')
plt.imshow(filter_diag, cmap='gray')

display()

# COMMAND ----------

# clear plot settings
plt.clf()

# COMMAND ----------

# MAGIC %md If we start at the upper left-hand corner of our source image and apply our filter, we can multiply the value for each pixel in the source image by it's intersecting pixel from the filter.  Most pixels will intersect with a 0 valued pixel in the filter so that multiplication will result in a zero.  But those pixels intersecting with the feature identified in the filter, *i.e.* the horizontal bar, vertical bar or diagonal line, will produce a non-zero product.  The higher the value of the source pixel, the larger the product produced.
# MAGIC 
# MAGIC With per-pixel multiplication applied, we can now sum up all the products from across the filter.  This value is then assigned to a single pixel in a new image.  We can then move one pixel to the right, perform our calculations again, and produce another pixel for our new image.  This continues across the row and then on to the next row until the entire source image is processed. The resulting image is a little smaller than the original image but features that were the target of the filter now stand out.
# MAGIC 
# MAGIC The logic to perform this work, which is known as a convolution, is a bit complex to implement from scratch so we'll make use of Keras to apply our filters to our source image.  This is done by constructing a two-dimensional convolutional layer:

# COMMAND ----------

import keras

# HLINE
# instantiate a keras model
model = keras.models.Sequential()

# apply a 5x5 filter to a 28x28 source image
model.add(keras.layers.Conv2D(1, (5,5), input_shape=(28,28,1)))
model.set_weights( [filter_hline.reshape(5,5,1,1), np.asarray([0.0])])

# apply the filter to produce the output image
output_hline = model.predict(image_np.reshape(1,28,28,1))

# COMMAND ----------

# VLINE
# instantiate a keras model
model = keras.models.Sequential()

# apply a 5x5 filter to a 28x28 source image
model.add(keras.layers.Conv2D(1, (5,5), input_shape=(28,28,1)))
model.set_weights( [filter_vline.reshape(5,5,1,1), np.asarray([0.0])] )

# apply the filter to produce the output image
output_vline = model.predict(image_np.reshape(1,28,28,1))

# COMMAND ----------

# DIAG
# instantiate a keras model
model = keras.models.Sequential()

# apply a 5x5 filter to a 28x28 source image
model.add(keras.layers.Conv2D(1, (5,5), input_shape=(28,28,1)))
model.set_weights( [filter_diag.reshape(5,5,1,1), np.asarray([0.0])] )

# apply the filter to produce the output image
output_diag = model.predict(image_np.reshape(1,28,28,1))

# COMMAND ----------

plt.figure(figsize=(20,5))

s = plt.subplot(1,5,1)
s.set_title('original')
plt.imshow(image_np[2:-2, 2:-2], cmap='gray') # adjust scaling of original to align with filtered images

s = plt.subplot(1,5,2)
s.set_title('vertical line enhanced')
plt.imshow(output_vline[0,:,:,0], cmap='gray')

s = plt.subplot(1,5,3)
s.set_title('horizontal line enhanced')
plt.imshow(output_hline[0,:,:,0], cmap='gray')

s = plt.subplot(1,5,4)
s.set_title('diagonal line enhanced')
plt.imshow(output_diag[0,:,:,0], cmap='gray')

display()

# COMMAND ----------

# MAGIC %md The application of the filter makes the feature of interest, be it a horizonal line, a vertical line, a diagonal line, or any other shape or pattern, stick out in the resulting image. But there is still a lot of noise in the image that we could remove.  
# MAGIC 
# MAGIC One technique for doing this is to pass over the image, evaluating several pixels at a time in a non-overlapping manner.  Across the pixels that are being evaluated, the maximum or average pixel value is recorded.  This technique is called pooling and the resulting image is greatly reduced in size depending on the number of non-overlapping pixels that are being considered.  For example, pooling with a 4x4 pixel pooling frame, the number of pixels in the resulting image is reduced by half.  In this resulting, pooled image, enhanced features, such as those detected through convolution:
# MAGIC 
# MAGIC NOTE In Keras, pooling defaults to a 2x2 pool size.  We are using a 4x4 to get an exagerated effect specific to the MNIST images.

# COMMAND ----------

import keras

# HLINE
# instantiate a keras model
model = keras.models.Sequential()

# apply a 5x5 filter to a 28x28 source image
model.add(keras.layers.Conv2D(1, (5,5), input_shape=(28,28,1)))
model.add(keras.layers.MaxPooling2D(pool_size=(4,4)))
model.set_weights( [filter_hline.reshape(5,5,1,1), np.asarray([0.0])] )

# apply the filter to produce the output image
output_hline = model.predict(image_np.reshape(1,28,28,1))

# VLINE
# instantiate a keras model
model = keras.models.Sequential()

# apply a 5x5 filter to a 28x28 source image
model.add(keras.layers.Conv2D(1, (5,5), input_shape=(28,28,1)))
model.add(keras.layers.MaxPooling2D(pool_size=(4,4)))
model.set_weights( [filter_vline.reshape(5,5,1,1), np.asarray([0.0])] )

# apply the filter to produce the output image
output_vline = model.predict(image_np.reshape(1,28,28,1))

# DIAG
# instantiate a keras model
model = keras.models.Sequential()

# apply a 5x5 filter to a 28x28 source image
model.add(keras.layers.Conv2D(1, (5,5), input_shape=(28,28,1)))
model.add(keras.layers.MaxPooling2D(pool_size=(4,4)))
model.set_weights( [filter_diag.reshape(5,5,1,1), np.asarray([0.0])] )

# apply the filter to produce the output image
output_diag = model.predict(image_np.reshape(1,28,28,1))

# COMMAND ----------

plt.figure(figsize=(20,5))

s = plt.subplot(1,5,1)
s.set_title('original')
plt.imshow(image_np, cmap='gray')

s = plt.subplot(1,5,2)
s.set_title('v-line')
plt.imshow(output_vline[0,:,:,0], cmap='gray')

s = plt.subplot(1,5,3)
s.set_title('h-line')
plt.imshow(output_hline[0,:,:,0], cmap='gray')

s = plt.subplot(1,5,4)
s.set_title('diag')
plt.imshow(output_diag[0,:,:,0], cmap='gray')

display()

# COMMAND ----------

# MAGIC %md As convolution and pooling are applied to the images, key features begin to emerge and noise begins to fade into the background.  What you are left with is a smaller set of images reduced to their key features which are then better prepared to bring into a classification exercise.  How we decide which features to pursue is a topic we will address when we examine Convolutional Neural Networks in just a few labs. 