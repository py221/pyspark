# Databricks notebook source
# MAGIC %md # TO RUN THIS NOTEBOOK, YOU MUST USE A DATABRICKS 6.4 ML CLUSTER

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Images: Horovod
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to introduce you to the Horovod framework for the parallelization of CNN training.

# COMMAND ----------

# MAGIC %md CNNs are very powerful but also very computationally expensive.  The last CNN you built was trained on a single node computer.  In this lab, I will demonstrate how to modify the training code in order to parallelize the work using a framework known as Horovod.
# MAGIC 
# MAGIC First, let's be clear that Horovod depends on some specialized resources that are not available in the Community Edition of Databricks.  Instead, we must make use of a specialized build for Machine Learning.  This is only available to paid subscribers of Databricks and therefore this lab will serve as more of a demonstration than a true hands-on lab.
# MAGIC 
# MAGIC Next, Horovod leverages Keras and Tensorflow.  Keras and Tensorflow have a weird relationship.  While Keras sits on top of Tensorflow, Tensorflow has implemented its own version of Keras within the Tensorflow framework.  Horovod currently works better with the Tensorflow version of Keras so we need to switch up some libraries.  This is really no big deal except that Tensorflow 2.0 was released last year and it kinda breaks a bunch of stuff.  We'll use Tensorflow 2.x but we'll make our Keras references to the v1 backwards-compatible sublibraries.  Confusing?  Welcome to the world of open source :-)

# COMMAND ----------

if not ('-ml' in spark.conf.get('spark.databricks.clusterUsageTags.sparkVersion')):
  raise Exception(
    'Invalid build of the Databricks runtime.\nYou must use an ML runtime (version 6.4 or higher).'
    )
  
import tensorflow as tf
import tensorflow.compat.v1.keras as keras

from keras.models import Sequential
from keras.layers import Dense, Dropout, Flatten
from keras.layers import Conv2D, MaxPooling2D
from keras.optimizers import Adadelta

import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md As in the last lab, we will tap into a set of the MNIST images that comes pre-installed with Keras.  Notice this time, however, that we are acquiring the dataset using a function call.  Horovod will attempt to distribute the work involved with training our model but sending different subsets of the data to different executors in our cluster.  Unlike with Spark, we must provide a way for Horovod to determine which subsets of data should go to which nodes.  The rank parameter on this function will receive an integer value which identifies the node.  The default values identified with these parameters allow the function to behave properly in a standard (non-Horovod) run:
# MAGIC 
# MAGIC Notice too that we are using long form notation to identify objects that should be accessible with shorter syntax given our import statements.  This is because Horovod pickle's our function definitions.  We can either use long-form or re-import the required libraries from within our function definitions.  It's an odd quirk of Horovod:

# COMMAND ----------

def get_dataset(num_classes, rank=0, size=1):
  
  img_rows, img_cols = 28, 28
  
  (X_train, y_train), (X_test, y_test) = tf.compat.v1.keras.datasets.mnist.load_data('MNIST-data-%d' % rank)
  
  X_train = X_train[rank::size]
  y_train = y_train[rank::size]
  
  X_test = X_test[rank::size]
  y_test = y_test[rank::size]
  
  X_train = X_train.reshape(X_train.shape[0], img_rows, img_cols, 1)
  X_test = X_test.reshape(X_test.shape[0], img_rows, img_cols, 1)
  
  X_train = X_train.astype('float32')
  X_test = X_test.astype('float32')
  
  X_train /= 255
  X_test /= 255
  
  y_train = tf.compat.v1.keras.utils.to_categorical(y_train, num_classes)
  y_test = tf.compat.v1.keras.utils.to_categorical(y_test, num_classes)
  
  return (X_train, y_train), (X_test, y_test)

# COMMAND ----------

# MAGIC %md Now we will build our model.  As with our data acquisition steps, we will encapsulate this work in a function definition.  Notice that we are omitting the compile step.  More on that in a bit:

# COMMAND ----------

def get_model(num_classes):
  model =Sequential()
  model.add(Conv2D(32, kernel_size=(3, 3),
                   activation='relu',
                   input_shape=(28, 28, 1)))
  model.add(Conv2D(64, (3, 3), activation='relu'))
  model.add(MaxPooling2D(pool_size=(2, 2)))
  model.add(Dropout(0.25))
  model.add(Flatten())
  model.add(Dense(128, activation='relu'))
  model.add(Dropout(0.5))
  model.add(Dense(num_classes, activation='softmax'))
  
  return model

# COMMAND ----------

# MAGIC %md Now we define a function for model training.  Notice this one function calls the get_dataset and get_model functions defined earlier.  When we send this to Horovod, it will *pickle* the combined logic across all three functions into a single unit of executable code.  (*Pickle* is Python-ese for serializing aka writing object definitions into a format that can easily be sent to different locations or stored between calls.)

# COMMAND ----------

def train(learning_rate=1.0):
  
  (X_train, y_train), (X_test, y_test) = get_dataset(num_classes)
  
  model = get_model(num_classes)
  optimizer = Adadelta(lr=learning_rate)

  model.compile(optimizer=optimizer,
                loss='categorical_crossentropy',
                metrics=['accuracy'])

  model.fit(X_train, y_train,
            batch_size=batch_size,
            epochs=epochs,
            verbose=2,
            validation_data=(X_test, y_test))

# COMMAND ----------

# MAGIC %md Let's now test our logic to make sure it works correctly.  At this point, we are not using Horovod though everything is organized for its use:
# MAGIC 
# MAGIC **NOTE** This may take up to **an hour or more** to complete.

# COMMAND ----------

batch_size = 128
epochs = 10
num_classes = 10

train(learning_rate=0.1)

# COMMAND ----------

# MAGIC %md Now that we have a working model, let's enable it for use in Horovod.  First, we'll add Horovod initialization logic to our training function. We'll then modify the get_data function call to pass in the number of Executors Horovod will be allowed to employ and the size of the batch.  The optimizer will also be modified to use a learn_rate that is proportional to the size and the optimizer will be wrapped in a Horovod distributed optimizer. This distributed optimizer is what allows the exchange of output information required for back-propogation in a distributed environment. We will then need to configure callbacks to enable a consistent initialization.  Checkpoints will also need to be written to the callbacks so that should a process fail, another Executor could pick it up without too much loss of progress:

# COMMAND ----------

import horovod.tensorflow.keras as hvd
from keras import backend as K

def train(learning_rate=1.0):
    
  hvd.init()
  
  config = tf.ConfigProto()
  config.gpu_options.allow_growth = True
  config.gpu_options.visible_device_list = str(hvd.local_rank())
  K.set_session(tf.Session(config=config))
    
  (X_train, y_train), (X_test, y_test) = get_dataset(num_classes, hvd.rank(), hvd.size())
  
  model = get_model(num_classes)

  optimizer = Adadelta(lr=learning_rate * hvd.size())
  optimizer = hvd.DistributedOptimizer(optimizer)
    
  model.compile(optimizer=optimizer,
                loss='categorical_crossentropy',
                metrics=['accuracy'])

  model.fit(X_train, y_train,
            batch_size=batch_size,
            epochs=epochs,
            verbose=2,
            validation_data=(X_test, y_test)
           )

# COMMAND ----------

# MAGIC %md Now we can execute our training fun through Horovod.  In Spark, this is done through a management component known as the HorovodRunner.  Notice that the HorovodRunner is passed the name of function which trains the model.  HorovodRunner will *pickle* this function to then distribute it across the workers in the clsuter and then call each in a coordinated manner:

# COMMAND ----------

batch_size = 128
epochs = 10
num_classes = 10
 
from sparkdl import HorovodRunner
 
hr = HorovodRunner(np=0)
hr.run(train, learning_rate=0.1)

# COMMAND ----------

# MAGIC %md While the timings don't show it, the Horovod runner solution has the potential to be much faster than the standalone Keras training solution.  As you add more and more workers to the clsuter, the timing of this last cell should decline.  (I personally do not have access to enough resources to demonstrate this as GPU-enabled systems are expensive.) 
# MAGIC 
# MAGIC That said, there is considerable overhead in distributing the code and the data around the clusters and then coordinating the on-going work.  For small clusters with small datasets, this overhead can make the Horovod solution run slower than a stand-alone Keras deployment. 