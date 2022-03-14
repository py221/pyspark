# Databricks notebook source
# MAGIC %md 
# MAGIC ###RDDs: Creating RDDs
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to teach you how to create an RDD using a variety of techniques.

# COMMAND ----------

# MAGIC %md You can think of an RDD (*Resilient Distribted Dataset*) as a list object.  Unlike a standard Python list, the elements in the RDD *"list"* are distributed across the workers that make up the Spark cluster. To see this in action, let's start by defining a range of integer values which we will later convert into a Spark RDD:

# COMMAND ----------

import numpy as np 

# COMMAND ----------

values = sc.range(0, 100000)

# COMMAND ----------

# MAGIC %md In the code above, we've created an RDD called values.  Notice the **sc** object (discussed in the last lab) is used to define the RDD.  As an RDD, the integer values from 0 to 99,999 are automatically distributed across the worker nodes in the cluster as subsets of values we refer to as partitions.  To see the number of partitions used by this RDD, use the it's **getNumPartitions()** method:

# COMMAND ----------

values.getNumPartitions()

# COMMAND ----------

# MAGIC %md At this point in our learning, it's not terribly important to manage the number of partitions employed by an RDD.  That said, when you run against complex scaling and performance challenges, adjusting the number of partitions and how data are distributed between them is a key practice.
# MAGIC 
# MAGIC Defining an RDD from a range is a commmon technique for creating an RDD on which we might wish to test some logic.  Another common way of creating an RDD is to define a standard Python list and then have the **sc** object convert it using its **parallelize()** method:

# COMMAND ----------

my_list = ['apples', 'grapes', 1, 2, 3.0, 'bannanas']

my_rdd = sc.parallelize( my_list )

my_rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md Two things to notice in the last code sample.  First, the number of partitions defined for an RDD does not necessarily align with the number of elements in it.  Each method for creating an RDD has a default set of rules for defining the number of partitions in the default RDD.  As a result, we can sometimes end up with too many or too few partitions for our actual needs.  This is a side-effect of Spark trying to retain as little information as is possible about the data in an RDD. And this brings us to the second thing to note about the last code sample .... 
# MAGIC 
# MAGIC Notice that the list contains elements of differing data types.  This is fine with a Python list as the list places no restrictions on the elements it contains.  The Spark RDD works the same way.  To verify this, let's inspect the data type used for each element in the RDD:

# COMMAND ----------

my_rdd.map( lambda element: type(element) ).collect()

# COMMAND ----------

# MAGIC %md **NOTE**: We haven't addressed the **map()** method just yet.  Think of it as behaving just like the Python **map()** function addressed in our last set of labs.  Also, the **collect()** method is being used to retreive all the data from the RDD back to the driver for printing to the screen.  We'll discuss both these methods in our next lab.

# COMMAND ----------

# MAGIC %md Let's get a little crazier with the struture of our list and see what the RDD reports back as the types in use:

# COMMAND ----------

my_list = ['apples', ('grapes', 1.23), 1, 2, 3.0, {'fruit':'bannanas'}]
my_rdd = sc.parallelize( my_list )
my_rdd.map( lambda element: type(element) ).collect()

# COMMAND ----------

# MAGIC %md Again, the RDD houses a loose collection of elements that are distributed across partitions which are themselves distributed to the workers in our cluster.  The RDD doesn't place any limitations on the types of data in use and that's a critical thing to understand about RDDs.  In later labs, we will work with higher-level constructs, *e.g.* dataframes, which attempt to define fairly consistent structures for our data.  In the real world, we often encounter some very messy datasets that are not easily mapped into well-defined structures.  In those scenarios, RDDs can be a great way to take a first crack at the data, allowing us to extract useful information from the chaos in the original dataset.

# COMMAND ----------

# MAGIC %md Crafting RDDs from ranges and in-code lists (as we have been doing so far in this lab) occassionally pops up in our coding but the vast majority of RDDs we will work with are created from data in files.  Several methods are available for reading file-based data into an RDD, but the **sc** object's **textFile()** method is the most commonly used by far.  This method reads a row-delimited text file into an RDD with each line of text in the file becoming a string element.  The **textFile()** method can be pointed to a specific file or to a directory of files.  When a directory is used, all files in that directory are read to form the RDD.

# COMMAND ----------

ufo_sightings = sc.textFile( 'wasbs://downloads@smithbc.blob.core.windows.net/ufo/ufo_awesome.tsv' )

print( ufo_sightings.count() )

# COMMAND ----------

ufo_sightings.getNumPartitions()

# COMMAND ----------

# MAGIC %md Using the **count()** method on the RDD, we can see that the file being read consists of 61,393 lines of text.  To see the first of these, we can use the **take()** method with the appropriate numerical value:

# COMMAND ----------

ufo_sightings.take(1)

# COMMAND ----------

# MAGIC %md Notice that the line is not parsed by Spark.  The first element in the RDD is simply a string representing the whole line as read from the targetted text file.  It will be on us to interpret the structure of the lines that have been read into the RDD.  This will be demonstrated in the next lab. 

# COMMAND ----------

# MAGIC %md And what about the partitioning of these 61,393 elements now housed in our RDD?

# COMMAND ----------

ufo_sightings.getNumPartitions()

# COMMAND ----------

# MAGIC %md Again, each method for creating an RDD has its own rules for defining the number of partitions to use.  With the **range()** and **parallelize()** methods, the number of workers in the cluster, exposed through the **sc.defaultParallelism** property, is typically used to define the number partitions in the RDD.  With the **textFile()** method, the **sc.defaultMinPartitions** property controls the number of partitions created.  Using the **minPartitions** parameter with the **textFile()** method, we can override this behavior to better distribute our data:  

# COMMAND ----------

ufo_sightings = sc.textFile( 
  'wasbs://downloads@smithbc.blob.core.windows.net/ufo/ufo_awesome.tsv',
  minPartitions = sc.defaultParallelism
  )

ufo_sightings.getNumPartitions()

# COMMAND ----------

# MAGIC %md Is this the right number or better number of partitions to use for the RDD created for this text file?  The answer to that is far more complicated than I wish to cover here, but the main point I need you to understand is that the RDD distributes the data across the workers and this is something that from time to time we will need to manage in order to achieve our scale and performance objectives.

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC With as few lines of code as is possible, create an RDD consisting of values 0 through 1 million (inclusive of 1 million) that are evenly divisible by 10. Provide a count of the records in the RDD. (Answer: The count of records should be 100,001. Hint: Read the documentation on the method call used to create the RDD from a range to see some additional parameters that we've not addressed in this lab.)

# COMMAND ----------

# your scenario 1 code here
values = sc.range(0, 1000001, 20, sc.defaultParallelism)

values.count()

# COMMAND ----------

# MAGIC %md ####Scenario 2
# MAGIC 
# MAGIC The **wasbs://downloads@smithbc.blob.core.windows.net/nyse/** directory contains a series of text files, each line of which provides daily summary statistics for a given stock on the New York Stock Exchange for given day between 1962 and some date in 2010. Read the contents of this directory into an RDD and provide a count of the records in the RDD. (Answer: The count of records should be 9,211,067.)

# COMMAND ----------

# your scenario 2 code here

NYSE = sc.textFile('wasbs://downloads@smithbc.blob.core.windows.net/nyse/')
NYSE.count()

# COMMAND ----------

NYSE.take(5)

# COMMAND ----------

# MAGIC %fs ls 'wasbs://downloads@smithbc.blob.core.windows.net/nyse/'

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

# Scenario 1
nums = sc.range(0, 1000001, step=10)
nums.count()

# COMMAND ----------

# Scenario 2
nyse = sc.textFile('wasbs://downloads@smithbc.blob.core.windows.net/nyse/')
nyse.count()