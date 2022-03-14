# Databricks notebook source
# MAGIC %md 
# MAGIC ###RDDs: The Spark Context
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to teach you about the Spark Context object.

# COMMAND ----------

# MAGIC %md When you run an application on a Spark cluster, the application itself runs on the driver just like it would on a stand-alone machine. It's only when we send instructions to the Spark software, through the Spark Context object, that we enable the driver to distribute work across the worker nodes of the cluster.
# MAGIC 
# MAGIC By default in most Spark environments, the Spark Context object is presented as a predefined object named **sc**.  Let's take a look at some of the information we can learn about our Spark environment through the **sc** object:

# COMMAND ----------

# spark version in use
print( sc.version )

# COMMAND ----------

# python version in use
print( sc.pythonVer )

# COMMAND ----------

# number of workers in the cluster
print( sc.defaultParallelism )

# COMMAND ----------

# MAGIC %md In addition to learning information about the Spark environment, we can use the **sc** object to configure the environment as well:

# COMMAND ----------

# appName is the application name used when logging information in the Spark logs
print( sc.appName, '\n')

# assign new value to app name
sc.appName = 'Bryan\'s Test'

# view app name value that is now included in Spark logs
print( sc.appName )

# COMMAND ----------

# MAGIC %md But the way we typically use the **sc** object is to create RDDs.  This topic will be discussed more in the next lab, but for now, know that the **sc** object presents a wide range of methods which we may employ in our applications.  To see a listing of these, use the **dir()** method as you would in any Python environment.  To locate information about what each method means, you can use the online help or navigate to the PySpark API help at https://spark.apache.org/docs/latest/api/python/pyspark.html and navigate to the documentation under the Spark Context object.

# COMMAND ----------

dir( sc )

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC In class, you learned that an RDD divides its elements into partitions which are distributed to the workers in the cluster.  Using the sc object, how might you determine the default number of partitions used with RDDs in a given cluster? (Hint: You should inspect the methods associated with the sc object and also gather information from its online documentation)

# COMMAND ----------

# your scenario 1 code here
sc.defaultMinPartitions

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

print( sc.defaultMinPartitions )

# from the online documentation: "Default min number of partitions for Hadoop RDDs when not given by user"
# @ https://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.SparkContext.defaultMinPartitions