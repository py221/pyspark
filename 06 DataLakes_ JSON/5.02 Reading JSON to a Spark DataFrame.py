# Databricks notebook source
# MAGIC %md 
# MAGIC ###JSON: Reading JSON to a Spark Dataframe
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to explore how JSON is read into a Spark dataframe.

# COMMAND ----------

# MAGIC %md The easiest way to read a data file with valid JSON documents to a Spark data frame is to use the SparkSQL [JSON reader](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.json) and simply have Spark infer the document's schema:

# COMMAND ----------

profiles_inferred = spark.read.json('/tmp/json/profiles/profiles.json', mode='permissive')
#profiles_inferred.show(1)

display(profiles_inferred)

# COMMAND ----------

# MAGIC %md In the sample above, we allowed Spark to infer the schema of our JSON documents. Notice that we specified a mode of *permissive* to tell the JSON reader that values should be set to NULL should we encounter invalid records.  This *permissive* mode is the default so that you will rarely see the mode specified like this. However, you should be aware that while JSON is a very simple format based on a standard, many organizations still manage to introduce non-standard syntax into their documents.  If you wish to be more strict with how non-standard syntax is handled, consider using FAILFAST or DROPMALFORMED options for this setting.
# MAGIC 
# MAGIC By not supplying a schema, we allowed Spark to infer the schema of our JSON documents. It does this by using key names as field names. From JSON document to JSON document, there is no guarantee the fields will be all supplied.  In fact, this is one of the benefits of JSON; we can specify slightly different structures for each document.  Spark attempts to handle this by reading through the JSON contents once, capturing all the different keys within the structure and attempting to reconcile any differences between the documents to define a schema that will accomodate all the JSON documents it has read. (You can use the *samplingRatio* parameter with the JSON reader to limit the number of documents read during schema inferrence.)
# MAGIC 
# MAGIC For the customer profile data that we are working with, this is what it came up with:

# COMMAND ----------

profiles_inferred.printSchema()

# COMMAND ----------

# MAGIC %md As with tabular data sets, you can specify the structure using Spark SQL types.  Notice that the nested structures use the StructType type.  This is the same type you use when you specify the schema itself.  With this in mind, you might think of the nested structures as simply schemas within schemas.
# MAGIC 
# MAGIC Notice too that field names do not need to be in the order as they may appear in the JSON text documents themselves.  Just like a Python dictionary does not maintain the ordering of key-value pairs, JSON can be flexible in its ordering as well. All that matters is that field names align with those found in the documents.
# MAGIC 
# MAGIC Finally, if you provide a schema for a JSON document, it does not need to include information on every key it might encounter.  If you wish to omit a key, simply don't provide a structure for it in the schema.  The JSON reader will simply ignore any information in the documents that doesn't align with the schema you've specified:

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([
    StructField('id', StringType()),
    StructField('firstpurchase', StringType()),
    StructField('name', StructType([
        StructField('first', StringType()),
        StructField('middle', StringType()),
        StructField('last', StringType())
        ])),
     StructField('contact', StructType([
         StructField('email', StringType()),
         StructField('phone', StringType())
         ])),
     StructField('address', StructType([
         StructField('city', StringType()),
         StructField('country', StringType()),
         StructField('line1', StringType()),
         StructField('line2', StringType()),
         StructField('stateprovince', StringType())
         ])),
     StructField('demographics', StructType([
         StructField('age', StringType()),
         StructField('commutedistance', StringType()),
         StructField('education', StringType()),
         StructField('gender', StringType()),
         StructField('houseownerflag', StringType()),
         StructField('maritalstatus', StringType()),
         StructField('numbercarsowned', StringType()),
         StructField('numberchildrenathome', StringType()),
         StructField('occupation', StringType()),
         StructField('region', StringType()),
         StructField('totalchildren', StringType()),
         StructField('yearlyincome', StringType())
         ])),
     StructField('modified_dt', StringType())
    ])

profiles_not_inferred = spark.read.json('/tmp/json/profiles/profiles.json', schema=schema)
#profiles_not_inferred.show(1)

display(profiles_not_inferred)

# COMMAND ----------

# MAGIC %md So, which is a better approach: provide a schema or have Spark infer it?  If you remember back to our labs on schema inference with delimited data sets, we said that we should almost always provide the schema.  That's because tabular documents tend to be very rigid in their schema definitions.  But one of the key uses of JSON is to store data in specialized "databases" called document stores within which the schema of the documents can rapidly and inconsistently evolve.  This is a very important approach to data modeling in many rapidly evolving business applications.  In these scenarios, the JSON you are working with is likely to be highly variable in its structure.  If you provide a schema, you may miss some data.  If you infer the schema, you take a performance hit. 
# MAGIC 
# MAGIC The right answer to the question posed is that *it depends*.  Many Spark developers elect to use the **samplingRatio** argument with the JSON reader to read some percentage of JSON documents for schema inference, taking a slightly lower peformance hit while retaining some flexibility.  The solution isn't perfect so you really need to know your data source to determine the right path for your situation. 

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC A [research paper review dataset](https://archive.ics.uci.edu/ml/machine-learning-databases/00410/) has been downloaded and placed in storage at wasbs://downloads@smithbc.blob.core.windows.net/reviews/reviews.json. **THIS DATA FILE DIFFERS FROM THE ONE USED IN THE LAB** in that it contains a single JSON document formatted to span multiple lines.  You can see this by executing the following cell.

# COMMAND ----------

dbutils.fs.head('/tmp/json/reviews/reviews.json')

# COMMAND ----------

# MAGIC %md To deal with multi-line JSON such as this, you will need to use the **multiLine** parameter set to **True** with the JSON reader. This will force the reader to interpret the JSON across CarriageReturns and LineFeeds, \r & \n, respectively.
# MAGIC 
# MAGIC Use the SparkSQL JSON reader to read this file into a DataFrame.

# COMMAND ----------

 # your scenario 1 code here

# COMMAND ----------

# MAGIC %md ####Scenario 2
# MAGIC 
# MAGIC NASA publishes a list of meteorite landings at https://data.nasa.gov/resource/y77d-th95.json. Using urllib, download this file to an RDD with each JSON document serving as an item in the RDD. (Code for this has been provided to you.) Use the RDD in place of a path with the JSON reader to read the sightings into an RDD.  You do not need to provide a schema for this.

# COMMAND ----------

from urllib import request as url 

# retrieve json data from website
request = url.Request('https://data.nasa.gov/resource/y77d-th95.json')
response = url.urlopen(request)

# convert to rdd (making sure to convert bytes to UTF-8 string)
json_rdd = sc.parallelize( [response.read().decode('utf-8').strip()] )

# your scenario 2 code here

# COMMAND ----------

# MAGIC   %md ####Answers (scroll down)
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC <br></p>

# COMMAND ----------

# scenario 1
reviews_path = '/tmp/json/reviews/reviews.json'

df = spark.read.json(reviews_path, multiLine=True)
#df.printSchema()

display(df)

# COMMAND ----------

# Scenario 2

from urllib import request as url 

# retrieve json data from website
request = url.Request('https://data.nasa.gov/resource/y77d-th95.json')
response = url.urlopen(request)

# convert to rdd (making sure to convert bytes to UTF-8 string)
json_rdd = sc.parallelize( [response.read().decode('utf-8').strip()] )

# your scenario 2 code here

# convert rdd to dataframe
meteors = spark.read.json( json_rdd )
display(meteors)

# COMMAND ----------

