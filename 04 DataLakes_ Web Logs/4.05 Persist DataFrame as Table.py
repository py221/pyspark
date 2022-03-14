# Databricks notebook source
# MAGIC %md 
# MAGIC ###Data Lakes: Persist DataFrames as Tables
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to teach you how persist your dataframe data to a SparkSQL table.

# COMMAND ----------

# MAGIC %md Let's load our weblogs to an RDD, parse the data, and convert it into a dataframe, using the logic we've built up the last few labs:

# COMMAND ----------

import re
from datetime import datetime

def get_fields(line):
    pattern = re.compile('^(\S*) (\S*) (\S) (-|\[.*\]) ([^\s\"]|\"[^\"]*\") (-|\d*) (-|\d*)(?: ([^\s\"]|\"[^\"]*\") ([^\s\"]|\"[^\"]*\"))+')
    match = pattern.match(line)
    if match is not None:
        return match.groups()
        
def get_http_elements( http_request ):
    pattern = re.compile('\"(\S*) (\S*) (\S*)\"')
    match = pattern.match(http_request)
    if match is not None:
        return match.groups()

def parseDateTimeString( datetime_string ):
    return datetime.strptime( datetime_string, '[%d/%b/%Y:%H:%M:%S %z]')
  
# read lines from weblog      
lines = sc.textFile('/tmp/weblogs/old/access.log')

# parse lines into fields, removing bad lines
fields = lines.map( get_fields ).filter( lambda f: f is not None)

# parse the http request field
expanded = (
  fields
    .map(lambda f: (f, get_http_elements(f[4])))  # split http request
    .filter(lambda f: f[1] is not None) # remove lines with bad http requests
    .map(lambda f: f[0] + f[1]) # restructure the data
    .map(lambda f: (f , parseDateTimeString(f[3]))) # convert string to datetime object
    .map(lambda f: f[0] + tuple([f[1]])) # restructure the data
    )

cleansed = expanded.map(lambda f: f[0:5] + (int(f[5]), int(0 if (f[6]=='-') else f[6])) + f[7:] )

# COMMAND ----------

from pyspark.sql.types import *

my_schema = StructType([
    StructField('host', StringType()),
    StructField('identity', StringType()), 
    StructField('user', StringType()), 
    StructField('time_string', StringType()), 
    StructField('request', StringType()), 
    StructField('status', IntegerType()), 
    StructField('size', LongType()),
    StructField('referer', StringType()), 
    StructField('agent', StringType()), 
    StructField('verb', StringType()),
    StructField('uri', StringType()),
    StructField('protocol', StringType()),
    StructField('utc_time', TimestampType())
    ])

df = spark.createDataFrame( cleansed, schema=my_schema)

from pyspark.sql.functions import regexp_replace

final = df.withColumn(
        'referer', regexp_replace(df.referer, '"', '')
        ).withColumn(
        'agent', regexp_replace(df.agent, '"', '')
        )

display(final)

# COMMAND ----------

# MAGIC %md With our data organized as a dataframe, we can create a temporary view of the data as was demonstrated last week.  This will allow us to query our data using SparkSQL but the table will disappear when the notebook session is terminated:

# COMMAND ----------

final.createOrReplaceTempView('weblogs_temp')

# COMMAND ----------

# MAGIC %md What might be a better choice is to save our data to a persisted table structure. But before we do this, let's clean up our dataframe just a little more.  The datetime string and HTTP requests are redundant given the parsed elements available.  Also, the user and identity fields are not employed in this environment; let's drop those fields as well:

# COMMAND ----------

ready_to_save = final.drop('identity', 'user', 'time_string', 'request')

# COMMAND ----------

# MAGIC %md Now, let's save our data to a table:

# COMMAND ----------

ready_to_save.write.saveAsTable(
    name='weblogs',
    format='parquet',
    mode='overwrite'
  )

# COMMAND ----------

# MAGIC %md To see this table in the "data lake", let's use some SQL commands:

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended default.weblogs

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table default.weblogs

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC Returning to the NYSE pricing dataset, read it directly into a dataframe, infering the required schema.  Persist the data as a queriable Spark SQL table named pricing within the nyse database (catalog) using a Parquet format.
# MAGIC 
# MAGIC HINT: You will need to create the nyse database using the [CREATE DATABASE](https://docs.databricks.com/spark/latest/spark-sql/language-manual/create-database.html) statement.  You will also need to refer to the pricing table as nyse.pricing in order to create it in that database.

# COMMAND ----------

pricing = spark.read.csv(
  '/tmp/nyse/', 
  header=True, 
  inferSchema=True,
  dateFormat='yyyy-MM-dd\'T\'HH:mm:ss.SSSXXX'
  )

# your scenario 1 code here
spark.sql('CREATE DATABASE IF NOT EXISTS nyse')

pricing.write.saveAsTable(
  name='nyse.pricing',
  format='parquet',
  mode='overwrite'
  )

display(spark.sql('SELECT * FROM nyse.pricing'))

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables from nyse

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended nyse.pricing

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

pricing = spark.read.csv(
  '/tmp/nyse/', 
  header=True, 
  inferSchema=True,
  dateFormat='yyyy-MM-dd\'T\'HH:mm:ss.SSSXXX'
  )

# your scenario 1 code here
spark.sql('CREATE DATABASE IF NOT EXISTS nyse ')

pricing.write.saveAsTable(
    name='nyse.pricing',
    format='parquet',
    mode='overwrite'
  )

display( spark.sql('SELECT * FROM nyse.pricing') )

# COMMAND ----------

