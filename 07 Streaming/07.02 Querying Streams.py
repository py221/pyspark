# Databricks notebook source
# MAGIC %md ## YOU MUST CREATE A CLUSTER USING THE DATABRICKS 6.4 RUNTIME FOR THIS CODE TO WORK

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Structured Streaming: Querying Structured Streams
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to teach you how to query a streaming dataframe.

# COMMAND ----------

# MAGIC %md In the last lab, we learned how to connect to our streaming ingest layer.  Data read from this layer moves into a dataframe with a schema defined by the ingest layer (driver) itself:

# COMMAND ----------

eventhubs_connstring =  {
  'eventhubs.connectionString':
  '''Endpoint=sb://iothub-ns-7yycal7byd-9472802-13a4406f7e.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=UpiVkyjhSCvR8ZxxYA8ysccKTaXZmaVmgpnzHCKtchU=;EntityPath=7yycal7bydz3njkn9zrsumcwu'''
  }

df = (
  spark
    .readStream
    .format('eventhubs')
    .options(**eventhubs_connstring)
    .load()
  )

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md Looking at our data, we can see a number of metadata fields that tell us something about how the data lands in the ingest layer. The partitionKey is a value specified by the client that sent the data to the EventHub.  The EventHub service uses that key to determine to which of its internal partitions to send the data.  The internal partitions are the units by which ingest layers like EventHub scale.  For some applications, ensuring that related messages arrive on the same partition is critical but for Spark Structured Streaming applications, this is more of a point of interest than a critical piece of information.
# MAGIC 
# MAGIC Within a partition, event messages are sequenced based on the order in which they arrive.  The sequenceNumber maintains this sequence as a simple incrementing integer value.  The enqueuedTime records the timestamp on which a message landed on the EventHub service. 
# MAGIC 
# MAGIC The data actually transmitted by our client device to the Azure EventHub resides in the body field. This field is typed as binary as the EventHub has no say over what kind of data might be transmitted to it.  That puts the burden on us as the Streaming Developers to interpret this binary payload data.  Typically, this data is transmitted as either delimited text or UTF-8 JSON.  Let's convert the body to a string to see if we are working with either of these:

# COMMAND ----------

from pyspark.sql.types import *

display( 
  df.select( df.body.cast(StringType()) )
  )

# COMMAND ----------

# MAGIC %md While we can see the body field is JSON, right now it's typed as a simple string.  We need the dataframe to interpret the JSON structure, converting it to a queriable structure type.  To do this, we can use the **from_json** PySpark SQL function.
# MAGIC 
# MAGIC The **from_json** function will parse a JSON string into a structure type field. The internal structure of that field must be supplied through an explicitly defined schema.  Examining the JSON and speaking with our IOT client providers, we might determine the internal structure adheres to the following schema: 

# COMMAND ----------

from pyspark.sql.functions import from_json

# JSON structure
body_schema = StructType([
  StructField('temperature', DoubleType()),
  StructField('humidity', DoubleType())
  ])

# replace binary body with parsed json structure
telemetry = (
  df
  .withColumn('body2', from_json( df.body.cast(StringType()), body_schema ) )
  .drop('body')
  .withColumnRenamed('body2', 'body')
  )

# COMMAND ----------

display(telemetry)

# COMMAND ----------

# MAGIC %md The nested structures in the dataframe should be familiar to you given the work you have already done with JSON.  You might notice that our body is now typed as a structure which we can navigate using dot-notation.  In addition, you might notice that some fields, such as systemProperties, are typed as maps.  We haven't addressed the querying of maps, but if you think of a map as something akin to a dictionary, you should be able to see how the query pattern below makes sense:

# COMMAND ----------

telemetry.createOrReplaceTempView('telemetry')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   systemProperties['iothub-connection-device-id'] as device_id,
# MAGIC   enqueuedTime as enqueued_time,
# MAGIC   body.temperature as temperature,
# MAGIC   body.humidity as humidity
# MAGIC FROM telemetry

# COMMAND ----------

# programmatic SQL API equivalent
display(
  telemetry.
    select(
      telemetry.systemProperties['iothub-connection-device-id'].alias('device_id'),
      telemetry.enqueuedTime,
      telemetry.body.temperature.alias('temperature'),
      telemetry.body.humidity.alias('humidity')
    )
  )

# COMMAND ----------

# MAGIC %md We now have a deviceid, a timestamp, and three readings from our stream.  In our next lab, we'll look at how we aggregate these values.
# MAGIC 
# MAGIC **BEFORE PROCEEDING**, be sure to run the following block to terminate running queries in your environment.

# COMMAND ----------

# kill any active streaming jobs
sqm = spark.streams
for q in sqm.active:
  q.stop()

# COMMAND ----------

