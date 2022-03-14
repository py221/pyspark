# Databricks notebook source
# MAGIC %md ## YOU MUST CREATE A CLUSTER USING THE DATABRICKS 6.4 RUNTIME FOR THIS CODE TO WORK

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Structured Streaming: Calculating Stream Aggregates
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to teach you how to calculate aggregations on a structured stream.

# COMMAND ----------

# MAGIC %md In the last lab, we learned how to extract data from a stream:

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import from_json

eventhubs_connstring =  {
  'eventhubs.connectionString':
  '''Endpoint=sb://iothub-ns-7yycal7byd-9472802-13a4406f7e.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=UpiVkyjhSCvR8ZxxYA8ysccKTaXZmaVmgpnzHCKtchU=;EntityPath=7yycal7bydz3njkn9zrsumcwu'''
  }

# connect to EventHub
df = (
  spark
    .readStream
    .format('eventhubs')
    .options(**eventhubs_connstring)
    .load()
  )

# JSON structure for body
body_schema = StructType([
  StructField('temperature', DoubleType()),
  StructField('humidity', DoubleType()),
  ])

# replace binary body with parsed json structure
telemetry = (
  df
  .withColumn('body2', from_json( df.body.cast('string'), body_schema ) )
  .drop('body')
  .withColumnRenamed('body2', 'body')
  )

# register dataframe as temp view
telemetry.createOrReplaceTempView('telemetry')

# COMMAND ----------

# MAGIC %sql -- retrieve relavent data from stream
# MAGIC 
# MAGIC SELECT
# MAGIC   systemProperties['iothub-connection-device-id'] as device_id,
# MAGIC   enqueuedTime as enqueued_time,
# MAGIC   body.temperature as temperature
# MAGIC FROM telemetry

# COMMAND ----------

## same as previous query
#display(
#  telemetry.
#    select(
#      telemetry.systemProperties['iothub-connection-device-id'].alias('device_id'),
#      telemetry.enqueuedTime,
#      telemetry.body.temperature.alias('temperature')
#      )
#    )       

# COMMAND ----------

# MAGIC %md Now we will attempt to calculate some simple aggregations on our data set:

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   x.device_id,
# MAGIC   avg(x.temperature) as temperature_avg,
# MAGIC   min(x.temperature) as temperature_min,
# MAGIC   max(x.temperature) as temperature_max,
# MAGIC   count(*) as readings
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     systemProperties['iothub-connection-device-id'] as device_id,
# MAGIC     enqueuedTime as enqueued_time,
# MAGIC     body.temperature as temperature
# MAGIC   FROM telemetry
# MAGIC   ) x
# MAGIC GROUP BY x.device_id

# COMMAND ----------

# programmatic SQL version of last query

aggregations = {
  'temperature':'avg',
  'temperature':'min',
  'temperature':'max',
  '*':'count'
  }

display(
  telemetry
      .select(
        telemetry.systemProperties['iothub-connection-device-id'].alias('device_id'),
        telemetry.enqueuedTime,
        telemetry.body.temperature.alias('temperature'),
        telemetry.body.humidity.alias('humidity')
        )
      .groupBy('device_id').agg(aggregations)
      .withColumnRenamed('avg(temperature)','temperature_avg')
      .withColumnRenamed('min(temperature)','temperature_min')
      .withColumnRenamed('max(temperature)','temperature_max')
      .withColumnRenamed('count(1)', 'readings')
  )

# COMMAND ----------

# MAGIC %md The SQL syntax should be very familiar to you.  Being built on Spark DataFrames, Spark Structured Streaming adheres to most of the familiar models for data manipulation and access that you are familar with from your work with batch data. 
# MAGIC 
# MAGIC The big difference between Structured Streaming DataFrames and traditional Spark SQL DataFrames is that the queries are executing on a regular basis and results from prior query runs affect downstream runs.  For example, when the last query calculates a max or min, it holds onto the results of its last microbatch run.  When it fires again, those results are employed with the incoming data to see if a new max or min has been found. Similarly, the count calculation is counting events coming through the streaming query.  With each microbatch, the count of the new records are being added to the previous count to produce a new, higher count result.
# MAGIC 
# MAGIC As you start to develop a picture of the Structured Streaming queries as operating across a stream of continously updating information, you might start to become aware of a serious problem.  How does Structured Streaming hold on to all the data required to produce results over the duration of a long-running standing query?
# MAGIC 
# MAGIC The magic behind Structured Streaming is that it is only holding onto the incremental values needed to calculate the next set of results.  For example, once it's calculated a min or a max for a batch of data, it doesn't need to retain the records of the prior batch in order to successfully address the next batch. Instead, it holds onto its current min and max values, compares it to the incoming data, calculates new min and max values, and then releases the incoming records as having been processed.  Simiarly with a count, all that's needed between microbatches is the last count.  As new records come in, the count is incremented and the incomign records can be released.  While this logic can get more complex for more sophisticated calculations, the basic principle of holding on to just those intermediate values needed to successfully perform the next wave of calculations is what keeps the memory pressure with Structured Streaming low.

# COMMAND ----------

# MAGIC %md When working with streaming data, time is often a critical element to consider.  There is the time data is generated on the device, the time data arrives on the ingest layer, and the time it is read from the ingest layer.  Often, these time elements in the streaming data may be used as the basis of various aggregations.  
# MAGIC 
# MAGIC When our goal is to aggregate data over ranges of time, we may use the [**window**](https://spark.apache.org/docs/2.3.1/api/python/pyspark.sql.html#pyspark.sql.functions.window) function to calculate one or more groups of time to which values may be assigned.  For example, here we are using the **window** function to calculate aggregates over sliding (overlapping) 60 second windows, each of which is calculated on a 5 second interval:

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   x.device_id,
# MAGIC   window( enqueued_time, '60 seconds', '5 seconds') as window,
# MAGIC   avg(x.temperature) as temperature_avg,
# MAGIC   min(x.temperature) as temperature_min,
# MAGIC   max(x.temperature) as temperature_max,
# MAGIC   count(*) as readings
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     systemProperties['iothub-connection-device-id'] as device_id,
# MAGIC     enqueuedTime as enqueued_time,
# MAGIC     body.temperature as temperature,
# MAGIC     body.humidity as humidity
# MAGIC   FROM telemetry
# MAGIC   ) x
# MAGIC GROUP BY x.device_id, window( enqueued_time, '60 seconds', '5 seconds')
# MAGIC ORDER BY window DESC

# COMMAND ----------

# programatic SQL version of last query
from pyspark.sql.functions import window

aggregations = {
  'temperature':'avg',
  'temperature':'min',
  'temperature':'max',
  '*':'count'
  }

display(
  telemetry
      .select(
        telemetry.systemProperties['iothub-connection-device-id'].alias('device_id'),
        telemetry.enqueuedTime,
        telemetry.body.temperature.alias('temperature'),
        telemetry.body.humidity.alias('humidity')
        )
      .groupBy('device_id', window(telemetry.enqueuedTime, '30 seconds', '5 seconds') ).agg(aggregations)
      .withColumnRenamed('avg(temperature)','temperature_avg')
      .withColumnRenamed('min(temperature)','temperature_min')
      .withColumnRenamed('max(temperature)','temperature_max')
      .withColumnRenamed('count(1)', 'readings')
      .orderBy('window', ascending=[0])
  )

# COMMAND ----------

# MAGIC %md The window function makes aggregating data around time intervals (windows) fairly easy. Watch as the data in the display above updates.  Window intervals are calculated and records are assigned to one or more windows based on how the timestamp on the record aligns with the window range.  As before, the Structured Streaming engine is performing its magic under the covers to keep the resource consumption low.

# COMMAND ----------

# MAGIC %md  **BEFORE PROCEEDING**, be sure to run the following block to terminate running queries in your environment.

# COMMAND ----------

# kill any active streaming jobs
sqm = spark.streams
for q in sqm.active:
  q.stop()

# COMMAND ----------

