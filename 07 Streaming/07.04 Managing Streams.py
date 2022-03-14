# Databricks notebook source
# MAGIC %md ## YOU MUST CREATE A CLUSTER USING THE DATABRICKS 6.4 RUNTIME FOR THIS CODE TO WORK

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Structured Streaming: Managing Streams
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to teach you how to control the flow of data through a streaming query as well as how to recover from query failure.

# COMMAND ----------

# MAGIC %md In the last lab, we learned that the Structured Streaming engine keeps hold of the information it needs to update a result set as new data arrives on the ingest layer. This keeps the memory pressure on the cluster low, but if we are aggregating over time, the number of windows grows and the size of the in-memory result set grows with it.
# MAGIC 
# MAGIC To keep our result set manageable, we we need to define a point in time beyond which we will discard our data and not attempt to recalculate values for a window.  Aggregates associated with windows occuring after this point, called a watermark, can be discarded and any values that may arrive late that would need to be assigned to those windows will be ignored. 
# MAGIC 
# MAGIC Here is one example of how we might define a watermark:

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import from_json

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
    .withWatermark('enqueuedTime', '5 minutes')  # watermark defined here
  )

body_schema = StructType([
  StructField('temperature', DoubleType()),
  StructField('humidity', DoubleType())
  ])

telemetry = (
  df
  .withColumn('body2', from_json( df.body.cast('string'), body_schema ) )
  .drop('body')
  .withColumnRenamed('body2', 'body')
  )

from pyspark.sql.functions import window

aggregations = {
  'temperature':'avg',
  'temperature':'min',
  'temperature':'max',
  '*':'count'
  }

windowed_agg = (
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
  )

# COMMAND ----------

display(windowed_agg)

# COMMAND ----------

# MAGIC %md In the preceeding query, we defined a 5 minute watermark.  (If you missed it, look for the **withWatermark()** method call in the original DataFrame definition.) With this watermark set, aggregations for windows older than 5 minutes will be dropped from the in-memory result set.  This keeps the memory footprint manageable but it does have a consequence: what happens if a record from our device arrives 5 or more minutes late to the ingest layer?
# MAGIC 
# MAGIC We have to anticipate that some data coming from external devices will arrive late. There can be network issues that prevent transmission of data from a device to the ingest layer; ingest layers can go offline, forcing IOT devices to queue up data locally which are then sent as part of a backlog with the ingest layer comes back online; and other internal issues can cause devices to hold onto data, transmitting it later than normal.
# MAGIC 
# MAGIC With our watermark set to 5 minutes, any data Spark might read from the ingest layer that's older than 5 minutes will simply be ignored. Is this the right approach?
# MAGIC 
# MAGIC The short answer is that it depends.  The higher we set the watermark value, the more data we must retain in memory and the more pressure we put on our cluster. At some watermark value, we simply can't provide our cluster enough resources and the job crashes. So our job as streaming developers is to think through the probability of late arriving data at different time intervals, the amount of memory consumed with increasing watermark durations, the cost of allocating memory resources to deal with higher memory demands, etc. and then attempting to find a balance for all these concerns.
# MAGIC 
# MAGIC As you consider this, carefully think through how the real-time data is being consumed.  How far back are decision makers looking at the data to make the real-time decisions these queries are enabling?  Being perfectly accurate in the calculation of an aggregate value for yesterday or even a few minutes ago may not affect a decision that needs to be made based on data for the current time period.  If your goal is to lose no data so that you have a complete history of data, keep in mind that you can setup multiple jobs to process data from an ingest layer.  One can employ watermarks to keep the memory pressure of windowed aggregations manageable will another simply sends data to long-term storage with no aggregations and no watermarks needed. 

# COMMAND ----------

# MAGIC %md The number one cause of streaming query failure is insufficient memory resources for the continued processing of a stream.  You saw earlier how a watermark can be used to limit the accumulation of aggregate data over time, but we should also take a look at the amount of data flowing from the ingest layer into the streaming query.
# MAGIC 
# MAGIC Most devices are configured to transmit data at a regular interval.  However, some may do so in response to device events which can make predicting data volumes difficult.  Even if we have regular transmission of data to an ingest layer, devices may go offline, accumulating data locally until they re-establish connectivity.  Once this occurs, they can burst their backload of data to the ingest layer.  Ideally, your ingest layer is configured to accept this volume of data but is your streaming job prepared to handle it?
# MAGIC 
# MAGIC One common trick for avoiding excessive data in a streaming batch is to configure the ingest layer driver to read a maximum number of records at a given time. In a burst situation, data accumulates on the ingest layer while Structure Streaming widdles away at it at a manageable pace.  Here is an example of how we might configure a max number of records with the Azure Event Hub driver:

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import from_json

eventhubs_connstring =  {
  'eventhubs.connectionString':
  'Endpoint=sb://iothub-ns-iothub-uuv-2655362-368a757296.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=V+IGBygZk0xp0t+SAya6h72jcGMnFkxw8Is2fs/gVmw=;EntityPath=iothub-uuvws',
  'maxEventsPerTrigger': 100
  }

df = (
  spark
    .readStream
    .format('eventhubs')
    .options(**eventhubs_connstring)
    .load()
    .withWatermark('enqueuedTime', '5 minutes')  # watermark defined here
  )


# COMMAND ----------

# MAGIC %md With the **maxEventsPerTrigger** configuration property set, we've limited the number of records being processed by our query at any given point in time.  This allows our ingest layer to absorb large bursts of data while not having to scale our Spark infrastructure to deal with unexpectedly large volumes of data.

# COMMAND ----------

# MAGIC %md  **BEFORE PROCEEDING**, be sure to run the following block to terminate running queries in your environment.

# COMMAND ----------

# kill any active streaming jobs
sqm = spark.streams
for q in sqm.active:
  q.stop()

# COMMAND ----------

