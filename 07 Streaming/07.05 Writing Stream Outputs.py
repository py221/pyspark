# Databricks notebook source
# MAGIC %md ## YOU MUST CREATE A CLUSTER USING THE DATABRICKS 6.4 RUNTIME FOR THIS CODE TO WORK

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Structured Streaming: Writing Stream Outputs
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to teach you how to persist the results of your stream calculations.

# COMMAND ----------

# MAGIC %md Now that you know how to query a streaming data source, let's consider what we do with the query output.  To get us started, let's re-initialize a query against our stream:

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
    .withWatermark('enqueuedTime', '5 minutes') 
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
        telemetry.body.temperature.alias('temperature')
        )
      .groupBy('device_id', window(telemetry.enqueuedTime, '30 seconds', '5 seconds') )
      .agg(aggregations)
      .withColumnRenamed('avg(temperature)','temperature_avg')
      .withColumnRenamed('min(temperature)','temperature_min')
      .withColumnRenamed('max(temperature)','temperature_max')
      .withColumnRenamed('count(1)', 'readings')
  )

# COMMAND ----------

# MAGIC %md Using the **writeStream** method on our streaming dataframe, we initiate a DataStreamWriter object.  This object has the ability to send output to numerous destinations.  A good one with which to start is the memory sink:

# COMMAND ----------

(
  windowed_agg
    .writeStream
    .format('memory')
    .queryName('device_temperature')
    .outputMode('update')
    .start()
  )

# COMMAND ----------

# MAGIC %md With our pinnned in an in-memory table, we can easily query it.  Notice that the sorting of the data takes place not in the stream dataframe definition but in the retreival of the data from the sink.  Streaming queries don't often support ordering of their outputs directly.  Notice too that I'm calling **spark.table** instead of calling **spark.sql('SELECT * FROM ...')**.  This is simply a short-hand way of specifying the same logic:

# COMMAND ----------

display( spark.table('device_temperature').orderBy('window', 'device_id', ascending=[0,1]) )

# COMMAND ----------

# MAGIC %md Our memory sink table, as configured above, is functionally similar to creating a temp view on a streaming dataframe though the **display** function doesn't automatically refresh the output display.  The memory data sink is a good option for development/debugging exercises but not one we would pursue for production purposes.
# MAGIC 
# MAGIC A more traditional way of persisting data is to persist the data to file.  Here, we will calculate a date for our data, partition our data on date and 

# COMMAND ----------

from pyspark.sql.functions import to_date

(
  windowed_agg
    .writeStream
    .format('parquet')
    .option('path', '/tmp/device_temperature2')
    .option('checkpointLocation', '/tmp/checkpoints/device_temperature2')
    .outputMode('append')
    .trigger(processingTime='60 seconds')
    .start()
  )

# COMMAND ----------

# MAGIC %fs ls /tmp/device_temperature2

# COMMAND ----------

# MAGIC %md Notice in the previous cell that we have specified a trigger interval.  When we write to disk, old entries are not updated.  We can send data for any window that receives an update (which is what the **outputMode** is configured for) but that means that should a window receive one, two and then three data updates, we will write three records out to storage.
# MAGIC 
# MAGIC One way to reduce the amount of redundant data moving to storage is to set a **trigger** on the write operation.  This trigger executes the write action on the interval specified.  By setting this trigger above the interval on the windows themselves, this allows time for the windows to achieve a more stable state before being written to disk.  Of course, there is no guarantee a window won't be updated after a 60 second interval and our watermark allows updates up to the 5 minute mark.  Thinking through the timings of records arriving, the frequency of updates to a sink, etc. and the implications for accuracy and redundancy is one of the key considerations in designing a real-time processing system.

# COMMAND ----------

# MAGIC %md With every trigger execution, an often small unit of data is written to from the stream to disk.  The result is one or more small files being generated in our destination folder.  Small files are problematic when reading large datasets in Big Data solutions as our file system drivers are optimized for scans.  With large files, we can scan a large volume of data in a continous manner.  With small files, scans are short-lived and we have to generate many more overall scan operations.
# MAGIC 
# MAGIC This was a driving factor behind the development of Delta Lake (mentioned in the last class) as the next generation of Parquet.  When we stream data to a Delta Lake storage format, we still generate a bunch of small files but there are operations which can be performed on the older data to consolidate files into larger files as part of a background process.  With the implementation of a transaction log, these operations can exist concurrently with on-going write operations.  (Reading from these files for normal queries also benefits from the concurrency provided by the transaction log.)
# MAGIC 
# MAGIC To write our data to a Delta Lake format, we simply later the previous statement to use delta instead of parquet:

# COMMAND ----------

(
  windowed_agg
    .writeStream
    .format('delta')
    .option('path', '/tmp/device_temperature2_delta')
    .option('checkpointLocation', '/tmp/checkpoints/device_temperature2_delta')
    .outputMode('append')
    .trigger(processingTime='60 seconds')
    .start()
  )

# COMMAND ----------

# MAGIC %md To clean up the fragmented files, we periodically run the optimize command as shown here:
# MAGIC 
# MAGIC **NOTE** You will need to wait for the trigger in the statement above to fire with at least one record in it before the next statement will work.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC OPTIMIZE '/tmp/device_temperature2_delta'

# COMMAND ----------

# MAGIC %md  **BEFORE PROCEEDING**, be sure to run the following block to terminate running queries in your environment.

# COMMAND ----------

# kill any active streaming jobs
sqm = spark.streams
for q in sqm.active:
  q.stop()

# COMMAND ----------

