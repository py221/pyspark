# Databricks notebook source
# MAGIC %md ## YOU MUST CREATE A CLUSTER USING THE DATABRICKS 6.4 RUNTIME FOR THIS CODE TO WORK

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Structured Streaming: Reading Structured Streams
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to teach you how to read a stream into a dataframe.

# COMMAND ----------

# MAGIC %md To read a stream, we need a driver capable of communicating with our ingest layer. Spark comes preconfigured with drivers for reading streams [from Apache Kafka and Linux file systems](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources). If we wish to read from a different streaming source such as an [AWS Kinesis Firehose](https://aws.amazon.com/kinesis/data-firehose/) or an [Azure EventHub/IOTHub](https://azure.microsoft.com/en-us/services/event-hubs/), we need to first install an appropriate driver. 
# MAGIC 
# MAGIC It's important to understand that Spark supports streaming through two libraries: Structured Streaming and Spark Streaming/DStreams.  Spark Streaming/DStreams is built upon Spark's lower-level RDD interfaces.  For most streaming applications, it's preferred that we leverage Structured Streaming, built upon the SparkSQL DataFrame interface, as it's much easier to work with for most streaming scenarios. When seeking a driver for reading a stream, it's important that the driver you choose is compatible with the streaming library you intend to use.
# MAGIC 
# MAGIC In this lab, we will be reading a stream from an Azure EventHub. (Technically, it's an Azure IOTHub but IOTHubs and EventHubs appear to be the same thing from the backend.)  Documentation for the Spark drivers for Azure EventHubs are found [here](https://github.com/Azure/azure-event-hubs-spark). Reading through the documentation, it appears Microsoft makes available two drivers: one for Streaming/DStreams and another for Structured Streaming. Reading the documentation for the [Structured Streaming driver](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/structured-streaming-eventhubs-integration.md), we don't find a downloadable driver but we do find its Maven coordinates.

# COMMAND ----------

# MAGIC %md Maven, or more formally the [Maven repository](https://mvnrepository.com/), is a platform for the distribution of open source libraries for use in Java applications.  Libraries are identified in the repository by a groupID, artifactID and version number, each of which identifies the libary owner, the library itself, and the version of the library, respectively. If you review the documentation linked to above, you will see these elements for the Spark Structured Streaming for Azure EventHub driver are:
# MAGIC 
# MAGIC `groupId = com.microsoft.azure`<br>
# MAGIC `artifactId = azure-eventhubs-spark_2.11`<br>
# MAGIC `version = 2.3.13`<br>
# MAGIC 
# MAGIC How we interpret the information in these elements varies a bit with each implementation.  For this driver, the group element tells us this is a driver developed by Microsoft for use with an Azure resource; nothing too revelatory there.  The artifactId tells us this driver is intended for use with Azure Event Hubs and Spark.  The 2.11 element following Spark denotes the Scala version used with Spark, not the Spark version itself.  (How'd we know this?  There is no standard for constructing artifact IDs so this is something you just pick up as you go.) The version element tells us not the version of the driver but the version of Spark that this driver has been tested with.  It is highly unlikely that we will find a driver version that perfectly matches the version of Spark we are using so the general rule of thumb is to use a driver that has been tested with a Spark version that is close to but which does not exceed the version of Spark we are working with.  (How'd we know the version info speaks to the Spark version and not the driver version?  Again, you just have to know.)
# MAGIC 
# MAGIC Let's do a quick check of the Scala and Spark versions of our environment to make sure this driver is compatible before attempting to install it:  

# COMMAND ----------

#SCALA VERSION

# iterate through the Spark configurations
for config_property in sc.getConf().toDebugString().split('\n'):
  # split keys and values from the configuration property
  kv = config_property.split('=')
  # if configu property key is "sparkVersion"
  if kv[0]=='spark.databricks.clusterUsageTags.sparkVersion':
    # print that version & stop looping
    print(kv[1])
    break

# COMMAND ----------

#SPARK VERSION

sc.version

# COMMAND ----------

# MAGIC %md It looks like we have the right version of Scala but we are a little ahead on our version of Spark.  This driver will most likely work with our Spark environment, but let's see if we can find a version that's a bit closer (but not higher) than our Spark version.
# MAGIC 
# MAGIC To do this, let's visit the Maven Repository page for this driver.  To do that, we need to construct a URL.  The basic structure is `https://mvnrepository.com/artifact/*groupId*/*artifactId*/*version*`.  For this driver, this comes out to https://mvnrepository.com/artifact/com.microsoft.azure/azure-eventhubs-spark_2.11/2.3.13
# MAGIC 
# MAGIC Clicking that link takes us to the Maven page for this driver, and just below the basic details for it, we see a note indicating a newer version of the driver is available. This newer version uses the same Scala version but is a little closer to our Spark version.  Let's use that one.  (Why didn't the GitHub documentation tell us about this newer version? Quite often the online documentation for open source drivers like this are out of date.  You need to be able to do a little detective work to make sure you are using the most recent, compatible driver for your environment.)

# COMMAND ----------

# MAGIC %md Okay. We have found the right driver.  But we don't actually download the driver.  Instead, we need to compile it using the mvn command-line interface (CLI) and then take that driver to our cluster.  I know; more work! But luckily the Databricks environment we are using can automate this for us.  All we need to do is combine the groupID, artifactID and version info in a colon-delimited string like this:
# MAGIC 
# MAGIC `com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.14.1`
# MAGIC 
# MAGIC Using this string, we can right-click in the background of a folder in our workspace and then select **Create** followed by **Library**.  Select **Maven** and enter the string above as your coordinate.  Once the library is compiled and imported, we simply associated it with our cluster (or in this Community Edition environment simply select **Install Automatically on All Clusters** to make things easier moving forward).
# MAGIC 
# MAGIC **NOTE** In an enterprise deployment of Databricks, using the *Install Automatically on All Clusters* is not a recommended practice.

# COMMAND ----------

# MAGIC %md With our driver installed, let's now connect to our Azure Event Hub stream source.  The basic structure of our method calls is to call **readStream** on the **spark** session object.  This tells Spark that we are creating a Structured Streaming app instead of a traditional SparkSQL app.  We then need to tell the API which driver to employ and provide any configuration options required for driver.  The configuration options are driver specific so you will need to spend a bit of time reviewing the online documentation to determine what to provide.  Most configuration settings will be supplied as a dictionary with the configuration property name and its value organized as key:value pairs.
# MAGIC 
# MAGIC Once configured, we simply call the **load** method to initiate the reading of data from the source to Spark:

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

# MAGIC %md So, where is the data on our stream? If you think back to the conversations we had about RDD and DataFrame pipelines, no work is performed until an action is defined.  The same holds true here with Structured Streaming.
# MAGIC 
# MAGIC At this point, all we've done is establish a connection from Spark to the streaming source and initalized our DataFrame with a schema that's defined by the streaming driver.  Here's that schema:

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md  We'll discuss these schema elements in more detail in the next lab, but for now, let's just verify we have data coming across by streaming it to output:

# COMMAND ----------

#df.show()
display(df)

# COMMAND ----------

# MAGIC %md **BEFORE PROCEEDING**, be sure to stop your streaming queries by pressing the Stop Execution button at the top of this notebook. Alternatively, you can run the code in the next cell.

# COMMAND ----------

# kill any active streaming jobs
sqm = spark.streams
for q in sqm.active:
  q.stop()

# COMMAND ----------

