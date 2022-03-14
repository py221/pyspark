# Databricks notebook source
# MAGIC %md 
# MAGIC ###DataFrames: Saving DataFrames as Files
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to teach you how to save Spark DataFrames to disk.

# COMMAND ----------

# MAGIC %md When we read data into a dataframe for the first time, we often are reading it from an *unstructured* source.  By *unstructured*, what we mean is that the data being read must be parsed or interpretted to align it with the schema we are assigning to the dataframe.  There's a bit of overhead that comes with this so that when we read a dataset to a dataframe, we often wish to save it in a structured data file that saves the data in a schema-aligned format (which includes the actual schema definition).
# MAGIC 
# MAGIC Consider again our UFO sightings dataset which is read from a tab-delimited text file:

# COMMAND ----------

spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY")

# COMMAND ----------

from pyspark.sql.types import *

sightings_schema=StructType([
    StructField('date_sighted', DateType()),
    StructField('date_reported', DateType()),
    StructField('location', StringType()),
    StructField('shape', StringType()),
    StructField('duration', StringType()),
    StructField('description', StringType())
    ])
sightings = spark.read.csv(
    'wasbs://downloads@smithbc.blob.core.windows.net/ufo/ufo_awesome.tsv',
    sep='\t', 
    schema=sightings_schema,
    dateFormat='yyyyMMdd')

#sightings.show(10)
display(sightings)

# COMMAND ----------

# MAGIC %md Now that the data is loaded into a dataframe, it might be nice to save it for future use in a schema aligned manner.  To do this, we can save the data in a number of schema-oriented formats such as Avro and ORC but the most popular of these formats is Parquet:

# COMMAND ----------

sightings.__class__

# COMMAND ----------

sightings.write.parquet( 
  '/tmp/sightings/', 
  mode='overwrite'
)

# COMMAND ----------

# MAGIC %md When we write the data in the parquet format, we write it to a target directory, in this case /tmp/sightings.  If the directory already exists, the operation will fail unless we specify with the mode option that we wish to overwrite it.  
# MAGIC 
# MAGIC Once saved to disk, we end up with one or more Parquet files representing the contents of the dataframe:

# COMMAND ----------

# MAGIC %fs ls /tmp/sightings/

# COMMAND ----------

# MAGIC %md Why do we have more than one Parquet file in this folder?  It's because the number of output files written is aligned with the number of partitions associated with the dataframe.  If we have two output files, it's because we had two partitions associated with the RDD that underlies the dataframe.  Want to see how many partitions we have before we write to output?:

# COMMAND ----------

print( sightings.rdd.getNumPartitions() )

# COMMAND ----------

# MAGIC %md If our goal is to write to a specific number of partitions, we can repartition the data in our dataframe with a simple method call:

# COMMAND ----------

repart_sightings = sightings.repartition( numPartitions=4 )

# COMMAND ----------

# MAGIC %md While **repartition()** looks like a simple method call, you should be aware that a repartition requires data to be moved between the nodes of a cluster.  This shuffle operation is expensive so we typically don't perform repartitions unless we have a specific need.  If we specify a column or columns around which to repartition, Spark attempts to distribute the data to the new partitions so that rows with the same values for the given columns reside together.  If no columns are specified, Spark will repartition with an eye towards a relatively even distribution of rows between the target number of partitions.
# MAGIC 
# MAGIC With the data repartitioned, let's write it to disk again to see the impact:

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/tmp/sightings

# COMMAND ----------

spark.conf.set("spark.sql.parquet.compression.codec", "gzip")

# COMMAND ----------

repart_sightings.write.parquet('/tmp/sightings/', mode='overwrite')

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY")

# COMMAND ----------

repart_sightings.write.format("parquet")\
                        .mode('Overwrite')\
                        .save("tmp/sightings")

# COMMAND ----------

# MAGIC %fs ls /tmp/sightings/

# COMMAND ----------

# MAGIC %md The value of using a schema-aligned format such as Parquet is that when data is read from disk back into a dataframe, the schema comes along with it.  Let's read our sightings data from Parquet to a new dataframe to see this in action:

# COMMAND ----------

new_sightings = spark.read.parquet('/tmp/sightings/')

#new_sightings.show(10)
display(new_sightings)

# COMMAND ----------

new_sightings.printSchema()

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC Return to Scenario 1 & 2 in the last lab.  Save the result of your query to **/tmp/popular_destinations/** using the Parquet format. (Hint: There are multiple ways you can approach the capturing of the SQL results. Find one that works well for you.)

# COMMAND ----------

# your scenario 1 code here

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
from pyspark.sql.types import *

flights_schema = StructType([
  StructField('month', IntegerType()),
  StructField('dayofmonth', IntegerType()),
  StructField('dayofweek', IntegerType()),
  StructField('deptime', IntegerType()),
  StructField('arrtime', IntegerType()),
  StructField('uniquecarrier', StringType()),
  StructField('flightnum', IntegerType()),
  StructField('tailnum', StringType()),
  StructField('elapsedtime', IntegerType()),
  StructField('airtime', IntegerType()),
  StructField('arrdelay', IntegerType()),
  StructField('depdelay', IntegerType()),
  StructField('origin', StringType()),
  StructField('dest', StringType()),
  StructField('distance', IntegerType()),
  StructField('taxiin', IntegerType()),
  StructField('taxiout', IntegerType()),
  StructField('cancelled', IntegerType()),
  StructField('cancellationcode', StringType()),
  StructField('diverted', IntegerType())
  ])

flights = spark.read.csv(
    'wasbs://downloads@smithbc.blob.core.windows.net/flights/flights.csv', 
    schema=flights_schema
    )
flights.createOrReplaceTempView('flights')

airports_schema = StructType([
  StructField('code', StringType()),
  StructField('name', StringType()),
  StructField('city', StringType()),
  StructField('state', StringType()),
  StructField('country', StringType()),
  StructField('latitude', FloatType()),
  StructField('longitude', FloatType())
  ])

airports = spark.read.csv(
  'wasbs://downloads@smithbc.blob.core.windows.net/flights/airports.csv',
  schema=airports_schema,
  header=True
  )
airports.createOrReplaceTempView('airports')

sql_statement =  '''
  SELECT
    a.dest as airport_code,
    b.name as airport_name,
    count(*) as arriving_flights
  FROM flights a
  LEFT OUTER JOIN airports b
    ON a.dest=b.code
  GROUP BY a.dest, b.name
  ORDER BY arriving_flights DESC
  '''

destinations = spark.sql(sql_statement)

destinations.write.parquet('/tmp/popular_destinations/', mode='overwrite')