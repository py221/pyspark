# Databricks notebook source
# MAGIC %md 
# MAGIC ###DataFrames: Creating DataFrames
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to teach you how to create Spark DataFrames.

# COMMAND ----------

# MAGIC %md A Spark DataFrame is a collection of data to which a well-defined schema has been defined. Here is an example of the creation of a DataFrame for the UFO sightings dataset we've been working with in previous labs:

# COMMAND ----------

from pyspark.sql.types import *

sightings_schema = StructType([
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
  dateFormat='yyyyMMdd'
  )

sightings.show(10)
#display(sightings)

# COMMAND ----------

# MAGIC %md There's a lot going on in the cell above, so let's take it one piece at a time.
# MAGIC 
# MAGIC First, we start by defining a schema.  A Spark dataframe's schema is defined using SparkSQL data types, imported here using the **pyspark.sql.types** library.  This library contains many scalar data types including:
# MAGIC 
# MAGIC * StringType()
# MAGIC * BinaryType()
# MAGIC * BooleanType()
# MAGIC * DateType()
# MAGIC * TimestampType()
# MAGIC * DecimalType(*precision*, *scale*)
# MAGIC * DoubleType()
# MAGIC * FloatType()
# MAGIC * ByteType()
# MAGIC * ShortType()
# MAGIC * IntegerType()
# MAGIC * LongType()
# MAGIC 
# MAGIC In addition, the library supports complex data types including:
# MAGIC 
# MAGIC * ArrayType()
# MAGIC * MapType()
# MAGIC * StructType()
# MAGIC 
# MAGIC Information on each of these data types can be found [here](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.types), but the critical things to remember when defining a schema is that each field in the schema is named and typed, in order of occurance, and organized within a **StructType()**.
# MAGIC 
# MAGIC Using the **spark** session variable, data is read from a file or directory into the dataframe. The **spark** session is the higher-level equivalent of the **sc** object used with RDDs.  The difference between the two is that the **spark** session object provides the developer access to dataframes and the SparkSQL functionality associated with them.
# MAGIC 
# MAGIC Using the **spark** session variable, we read data. The **read** method generates a DataFrameReader object.  By indicating the DataFrameReader object is reading from a delimited text file, as identified by the **csv** method call, a built-in set of parsing instructions are called up to read and interpret the incoming data.  
# MAGIC 
# MAGIC Of course, not all delimited text files have fields separated by commas.  In our call to the **csv** method, we identify that a tab-character is used as the field separator.  Now that the DataFrameReader knows how to parse the incoming data, we supply it the schema to map the data to.  For some types like StringType() and IntegerType(), the translation of values to typed-fields in the dataframe is very straightforward.  For others, like the DateType(), incoming string values might employ differing formats so that an optional **dateFormat** option is specified to further aid the DataFrame Reader.
# MAGIC   

# COMMAND ----------

# MAGIC %md The following was an example of the explicit definition of a schema. This is the prefered manner of creating Spark dataframes in most scenarios, but there are occassions, especially during the development of data processing routines against new datasets, when you might ask the DataFrame Reader to take a crack at deriving a schema for the incoming data.  This is referred to as implicit schema definition.
# MAGIC 
# MAGIC Notice in this example of implicit schema definition the lack of a well-defined schema and the structure of the resulting dataframe:

# COMMAND ----------

sightings = spark.read.csv( 
  'wasbs://downloads@smithbc.blob.core.windows.net/ufo/ufo_awesome.tsv', 
  sep='\t',
  # schema=sightings_schema,
  dateFormat='yyyyMMdd',
  inferSchema=True
  )

#sightings.show(10)
display(sightings)

# COMMAND ----------

sightings.printSchema()

# COMMAND ----------

sightings.__class__

# COMMAND ----------

# MAGIC %md When Spark has no way to determine the names of the fields in a dataset, it will assign default values of *_c* followed by a number.  It will also default to simple data types that may or may not best fit the data.  (If the **inferSchema** option is not set to True, implicit schema inference will default all data types to a StringType().)
# MAGIC 
# MAGIC Of course, not every dataset is so poorly interpretted with an implicit schema definition:

# COMMAND ----------

pricing = spark.read.csv(
  'wasbs://downloads@smithbc.blob.core.windows.net/nyse/', 
  header=True, 
  inferSchema=True,
  dateFormat='yyyy-MM-dd\'T\'HH:mm:ss.SSSXXX'
  )

pricing.show(10)
#display(pricing)

# COMMAND ----------

pricing.printSchema()

# COMMAND ----------

# MAGIC %md Implicit schema inference did a really good job with the pricing data set.  Still, there is overhead with it in that Spark must read some portion of the incoming data in order to generate a schema. It then re-reads the data with its auto-generated schema to populate the dataframe.  Because of the overhead of schema generation, it's best to avoid implicit schema inference and instead explicitly define a schema as demonstrated here:

# COMMAND ----------

pricing_schema = StructType([
  StructField('exchange', StringType()),
  StructField('symbol', StringType()),
  StructField('date', DateType()),
  StructField('price_open', DoubleType()),
  StructField('price_high', DoubleType()),
  StructField('price_low', DoubleType()),
  StructField('price_close', DoubleType()),
  StructField('volume', IntegerType()),
  StructField('price_adj_close', DoubleType())  
  ])

pricing = spark.read.csv(  
  'wasbs://downloads@smithbc.blob.core.windows.net/nyse/', 
  header=True, 
  schema=pricing_schema,
  dateFormat='yyyy-MM-dd\'T\'HH:mm:ss.SSSXXX'
  )

#pricing.show(10)
display(pricing)

# COMMAND ----------

pricing.printSchema()

# COMMAND ----------

# MAGIC %md Notice in this example that the field names do not necessarily match those given in the header row.  I still indicate the presense of the header row in the **csv()** method call but through the proviced schema I can define my own field names.
# MAGIC 
# MAGIC Notice too that in the pricing dataframe created using schema inference, the date field is interpretted as a TimestampType() while in the explicitly defined schema, it's interpretted as a DateType().  The difference between timestamps and dates will be discussed in a future section of this class, but for now consider timestamps as date-time values while dates are simply dates without time.

# COMMAND ----------

# MAGIC %md Dataframes provide access to higher-level functionality but they are still fundamentally RDDs.  As such, you can access the RDD under each dataframe with a simple call:

# COMMAND ----------

pricing_rdd = pricing.rdd

pricing_rdd.take(1)

# COMMAND ----------

# MAGIC %md Accessing an element in the underlying RDD this way, you can see that each element in the RDD is typed as a Row object.  While there is functionality you can employ through this object, accessing it is often for academic purposes as we rarely move from the dataframe down to the RDD level in Spark. 
# MAGIC 
# MAGIC That said, quite frequently we will access messy data using an RDD, clean it up, and then convert the cleansed RDD into a dataframe. Here is a trivial example of this:

# COMMAND ----------

def splitSighting( line ):
  '''
  Split the line based on a tab delimiter and returns the first 6 fields from the split.
  The function will return a 6-item list regardless of the shape of the incoming line.
  '''
  ret = (line + ('\t'*6)).split('\t')
  return ret[:6]

def isGoodSighting( fields ):
  if len(fields[0].strip()) == 0:
    return False
  if len(fields[1].strip()) == 0:
    return False
  if len(fields[5].strip()) == 0:
    return False
  else:
    return True

sightings = sc.textFile( 'wasbs://downloads@smithbc.blob.core.windows.net/ufo/ufo_awesome.tsv' )
split = sightings.map( splitSighting )
filtered = split.filter(isGoodSighting )

sightings_schema = StructType([
  StructField('date_sighted', StringType()),
  StructField('date_reported', StringType()),
  StructField('location', StringType()),
  StructField('shape', StringType()),
  StructField('duration', StringType()),
  StructField('description', StringType())
  ])

sightings = spark.createDataFrame( 
  filtered,
  schema=sightings_schema
  )

display(sightings)
#sightings.show(10)

# COMMAND ----------

# MAGIC %md Notice in the previous example that I did not type the date_sighted and date_reported fields to DateType().  This is because when moving from an RDD to a dataframe using the **createDataFrame** method, type conversions are expected to have been performed in advance on the RDD.  I'll show you how to do that in a later lab but for now I'd like to keep the code simple.
# MAGIC 
# MAGIC So, why might I have read this data into an RDD first before flipping it into a dataframe? Doing so provides me the opportunity to manipulate the data at a lower level which can be helpful when data is very poorly formed. Here, I'm taking advantage of the RDD to read the raw lines, parse them, fill in the gaps of any missing fields and remove any extras, and then filter out bad data rows.  
# MAGIC 
# MAGIC While I might like to manage my data set very tightly like this, it's not really necessary in this particular case.  When we read data directly to a dataframe with a supplied schema, the DataFrameReader performs a very liberal interpretation of the incoming data.  Where there is misalignment with the schema, *Null* values will be deposited in place of those fields that can't be interpretted in a consistent manner. In this way, the DataFrameReader is able to deal with common, minor misalignements between the incoming dataset and the defined schema without generating an exception.  When this interpretation is not desired or the misalignment is too great for the reader to overcome, starting with an RDD is always an option.

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC Read the flight data found at **wasbs://downloads@smithbc.blob.core.windows.net/flights/flights.csv** into a dataframe, supplying an appropriate schema able to accomodate the following fields:
# MAGIC 
# MAGIC 0.month - the month number of the year, i.e. 1-12<br>
# MAGIC 1.dayofmonth - the day number of the month, i.e. 1-31<br>
# MAGIC 2.dayofweek - the day number of the week, i.e. 1-7<br>
# MAGIC 3.deptime - the time of departure in hhmm notation<br>
# MAGIC 4.arrtime - the time of arrival in hhmm notation<br>
# MAGIC 5.uniquecarrier - the FAA carrier code<br>
# MAGIC 6.flightnum - the carrier's flight number<br>
# MAGIC 7.tailnum - the tail number of the plane<br>
# MAGIC 8.elapsedtime - the time from departure to arrival<br>
# MAGIC 9.airtime - the time in the air<br>
# MAGIC 10.arrdelay - the minutes of arrival delay<br>
# MAGIC 11.depdelay - the minutes of departure delay<br>
# MAGIC 12.origin - the airport of orgin<br>
# MAGIC 13.dest - the airport of destination<br>
# MAGIC 14.distance - the flight distance in miles<br>
# MAGIC 15.taxiin - the minutes of landing to arrival at the terminal gate<br>
# MAGIC 16.taxiout - the minutes from departure from terminal gate to take off<br>
# MAGIC 17.cancelled - 0 = not cancelled, 1 = cancelled<br>
# MAGIC 18.cancellationcode - the FAA code indicating the reason for canecellation<br>
# MAGIC 19.diverted - 0 = not diverted, 1 = diverted
# MAGIC 
# MAGIC Note that there are no headers in this file.
# MAGIC 
# MAGIC (Answer: 
# MAGIC `+-----+----------+---------+-------+-------+-------------+---------+-------+-----------+-------+--------+--------+------+----+--------+------+-------+---------+----------------+--------+`<br>
# MAGIC `|month|dayofmonth|dayofweek|deptime|arrtime|uniquecarrier|flightnum|tailnum|elapsedtime|airtime|arrdelay|depdelay|origin|dest|distance|taxiin|taxiout|cancelled|cancellationcode|diverted|`<br>
# MAGIC `+-----+----------+---------+-------+-------+-------------+---------+-------+-----------+-------+--------+--------+------+----+--------+------+-------+---------+----------------+--------+`<br>
# MAGIC `|    1|         3|        4|   2003|   2211|           WN|      335| N712SW|        128|    116|     -14|       8|   IAD| TPA|     810|     4|      8|        0|            null|       0|`<br>
# MAGIC `|    1|         3|        4|    926|   1054|           WN|     1746| N612SW|         88|     78|      -6|      -4|   IND| BWI|     515|     3|      7|        0|            null|       0|`<br>
# MAGIC `|    1|         3|        4|   1940|   2121|           WN|      378| N726SW|        101|     87|      11|      25|   IND| JAX|     688|     4|     10|        0|            null|       0|`<br>
# MAGIC `|    1|         3|        4|   1937|   2037|           WN|      509| N763SW|        240|    230|      57|      67|   IND| LAS|    1591|     3|      7|        0|            null|       0|`<br>
# MAGIC `|    1|         3|        4|    754|    940|           WN|     1144| N778SW|        226|    205|     -15|       9|   IND| PHX|    1489|     5|     16|        0|            null|       0|`<br>
# MAGIC `...`<br>
# MAGIC )

# COMMAND ----------

# your scenario 1 code here
flights_schema = StructType([
  StructField('month', IntegerType()),
  StructField('dayofmonth', IntegerType()),
  StructField('dayofweek', IntegerType()),
  StructField('deptime', IntegerType()),
  StructField('arttime', IntegerType()),
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
  StructField('diverted', IntegerType()),
  ])

flights = spark.read.csv(
    'wasbs://downloads@smithbc.blob.core.windows.net/flights/flights.csv', 
    schema=flights_schema
    )

flights.show()
#display(flights)

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

## Do this first to force Spark to infer a schema, then use that data as a starting point for your own schema definition
#flights = spark.read.csv('wasbs://downloads@smithbc.blob.core.windows.net/flights/flights.csv', inferSchema=True)
#flights.printSchema()

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

#flights.show()
display(flights)