# Databricks notebook source
# MAGIC %md 
# MAGIC ###DataFrames: Querying DataFrames
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to teach you how to query Spark DataFrames.

# COMMAND ----------

# MAGIC %md In the last lab, we discussed the purpose of creating a Spark dataframe is to enable higher-level interactions with the data.  In this lab, we will explore the various ways this functionality can be employed.
# MAGIC 
# MAGIC Let's start by first defining a dataframe for our UFO sighting dataset:

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

# MAGIC %md If you learned SQL, you might notice the schema definition in the last cell looks a lot like the schema definition we might employ in a SQL CREATE TABLE statement. If we think of our dataframe as something like a table in a database, then we might expect that we can query it in a manner much like a table:

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

results = (
  sightings
    .where('date_sighted >= \'2000-01-01\' AND INSTR(description, \'Dallas\') >= 0')
    .select('date_sighted', 'description')
    .orderBy('date_sighted', ascending=[0])
  )

#results.show(10)
display(results)

# COMMAND ----------

# MAGIC %md The previous cell is an example of the Programmatic SQL API exposed by the Spark dataframe.  Nearly every clause in a SQL SELECT statement is aligned with a dataframe method call, making the API easy to employ by those familiar with the SQL language. Probably the hardest part of working with the API is understanding the various ways fields in the dataframe can be referenced.  For example, here is the same query from the previous cell rewritten to use a more object-oriented format:

# COMMAND ----------

from pyspark.sql.functions import instr

results = (
  sightings
    .where( (sightings.date_sighted >= '2000-01-01') & (instr(sightings.description, 'Dallas') >= 0) )
    .select(sightings.date_sighted, sightings.description)
    .orderBy(sightings.date_sighted, ascending=[0])
  )

results.show(10)
#display(results)

# COMMAND ----------

# MAGIC %md A few things to note between this style and the previous.  When using the first style, the SQL blocks need to be encoded exactly as they would in the SELECT clause aligned with the method call, *e.g.* the string in the **where()** method call must look like a valid SQL WHERE clause.  When using the second style, you must import any functions you wish to employ (and the names of these may not perfectly align with the corresponding function in the SQL language). (You can review the available SQL function-aligned method [here](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions).) You must also use **&**, **|** and **~** as the AND, OR and NOT Boolean operators, respectively, in your WHERE-clause equivalent and wrap each comparison operation in paranetheses when a mutli-part comparison is employed.
# MAGIC 
# MAGIC While you might elect to focus your development on one style or the other, there are situations within which it's easiest to bounce between the two styles so take a little time to familiarize yourself with each.

# COMMAND ----------

# MAGIC %md Let's continue our exploration of the Programmatic SQL API by deriving some new fields for our dataframe.  Here, we will use the **datediff()** SQL function to create a field named *days_to_report*. The **withColumn()** method call creates and appends this field to our dataframe:

# COMMAND ----------

from pyspark.sql.functions import datediff

results = (
  sightings
    .where( (sightings.date_sighted >= '2000-01-01') & (instr(sightings.description, 'Dallas') >= 0) )
    .withColumn( 'days_to_report', datediff(sightings.date_reported, sightings.date_sighted) )
    .select(sightings.date_sighted, sightings.date_reported, 'days_to_report', sightings.description)
    .orderBy(sightings.date_sighted, ascending=[0])
  )

#results.show(10)
display(results)

# COMMAND ----------

# MAGIC %md Did you notice in the last cell that the **select()** method retrieved most fields using the object-oriented field notation while *days_to_report* was retrieved using a string?  Because we haven't yet assigned the *days_to_report* field to a dataframe variable, there's no way to call it using an object-oriented notation.  Until the dataframe is assigned to a new variable, string-based references are the only way to access this field.

# COMMAND ----------

# MAGIC %md As the complexity of our logic increases, the complexity of our API calls does as well.  In this next cell, we will look at the average number of *days_to_report* on an annual basis.  The grouping and aggregation of data using the API is one example where the complexity of these calls jumps quite a bit:

# COMMAND ----------

from pyspark.sql.functions import year

results = (
  sightings
    .where( (sightings.date_sighted >= '2000-01-01') & (instr(sightings.description, 'Dallas') >= 0) )
    .withColumn( 'days_to_report', datediff(sightings.date_reported, sightings.date_sighted) )
    .withColumn( 'year', year(sightings.date_sighted) )
    .select('year', 'days_to_report' )
    .groupby('year').avg('days_to_report')
    .withColumnRenamed( 'avg(days_to_report)', 'avg_days_to_report')
    .orderBy('year', ascending=[1])
  )

results.show(10)
#display(results)

# COMMAND ----------

# MAGIC %md Let's add some more aggregations to the logic to raise the complexity just a bit more:

# COMMAND ----------

results = (
  sightings
    .where( (sightings.date_sighted >= '2000-01-01') & (instr(sightings.description, 'Dallas') >= 0) )
    .withColumn( 'days_to_report', datediff(sightings.date_reported, sightings.date_sighted) )
    .withColumn( 'year', year(sightings.date_sighted) )
    .select('year', 'days_to_report' )
    .groupby('year').agg( {'*':'count', 'days_to_report':'avg'} )
    .withColumnRenamed('avg(days_to_report)', 'avg_days_to_report')
    .withColumnRenamed('count(1)', 'sightings')
    .orderBy('year', ascending=[1])
  )

results.show(10)
#display(results)

# COMMAND ----------

# MAGIC %md If at some point you feel it would be easier to just write a SQL statement, then you are in luck.  The whole point of providing a programmatic SQL API is to enable Spark to present dataframes as table-like objects against which analyst's can employ SQL SELECT statements. The trick to doing this is to *pin* your dataframe object to the SQL catalog as either a permanent or a temporary object.  Here, we'll do the latter:

# COMMAND ----------

sightings.createOrReplaceTempView('sightings')

# COMMAND ----------

# MAGIC %md With the dataframe *pinned* as a temporary view named *sightings*, we can now query it using SQL:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   x.year,
# MAGIC   COUNT(*) as sightings,
# MAGIC   AVG(x.days_to_report) as avg_days_to_report
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     YEAR(date_sighted) as year,
# MAGIC     DATEDIFF(date_reported, date_sighted) as days_to_report
# MAGIC   FROM sightings
# MAGIC   WHERE date_sighted >= '2000-01-01' AND INSTR(description, 'Dallas') >= 0
# MAGIC   ) x
# MAGIC GROUP BY x.year
# MAGIC ORDER BY x.year

# COMMAND ----------

# MAGIC %md 
# MAGIC It's important to note that when using SQL SELECT statements, you do not need to import any functions from the pyspark.sql.functions.  Instead, you simply employ SQL syntax, just as it's implimented in Hive.  Hive SQL can be a little tricky if you are accustomed to traditional RDBMS SQL syntax, so here is a link to the Hive SQL [SELECT](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select) and [function/operators](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF) references.
# MAGIC 
# MAGIC In the last call, we submitted our SELECT statement to the SparkSQL engine using a SQL magic command, *i.e.* **%sql**, which spills the results to the screen. There's no reason we can't capture the results as a dataframe with a slightly different call against the **spark** session variable:

# COMMAND ----------

query = '''
SELECT
  x.year,
  COUNT(*) as sightings,
  AVG(x.days_to_report) as avg_days_to_report
FROM (
  SELECT
    YEAR(date_sighted) as year,
    DATEDIFF(date_reported, date_sighted) as days_to_report
  FROM sightings
  WHERE date_sighted >= '2000-01-01' AND INSTR(description, 'Dallas') >= 0
  ) x
GROUP BY x.year
ORDER BY x.year
'''

results = spark.sql(query)

#results.show(10)
display(results)

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC Define a dataframe for the flight dataset at wasbs://downloads@smithbc.blob.core.windows.net/flights/flights.csv and the airport dataset at wasbs://downloads@smithbc.blob.core.windows.net/flights/airports.csv. Refer to previous labs to determine the schema for each of these files.
# MAGIC 
# MAGIC Using a SQL SELECT statement, identify the destination airports with the most arrivals (for the period represented by the flight dataset).  Provide a friendly name for the destination airport.
# MAGIC <br>(Answer: First record returned is ATL | WilliamBHartsfield-AtlantaIntl | 122096)
# MAGIC <br>(Hint: You may find it easiest to break your code into multiple cells so that you can employ the %sql magic command for the query.)

# COMMAND ----------

# your scenario 1 code here
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

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

#%sql --your scenario 1 query goes here

top_airport_query = '''
SELECT 
  x.dest AS DestinationCode,
  y.name AS Destination,
  count(*) AS TotalFlights
FROM flights x
LEFT OUTER JOIN airports y
  ON x.dest = y.code
GROUP BY 
  x.dest, y.name
ORDER BY 
  TotalFlights DESC
LIMIT 1
'''

top_flight_airport = spark.sql(top_airport_query)

top_flight_airport.show()

# COMMAND ----------

# MAGIC %md ####Scenario 2
# MAGIC 
# MAGIC Using the flights and airport dataframes defined in Scenario 1, identify the destination airports with the most arrivals (for the period represented by the flight dataset), providing a friendly name for the destination airport.  This is the same query as Scenario 1 but the challenge is to generate the result set using the programmatic SQL API.<br>
# MAGIC (Answer: Same as with previous scenario)<br>
# MAGIC (Hint: You will need to use the **join()** method which is addressed in the online documentation.)

# COMMAND ----------

# your scenario 2 code here
top_flight_airport_prgrmAPI = (
  flights
    .join(airports, flights.dest == airports.code, 'leftouter')
    .select(flights.dest, airports.name)
    .groupBy(flights.dest, airports.name).agg({'*':'count'})
    .withColumnRenamed('dest', 'DestCode')
    .withColumnRenamed('name', 'DestFullName')
    .withColumnRenamed('count(1)', 'TotalFlights')
    .orderBy('TotalFlights', ascending=[0])
    .limit(5)
  )

top_flight_airport_prgrmAPI.show()

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

# COMMAND ----------

# MAGIC %sql -- Scenario 1
# MAGIC 
# MAGIC SELECT
# MAGIC   a.dest as airport_code,
# MAGIC   b.name as airport_name,
# MAGIC   count(*) as arriving_flights
# MAGIC FROM flights a
# MAGIC LEFT OUTER JOIN airports b
# MAGIC   ON a.dest=b.code
# MAGIC GROUP BY a.dest, b.name
# MAGIC ORDER BY arriving_flights DESC

# COMMAND ----------

# Scenario 2
from pyspark.sql.functions import count

results = (
  flights
    .groupby('dest').agg( count('*') ) # OR .groupby('dest').agg({'*':'count'})
    .join( airports, flights.dest==airports.code, 'left_outer' )
    .withColumnRenamed('dest', 'airport_code')
    .withColumnRenamed('name', 'airport_name')
    .withColumnRenamed( 'count(1)', 'arriving_flights' )
    .select( 'airport_code', 'airport_name', 'arriving_flights' )
    .orderBy( 'arriving_flights', ascending=[0] )
  )

display(results)

# COMMAND ----------

