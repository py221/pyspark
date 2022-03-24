# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import instr
from pyspark.sql.functions import year
from pyspark.sql.functions import datediff

spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


udf_schema = StructType([
  StructField('date_sighted', DateType()),
  StructField('date_reported', DateType()),
  StructField('location', StringType()),
  StructField('shape', StringType()),
  StructField('duration', StringType()),
  StructField('description', StringType())
  ])


# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format("csv") \
  .option("inferSchema", False) \
  .option("header", False) \
  .option("sep", '\t') \
  .option("dateFormat", "yyyyMMdd") \
  .schema(udf_schema) \
  .load("/FileStore/tables/rm/710838/ufo_awesome.csv")

display(df)

# -----------------------------------------------------------------------------------------
# orrr

sightings_schema=StructType([
    StructField('date_sighted', DateType()),
    StructField('date_reported', DateType()),
    StructField('location', StringType()),
    StructField('shape', StringType()),
    StructField('duration', StringType()),
    StructField('description', StringType())
    ])

sightings = spark.read.csv(
    '/FileStore/tables/rm/710838/ufo_awesome.csv',
    sep='\t', 
    schema=sightings_schema,
    dateFormat='yyyyMMdd')

#sightings.show(10)
display(sightings)

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



# COMMAND ----------

# MAGIC %md 
# MAGIC # createOrReplaceTempView 

# COMMAND ----------

df.createOrReplaceTempView('df')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df where shape is not null

# COMMAND ----------



# COMMAND ----------


