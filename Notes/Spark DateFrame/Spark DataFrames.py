# Databricks notebook source
# MAGIC %md
# MAGIC #### spark.read.format -- creating Spark DataFrames

# COMMAND ----------

from pyspark.sql.types import *


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

# COMMAND ----------

# MAGIC %md 
# MAGIC # Schema
# MAGIC 
# MAGIC A Spark dataframe's schema is defined using SparkSQL data types, imported here using the **pyspark.sql.types** library.  This library contains many scalar data types including:
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

# COMMAND ----------

print(df.printSchema())
df.__class__

# COMMAND ----------

rdd = df.rdd

# COMMAND ----------

rdd_to_df = spark.createDataFrame(
  rdd,
  schema = udf_schema
)
rdd_to_df.__class__

# COMMAND ----------


