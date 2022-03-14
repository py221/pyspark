# Databricks notebook source
# MAGIC %md ###General Instructions
# MAGIC In this assignment, you will need to complete the code samples where indicated to accomplish the given objectives. **Be sure to run all cells** and export this notebook as an HTML with results included.  Upload the exported HTML file to Canvas by the assignment deadline.

# COMMAND ----------

# MAGIC %md ####Assignment
# MAGIC Complete the following python script per the instructions provided at the top of each code block. Look for the 
# MAGIC *# MODIFY THIS LINE* comment to indicate where you need to make code modifications. Do not add or remove any lines to this code. Everything should be able to be performed with the provided number of code lines.

# COMMAND ----------

from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# set directory variables
input_dir_name = 'wasbs://downloads@smithbc.blob.core.windows.net/nyse/'
output_dir_name = '/tmp/ibm_highest_1990s'

# COMMAND ----------

# define a schema for the nyse pricing data
nyse_schema = StructType([
  StructField('exchange', StringType()),
  StructField('stock_symbol', StringType()),
  StructField('date', DateType()),
  StructField('stock_price_open', FloatType()),
  StructField('stock_price_high', FloatType()),
  StructField('stock_price_low', FloatType()),
  StructField('stock_price_close', FloatType()),
  StructField('stock_volume', IntegerType()),
  StructField('stock_price_adj_close', FloatType())
  ])

# load the nyse pricing data into a dataframe applying the schema 
# created in the previous step
df = spark.createDataFrame(
  spark.read.csv(input_dir_name)
    .map(lambda ln: ln.split(','))
    .filter(lambda lst: lst[1] != 'stock_symbol'),
  schema = nyse_schema
  )

#df.show(1)

# using the programmatic sql api, add a field named year to your dataframe which 
# will be assigned the year value from the date field
from pyspark.sql.functions import year, max

df2 = df.withColumn("year", year(df.date))
df2.show(5)

# COMMAND ----------

# question 1, between 1990 and 1999, what was the highest closing price for IBM stock
# across any year? use the programmatic sql api to construct this result.
results1 = (
  df2
    .where((df2.year >= '1990') & (df2.year <= '1999') & (df2.stock_symbol == 'IBM'))
    .select(df2.year, df2.stock_price_close)
    .groupBy(df2.year).max('stock_price_close')
    .withColumnRenamed('max(stock_price_close)', 'max_price_close')
    .orderBy('max_price_close', ascending=[0])
    .limit(1)
)

results1.show()

# COMMAND ----------

# question 2, between 1990 and 1999, what was the highest closing price for IBM stock
# by year? display your answer on the screen with data ordered by year in ascending order
# use a SQL statement to construct this result

df2.createOrReplaceTempView('pricing')

sql_statement = '''
SELECT 
  year,
  max(stock_price_close) as max_price_close
FROM pricing
WHERE 
 (year BETWEEN 1990 AND 1999)
  AND (stock_symbol = 'IBM')
GROUP BY
  year
ORDER BY 
  year ASC
''' 

results2 =  spark.sql(sql_statement)

results2.show()

# COMMAND ----------

# delete output dir
dbutils.fs.rm(output_dir_name, recurse=True)

# COMMAND ----------

spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY")

# COMMAND ----------

# save your results from question 2 to a new directory named homework_out under 
# your /tmp/imb_highest_1990s directory in the parquet format
results1.write.parquet(
  '/tmp/ibm_highest_1990s/homework_out',
  mode = 'Overwrite'
  )

# COMMAND ----------

# display output files
display( dbutils.fs.ls(output_dir_name) )

# COMMAND ----------

# MAGIC %fs ls /tmp/ibm_highest_1990s/homework_out/