# Databricks notebook source
# MAGIC %md Run this notebook one time before attempting the remaining labs.  This will setup the required data assets for the labs in Class 4:

# COMMAND ----------

# setup weblog files
try:
  dbutils.fs.rm('/tmp/weblogs', recurse=True)
except:
  pass

dbutils.fs.mkdirs('/tmp/weblogs/old')
dbutils.fs.cp('wasbs://downloads@smithbc.blob.core.windows.net/weblogs/old/access.log', '/tmp/weblogs/old/access.log')

dbutils.fs.mkdirs('/tmp/weblogs/new')
dbutils.fs.cp('wasbs://downloads@smithbc.blob.core.windows.net/weblogs/new/u_ex181031_x.log', '/tmp/weblogs/new/u_ex181031_x.log')

# COMMAND ----------

# MAGIC %fs ls dbfs:/tmp/weblogs

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/tmp/weblogs"))

# COMMAND ----------

try:
  dbutils.fs.rm('/tmp/nyse', recurse=True)
except:
  pass

dbutils.fs.cp('wasbs://downloads@smithbc.blob.core.windows.net/nyse/', '/tmp/nyse/', recurse=True)

# COMMAND ----------

# MAGIC %fs ls dbfs:/tmp/nyse

# COMMAND ----------

