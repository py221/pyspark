# Databricks notebook source
# MAGIC %md Run this notebook to load data assets for the labs in this section:

# COMMAND ----------

try:
  dbutils.fs.rm('/tmp/json/profiles/', recurse=True)
except:
  pass

dbutils.fs.cp('wasbs://downloads@smithbc.blob.core.windows.net/profiles_json/profiles.json','/tmp/json/profiles/profiles.json')

# COMMAND ----------

try:
  dbutils.fs.rm('/tmp/json/reviews/', recurse=True)
except:
  pass

dbutils.fs.cp('wasbs://downloads@smithbc.blob.core.windows.net/reviews/reviews.json','/tmp/json/reviews/reviews.json')

# COMMAND ----------

