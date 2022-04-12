# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### rdd w sc.range()

# COMMAND ----------

import numpy as np
values = sc.range(0, 10000000)
print(type(values))

# COMMAND ----------

values.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### rdd w sc.parallelize(<lst_obj>)

# COMMAND ----------

lst = ['p1', 'p2', 'p3', 1, 2, 3, {'p4': 4}]
rdd_parallelize = sc.parallelize(lst)
rdd_parallelize.map(lambda item: type(item)).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC #### rdd w sc.textfile

# COMMAND ----------

# File location and type
ufo_rdd = sc.textFile("/FileStore/tables/rm/710838/ufo_awesome.tsv", minPartitions = sc.defaultParallelism)

print(ufo_rdd.count())

print(ufo_rdd.getNumPartitions())

print(ufo_rdd.take(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #### df w spark.read.load()

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/rm/710838/ufo_awesome.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "false"
delimiter = "\t"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

type(df)

# COMMAND ----------


