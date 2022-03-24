# Databricks notebook source
prepared = (
 sc.textFile( '/FileStore/tables/rm/710838/ufo_awesome.tsv' )
  .map( splitSighting )
  .filter(isGoodSighting )
  .sortBy(lambda fields: fields[0], ascending=False)
  .map( lambda fields: '\t'.join(fields) )
  )

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Detailed steps 

# COMMAND ----------

# File location and type
ufo_rdd = sc.textFile("/FileStore/tables/rm/710838/ufo_awesome.tsv", minPartitions = sc.defaultParallelism)

print(ufo_rdd.count())

print(ufo_rdd.getNumPartitions())

print(ufo_rdd.take(5))

# COMMAND ----------

split = ufo_rdd.map(lambda line: line.split('\t'))
split.take(1)

# COMMAND ----------

filtered = split.filter(lambda fields: len(fields[5].strip()) != 0)
filtered.take(2)

# COMMAND ----------

# MAGIC %%timeit
# MAGIC sorted = filtered.sortBy(lambda fields: fields[0], ascending=False)
# MAGIC sorted.take(1)

# COMMAND ----------


