# Databricks notebook source
# MAGIC %md
# MAGIC #### PAIR RDD

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

# COMMAND ----------

sightings = sc.textFile( '/FileStore/tables/rm/710838/ufo_awesome.tsv' )
split = sightings.map( splitSighting )
filtered = split.filter(isGoodSighting )

filtered.take(1)

# COMMAND ----------

# MAGIC %md
# MAGIC <i> PAIR RDD </i> as tuple format 

# COMMAND ----------

pair = filtered.map( lambda fields: (fields[0][:4], fields) )
pair.take(1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### PAIR RDD <b>keys / values</b>

# COMMAND ----------

pair.keys().take(1)

# COMMAND ----------

pair.values().take(1)

# COMMAND ----------

# generate an rdd with values 0 to row_count (exclusive) that's partitioned similarly to our original RDD
row_count = pair.count()
partition_count = pair.getNumPartitions()
row_number = sc.range(0, row_count, numSlices = partition_count)

# merge the two RDDs to form a new key-value pair structure
id_pair = row_number.zip( pair )
id_pair.take(1)

# COMMAND ----------


