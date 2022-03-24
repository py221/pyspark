# Databricks notebook source
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
  
# read ufo sightings to assemble (year, 1) pairs
sightings = (
  sc.textFile( '/FileStore/tables/rm/710838/ufo_awesome.tsv' )
    .map( splitSighting )
    .filter(isGoodSighting )
    .map( lambda fields: (fields[0][:4], fields) )
  )

# COMMAND ----------

# MAGIC %md
# MAGIC #### PAIR RDD METHODS

# COMMAND ----------

sightings.countByKey()

# COMMAND ----------

# rdd = {(1, 2), (3, 4), (3, 6)} other = {(3, 9)}

sightings.reduceByKey(lambda x: x)
sightings.mapValues(lambda x: x)
sightings.flatMapValues(lambda x: 'x to x+5') # {{(1, 2), (1, 3), (1, 4), (1, 5), (3, 4), (3, 5)}
sightings.sortByKey(lambda x: x) # {(1, 2), (3, 4), (3, 6)}

sightings.substractByKey('<other_rdd_w_key_desired_to_remove') # {(1, 2)}
sightings.join('<other_rdd') # ... {(keys, (value_1, val 2))}
sightings.rightOUterJoin('<other_rdd>') # {(3,(Some(4),9)), (3,(Some(6),9))}
sightings.leftOuterJoin('<other_rdd>') # {(1,(2,None)), (3,(4,Some(9))), (3,(6,Some(9)))}
sightings.cogroup('<other_rdd>') #... {(1,([2],[])), (3,([4, 6],[9]))}

# COMMAND ----------

sightings = (
  sc.textFile( '/FileStore/tables/rm/710838/ufo_awesome.tsv' )
    .map( splitSighting )
    .filter(isGoodSighting )
    .map( lambda fields: (fields[0][:4], fields) )
  )

(
  sightings.mapValues(lambda fields: 1)
    .reduceByKey(lambda value, accum: value + accum)
    .collectAsMap()
)

# COMMAND ----------


