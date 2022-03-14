# Databricks notebook source
# MAGIC %md ###General Instructions
# MAGIC In this assignment, you will need to complete the code samples where indicated to accomplish the given objectives. **Be sure to run all cells** and export this notebook as an HTML with results included.  Upload the exported HTML file to Canvas by the assignment deadline.

# COMMAND ----------

# MAGIC %md ####Assignment
# MAGIC Unlike previous exercises, you will not be provided any sample code from which to work.  You will be given some very high-level instructions and are expected to figure out a solution from there.

# COMMAND ----------

# MAGIC %md Rick & Morty is a popular animated series so, of course, it has it's own API. Using https://rickandmortyapi.com/api/character/ as a data source, load the API data into a temporary view named *characters* and answer the question, In which episode does each character listed in the JSON returned by the API first appear.
# MAGIC 
# MAGIC When you've completed your work, run the three SQL statements at the bottom of this notebook to assist with grading.

# COMMAND ----------

# retrieve the JSON to an RDD and convert to a DataFrame
from urllib import request as url 
from pyspark.sql.functions import *
from pyspark.sql.types import *

response = url.urlopen(url.Request('https://rickandmortyapi.com/api/character/'))

rdd_rm = sc.parallelize([response.read().decode('utf-8').strip()])

characters = spark.read.json(rdd_rm)

# COMMAND ----------

# register the DataFrame as a temporary view named characters
characters.createOrReplaceTempView("characters")

# COMMAND ----------

# answer the question using either programmatic SQL API or a SQL query  

first_eps = (
  characters
    .select(explode(characters.results).alias('char'))
    .withColumn('episode_link', explode('char.episode'))
    .withColumn('episode_str', regexp_extract("episode_link", "\\d+", 0))
    .withColumn('episode', col('episode_str').cast('int'))
    .select('char.name', 'episode')
    .groupBy('name').agg({'episode':'min'})
    .withColumnRenamed('min(episode)', 'first_appeared_episode')
    .orderBy('first_appeared_episode')
  )

first_eps.write.saveAsTable(
  'character',
  partitionBy='first_appeared_episode',
  mode='overwrite',
  format='parquet'
  )

display(first_eps)


# COMMAND ----------

# MAGIC %md Execute the following SQL statements to validate your results:

# COMMAND ----------

# MAGIC %sql -- verify row count
# MAGIC 
# MAGIC SELECT COUNT(*) FROM character;

# COMMAND ----------

# MAGIC %sql -- verify values
# MAGIC 
# MAGIC SELECT * FROM character;

# COMMAND ----------

# MAGIC %sql -- verify partitioning
# MAGIC DESCRIBE EXTENDED character;

# COMMAND ----------

