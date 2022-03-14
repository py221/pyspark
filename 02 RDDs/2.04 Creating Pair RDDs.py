# Databricks notebook source
# MAGIC %md 
# MAGIC ###PairRDDs: Creating Pair RDDs
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to teach you how to create pair RDDs.

# COMMAND ----------

# MAGIC %md The flexibility to load just about any kind of data into an RDD is incredibly powerful when working with messy data.  But the lack of a defined structure for the elements in an RDD also makes it difficult for Spark to present higher-level transformations and actions which may make solving these problems easier for you as a developer.
# MAGIC 
# MAGIC To bridge this gap, Spark enables the definition of a special RDD called a pair RDD which employs a simple key-value pair pattern for each the element.  When elements in an RDD have been structured in this manner, Spark can make available a suprisingly diverse array of transformations and actions available to you.  
# MAGIC 
# MAGIC In the next lab, we'll take a look at some of these transformations and actions, but in this lab, we'll focus on getting familiar with the creation of pair RDDs.

# COMMAND ----------

# MAGIC %md To get started, let's return to the data set we worked with in the lab:

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

sightings = sc.textFile( 'wasbs://downloads@smithbc.blob.core.windows.net/ufo/ufo_awesome.tsv' )
split = sightings.map( splitSighting )
filtered = split.filter(isGoodSighting )

filtered.take(1)

# COMMAND ----------

# MAGIC %md Each element in the filtered RDD a list.  That list consists of 6 items parsed from the original line of data read from the targetted file and filtered to ensure they meet some minimal requirements for data quality.  But even though each element in the RDD is a 6-item list, Spark is aware of the element as having a single value of type list.
# MAGIC 
# MAGIC But we can restructure the elements in the RDD, making Spark aware that each element is a two-part tuple: 

# COMMAND ----------

pair = filtered.map( lambda fields: (fields[0][:4], fields) )
pair.take(1)

# COMMAND ----------

# MAGIC %md It's a simple transformation.  We apply the **map()** transformation and use it to generate a two-value tuple, the first item of which is the first four characters of the first field in the split line. The second item is the list representing the entire split line.
# MAGIC 
# MAGIC While simple, this modification makes Spark see the RDD differently.  Compare the types assigned to the *filtered* and the *pair* RDD variables:

# COMMAND ----------

print( type(filtered) )
print( type(pair) )

# COMMAND ----------

# MAGIC %md When we structure the elements of an RDD as a two-value tuple, Spark recognizes the first item in the tuple as the elements' key and the second item in the tuple as the elements' value: 

# COMMAND ----------

pair.keys().take(1)
#pair.values().take(1)

# COMMAND ----------

# MAGIC %md Applying a **map()** transformation is just one way to create a key-value pair.  Let's say we wished to index the lines in our data set so that each element received a unique row number. We could generate an RDD representing a range of values 0 through (one short of) row-count in the original RDD and zip this RDD with that original RDD using the **zip()** method, creating a pair RDD with the row/item number as the key and the original item as the value:

# COMMAND ----------

# generate an rdd with values 0 to row_count (exclusive) that's partitioned similarly to our original RDD
row_count = pair.count()
partition_count = pair.getNumPartitions()
row_number = sc.range(0, row_count, numSlices = partition_count)

# merge the two RDDs to form a new key-value pair structure
id_pair = row_number.zip( pair )
id_pair.take(1)

# COMMAND ----------

id_pair.keys().take(1)
id_pair.values().take(1)

# COMMAND ----------

# MAGIC %md **NOTE**: The indexing of elements in an RDD in the manner demonstrated here is such a common practice that a **zipWithIndex()** transformation is provided by Spark for this purpose.  Using that method, our code gets simplified as: `id_pair = pair.zipWithIndex()`

# COMMAND ----------

# MAGIC %md In the last example, we created a pair RDD with an integer key and a two-part tuple as its value.  In other words, we've created a key-value within a key-value pair. Why would someone want to nest key-value pairs like this?  
# MAGIC 
# MAGIC Hopefully why such a structure makes sense will become clear as you see examples in the next lab.  For now, the critical thing to understand is that a pair RDD consists of a key and a value in a tuple.  The data type of the key and that of the value are irrelevant to Spark.  The critical thing is that the highest-level structure of the elements in the RDD represents a two-part tuple. 
# MAGIC 
# MAGIC To emphasize this point, let's convert the key into a more complex structure:

# COMMAND ----------

pair = filtered.map( lambda fields: ( (fields[0][:4], fields[4]), fields) )
pair.take(1)

# COMMAND ----------

pair.keys().take(1)
pair.values().take(1)

# COMMAND ----------

# MAGIC %md Now we have an RDD comprised of elements which are each a two-part tuple.  The key is a key-value pair.  The value is a key-value pair. There's no limit to how complex we can make our RDD elements so long as we adhere to the basic two-part structural requirement for each item:

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC The **wasbs://downloads@smithbc.blob.core.windows.net/nyse/** directory contains a series of text files, each line of which provides daily summary statistics for a given stock on the New York Stock Exchange for each day between 1962 and some time in 2010. 
# MAGIC 
# MAGIC Read the contents of this directory into an RDD. Split each line based on a comma-delimiter.  Knowing that the third field on each line is a date string in the format YYYY-MM-DD and the second field is the stock symbol.  Assemble this data as a pair RDD with the tuple (year, stock symbol) as its key and the list representing the split line of data as it's value. Return the first element in the pair RDD to the screen. (Answer: `(('1980', 'FDX'), ['NYSE',
# MAGIC   'FDX',
# MAGIC   '1980-01-02',
# MAGIC   '44.38',
# MAGIC   '46.62',
# MAGIC   '44.38',
# MAGIC   '45.00',
# MAGIC   '369600',
# MAGIC   '2.72'])` though the exact line presented will likely differ.)

# COMMAND ----------

# your scenario 1 code here

nyse = sc.textFile('wasbs://downloads@smithbc.blob.core.windows.net/nyse/')

nyse_pair = (
    nyse.map(lambda record: record.split(',')) 
    .map(lambda record: ((record[2], record[1]), record))
    )
nyse_pair.take(1)

# COMMAND ----------

# MAGIC %md ####Answers (scroll down)
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p> 
# MAGIC <br></p>

# COMMAND ----------

# scenario 1
pricing = (
  sc.textFile('wasbs://downloads@smithbc.blob.core.windows.net/nyse/')
    .map( lambda line: line.split(',') )
  )
pricing_pair = pricing.map( lambda fields: ( (fields[2][:4], fields[1]), fields ) )

pricing_pair.take(1)

# COMMAND ----------

