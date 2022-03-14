# Databricks notebook source
# MAGIC %md 
# MAGIC ###PairRDDs: Pair RDD Methods & Actions
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to teach you how to transform data in a pair RDD.

# COMMAND ----------

# MAGIC %md In the last lab, we explained that pair RDDs open up a set of additional transformations that depend on the key-value pair structure of the elements in the RDD. Let's assemble a pair RDD and then take a look at some of these transformations:

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
  
# read ufo sightings to assemble (year, 1) pairs
sightings = (
  sc.textFile( 'wasbs://downloads@smithbc.blob.core.windows.net/ufo/ufo_awesome.tsv' )
    .map( splitSighting )
    .filter(isGoodSighting )
    .map( lambda fields: (fields[0][:4], fields) )
  )

# COMMAND ----------

# MAGIC %md We now have a pair RDD for our dataset. If we recognize that each line in of data in this RDD represents a UFO sighting and the first field in each line is the date of the sighting in the format YYYY-MM-DD, then we should see that we now have a pair RDD within which the key is the year the sighing occured and the value is the sighting data. Exploiting this structure, let's obtain a count of UFO sightings by year:

# COMMAND ----------

sightings.countByKey()

# COMMAND ----------

# MAGIC %md Disregarding for a bit that our years may be out of sequential order and that the output of the action is a dictionary, let's consider what Spark has done for us.  By organizing the data in our elements into key-value pairs, we can now ask Spark to perform a count operation across the keys in the RDD. When we call the **countByKey()** action, Spark reorganizes the elements in the RDD so that all elements with the same keys (year) are in the same partition.  It then can iterate over the elements with the same keys, counting each occurance to produce a count by key. 
# MAGIC 
# MAGIC To better understand exactly what Spark is doing under the covers, let's decompose the **countByKey()** action into some lower level transformation and action calls:

# COMMAND ----------

sightings = (
  sc.textFile( 'wasbs://downloads@smithbc.blob.core.windows.net/ufo/ufo_awesome.tsv' )
    .map( splitSighting )
    .filter(isGoodSighting )
    .map( lambda fields: (fields[0][:4], fields) )
  )

(
  sightings.mapValues(lambda fields: 1)
    #.reduceByKey(lambda value, accum: value + accum)
    .collectAsMap()
)

# COMMAND ----------

# MAGIC %md In the **mapValues()** call, a map transform is applied against the values portion of the key-values pair, replacing the values with the integer 1. The **reduceByKey()** transformation is then called to first re-organize the elements around shared keys and then iterate over the elements associated with each key, adding the integer 1 value associated with each element to the accumulating total for each key. (The **reduceByKey()** transformation is slightly more efficient than this but it's not important to go into that detail at this time.) **collectAsMap()** is then called to return the results to the screen as a dictionary, mimicing the results of the **countByKey()** action. (If our goal was simply return items as (year, count) tuples, we would simply call **collect()**.)

# COMMAND ----------

# MAGIC %md As you can see in the last two examples, there are numerous transformations available in Spark which operate on key-value pair elements.  Most of these focus on bringing together the elements of an RDD that share a common key (as indicated by the **ByKey** prefix on the method name). When thinking about how we might tackle a data processing challenge with a pair RDD, we typically think about assigning values to keys around which we might group our data for aggregation (as demonstrated above) or otherwise sort or join our data.  Let's see some more examples of these specific patterns, starting with a sort:

# COMMAND ----------

(
  sightings.mapValues(lambda fields: 1)
    .reduceByKey(lambda value, accum: value + accum)
    .sortByKey(ascending=False)
).take(10)

# COMMAND ----------

# MAGIC %md Here, we've returned to our previous count-by-key logic. Using **sortByKey()** we now have a result set sorted by year in descending order.

# COMMAND ----------

# MAGIC %md To look at joins, let's reorganize our sighting data, considering each word in the description provided with the sighting. To assist us, we'll first define a function which strips all punctuation marks from our sighting description:
# MAGIC 
# MAGIC **NOTE**: The use of the **maketrans()** method (of the generic string object) is a little advanced.  In a nutshell, that method call converts one set of strings into another.  In this case, we've set both of these to an empty string so that we are not doing any conversion.  The last arguement specifies a list of values to simply remove.  We've set this to the standard list of punctuation values associated with strings.

# COMMAND ----------

import string

def removePunctuation( description ):
  '''
  remove all punctuation marks from the provided description string variable
  '''
  translator = str.maketrans('', '', string.punctuation)
  return description.translate(translator) 

# COMMAND ----------

words = (
  sc.textFile( 'wasbs://downloads@smithbc.blob.core.windows.net/ufo/ufo_awesome.tsv' )
    .map( splitSighting )
    .filter(isGoodSighting )
    .map( lambda fields: removePunctuation( fields[-1].strip().lower() ) )
    .flatMap( lambda description: description.split(' ') )
  )

words.take(10)

# COMMAND ----------

# MAGIC %md In the previous cell, we read our data, split it and filter out bad records like we've done many times before.  We then use the **map()** transformation to remove punctuation from the description field.  In that same call, we are removing any leading or trailing white space from the description and setting all it's characters to lower-case.  We then then apply the **flatMap()** transformation to the now cleansed description which has been split on the occurance of a space character (which is typically used to separate words). The **flatMap()** transformation differs from the **map()** transformation in that it applies a function to each element in the incoming RDD.  If that function returns a list (as the **split()** method does), each item in the list becomes its own element in the resulting RDD.  You can think of this as something a bit like an *unpivot* operation.  
# MAGIC 
# MAGIC To illustrate this, let's say we had two incoming cleansed descriptions like you see here:
# MAGIC 
# MAGIC `'i saw a ufo'`
# MAGIC 
# MAGIC `'he saw a ufo too'`
# MAGIC 
# MAGIC the **split()** call in the **flatMap()** transformation would produce lists like these:
# MAGIC 
# MAGIC `['i', 'saw', 'a', 'ufo']`
# MAGIC 
# MAGIC `['he', 'saw', 'a', 'ufo', 'too']`
# MAGIC 
# MAGIC and them flattening/unpivot logic in **flatMap()** would then produce an RDD with these elements:
# MAGIC 
# MAGIC `['i']`
# MAGIC 
# MAGIC `['saw']`
# MAGIC 
# MAGIC `['a']`
# MAGIC 
# MAGIC `['ufo']`
# MAGIC 
# MAGIC `['he']`
# MAGIC 
# MAGIC `['saw']`
# MAGIC 
# MAGIC `['a']`
# MAGIC 
# MAGIC `['ufo']`
# MAGIC 
# MAGIC `['too']`

# COMMAND ----------

# MAGIC %md With our data organized like this, we can now get a count of unique words that pop up in our sighting descriptions. Like before, we can organize our elements as key-value pairs with the word being the key and an integer value of 1 serving as the value.  And from there we can either use **reduceByKey()** or **countByKey()** to perform the count and a **sort()** or **sortByKey()** to show the most frequently occuring words: 

# COMMAND ----------

(
  words
    .map(lambda word: (word, 1) )
    .reduceByKey( lambda v, a: v + a)
    .map(lambda kv: (kv[1], kv[0]))
    .sortByKey(ascending=False)
).take(10)

# COMMAND ----------

# MAGIC %md The results show us that words like 'the', 'a', and 'i' that frequently occur in the English language are the most frequently occuring in our data set.  These are words we typically want to ignore in word count exercises such as this.  So let's bring in a list of *words to ignore* and exclude these from our analysis: 

# COMMAND ----------

ignore_words = sc.textFile( 'wasbs://downloads@smithbc.blob.core.windows.net/ignore_words/' ).map(lambda x: (x, 1))
ignore_words.take(5)

# COMMAND ----------

# MAGIC %md By organizing both the list of words in the descriptions and the words to ignore in key-value pairs with the word part in each used as the key, Spark can perform a join between the RDDs. Spark supports inner and outer join patterns with the **innerJoin()**, **leftOuterJoin()**, **rightOuterJoin()**, and **fullOuterJoin()** transformations.  The result of the join is a new key-value pair where the key is the key on which the join was performed and the value is a two-part tuple representing the value of the left-hand and the value of the right-hand RDDs that participated in the join:

# COMMAND ----------

joined = (
  words.map( lambda word: (word, 1) )
    .leftOuterJoin( ignore_words)
  )

joined.take(20)

# COMMAND ----------

# MAGIC %md Looking at the results of the **leftOuterJoin()** transformation, you can see the key represents the key on the left-hand side of the join, *i.e.* the words RDD.  The value is a tuple with values from the left-hand (description words) RDD and then the values from the right-hand (ignore words) RDD.  In the case that there was a match, we should expect a values tuple of (1, 1). When no match occurs, we should expect to see a values tuple of (1, None).
# MAGIC 
# MAGIC We can now complete our analysis, removing matches to words in the ignore words list:

# COMMAND ----------

joined = (
  words.map( lambda word: (word, 1) )
    .leftOuterJoin( ignore_words)
    .filter( lambda kv: kv[1][1] is None ) # find unmatched words, i.e. words not in the ignore list
    .map( lambda kv: (kv[0], 1) ) # restructure as (word, 1)
  )

(
  joined
    .reduceByKey( lambda v, a: v+a )
    .map( lambda kv: (kv[1], kv[0]) )
    .sortByKey(ascending=False)
  ).take(10)


# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC The **wasbs://downloads@smithbc.blob.core.windows.net/nyse/** directory contains a series of text files, each line of which provides daily summary statistics for a given stock on the New York Stock Exchange for each day between 1962 and some time in 2010. 
# MAGIC 
# MAGIC Read the contents of this directory into an RDD. Split each line based on a comma-delimiter.  Knowing that the third field on each line is a date string in the format YYYY-MM-DD and the second field is the stock symbol.  Assemble this data as a pair RDD with the tuple (year,stock symbol) as its key and the integer 1 as its value. Sum the values for each key to determine the number of days in each year that a stock was traded. Sort the data by year and then stock symbol. (Answer: `[(('1962', 'AA'), 252),
# MAGIC  (('1962', 'BA'), 252),
# MAGIC  (('1962', 'CAT'), 252),
# MAGIC  (('1962', 'DD'), 252),
# MAGIC  (('1962', 'DIS'), 252) .... ]`
# MAGIC  )

# COMMAND ----------

# your scenario 1 code here

nyse = sc.textFile('wasbs://downloads@smithbc.blob.core.windows.net/nyse/')

stock_freq = (
    nyse.map(lambda line: line.split(','))
      .filter(lambda str_list: str_list[2][:4] != 'date')
      .map(lambda str_list: (str_list[2][:4], str_list[1]))
      .map(lambda pair_key: (pair_key, 1))
      .reduceByKey(lambda value, accum: value+accum)
      .sortByKey(ascending = True)
    )

stock_freq.take(5)

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
pricing_pair = pricing.map( lambda fields: ( (fields[2][:4], fields[1]), 1 ) )

days_traded = pricing_pair.reduceByKey( lambda v, a: v+a).sortByKey(ascending=True)
days_traded.take(10)