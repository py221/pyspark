# Databricks notebook source
# MAGIC %md 
# MAGIC ###RDDs: Methods & Actions
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to teach you some methods and actions commonly used with RDDs.

# COMMAND ----------

# MAGIC %md The RDD represents a distributed list of elements.  When we apply transformation logic to the RDD, the Spark Driver (as identified by the Spark Context object used to create the RDD) coordinates the application of that logic to the RDD's partitions. 
# MAGIC 
# MAGIC The most commonly used of the transformations we might apply to an RDD is the **map()** transformation.  This transformation identifies a function that is applied to each element in the RDD:

# COMMAND ----------

# read the lines in the file to create the rdd
sightings = sc.textFile( 'wasbs://downloads@smithbc.blob.core.windows.net/ufo/ufo_awesome.tsv' )

# apply a tab-delimited split to each element in the rdd
split = sightings.map( lambda line: line.split('\t') )

# return a few split lines
split.take(3)

# COMMAND ----------

# MAGIC %md In the last cell, the **map()** transformation was used to apply an inline (*lambda*) function to each element in the sightings RDD.  Each element of the RDD is treated as the input to the function which we are capturing to a variable named *line*.  The lambda function defines that each *line* variable is split at each occurance of the tab-character.  The split generates a list and that list is an element in a new RDD named *split*.

# COMMAND ----------

# MAGIC %md Lambda functions are great for defining logic that can be specified as a single line of code.  When more complex, multi-line logic is required (or desirable), we might define a custom function for use with the **map()** transformation:

# COMMAND ----------

def splitSighting( line ):
  '''
  Split the line based on a tab delimiter and returns the first 6 fields from the split.
  The function will return a 6-item list regardless of the shape of the incoming line.
  '''
  ret = line + ('\t'*6)  # append tabs to ensure 6-tabs always present
  ret = ret.split('\t')  # split the line based on tab delimiter
  return ret[:6]         # return first 6 items in the list
                             
split = sightings.map( lambda line: splitSighting(line) )

split.take(3)

# COMMAND ----------

# MAGIC %md The custom function defined here isn't too complex but it illustrates how we can define multi-line logic and pass it to the **map()** transformation.  As before, each element in the RDD is captured to a variable called line and passed as the argument of the custom function. The function processes the incoming line and returns a list representing the split line similar to what we did before but with additional logic to ensure we always receive a certain number of elements back with each list.
# MAGIC 
# MAGIC Given that the element in the RDD is the sole argument of the function, we could skip over the lambda function declaration.  Instead, we can simply call the custom function and the element of the RDD being processes is assumed to be the first argument of that function call:

# COMMAND ----------

#split = sightings.map( lambda line: splitSighting(line) )

split = sightings.map( splitSighting )

split.take(3)

# COMMAND ----------

# MAGIC %md This technique works with both custom and built-in functions when the element in the RDD matches the expected function argument.

# COMMAND ----------

# MAGIC %md In addition to the **map()** transformation, we frequently make use of the **filter()** transformation. This transformation again uses a function that is applied to each element in the RDD. Unlike a function we might use with **map()**, functions passed to **filter()** return a *True* or *False* value. Elements that evaluate to *True* pass into the new RDD.  Elements that do not evaluate to *True* do not.
# MAGIC 
# MAGIC To see this in action, let's remove any bad sightings as identified by not having any text in the 6th field (indexed as 5):

# COMMAND ----------

split = sightings.map( splitSighting )

# remove any lines missing text in field (index) 5
filtered = split.filter( lambda fields: len(fields[5].strip()) != 0 )

# compare pre- and post-filter counts
print( split.count() )
print( filtered.count() )

# COMMAND ----------

# MAGIC %md As before, the **filter()** transformation can employ an (inline) lambda function or an externally defined function when multi-line logic might be better:

# COMMAND ----------

def isGoodSighting( fields ):
  '''
  return True unless fields 0, 1 or 5 have no text content
  '''
  if len(fields[0].strip()) == 0:
    return False
  if len(fields[1].strip()) == 0:
    return False
  if len(fields[5].strip()) == 0:
    return False
  else:
    return True

split = sightings.map( splitSighting )
filtered = split.filter(isGoodSighting )

print( split.count() )
print( filtered.count() )

# COMMAND ----------

# MAGIC %md The **map()** and **filter()** transformations will take you suprisingly far when you are coding against RDDs.  That said, there are many other transformations worth exploring.  Information on these can be found in the RDD documentation found here: https://spark.apache.org/docs/latest/api/python/pyspark.html.
# MAGIC 
# MAGIC One of these additional transformations that's worth mentioning here is the **sortBy()** transform.  This transformation orders the elements of the RDD based on a function-derived value. The **ascending** argument is (by default) set to *True* to enable sorting from lowest to highest values.  Setting the **ascending** argument to *False* reverses the sort order: 

# COMMAND ----------

split = sightings.map( splitSighting )
filtered = split.filter(isGoodSighting )
sorted = filtered.sortBy(lambda fields: fields[0], ascending=False)

sorted.take(3)

# COMMAND ----------

# MAGIC %md In this last example, we sorted on the first field in the split line in descending order. As this field represents an incident date for the line in the dataset, we've now sorted our RDD from newest to oldest indicent.

# COMMAND ----------

# MAGIC %md It's important to remember with Spark that defining transformation logic does nothing to the data on it's own.  It's only when we call a method that forces Spark to return data from the RDD that Spark actually executes this logic.  Methods such as these are known as *actions*. 
# MAGIC 
# MAGIC To observe this *lazy execution* approach, let's break some of our earlier code into two cells.  Executing the first cell sends transformation logic to Spark but no data processing actually takes place:

# COMMAND ----------

split = sightings.map( splitSighting )
filtered = split.filter(isGoodSighting )

# COMMAND ----------

# MAGIC %md With an action method call, data is spilled to the screen.  The transformation logic defined in previous cells is now executed:

# COMMAND ----------

filtered.take(3)

# COMMAND ----------

# MAGIC %md Knowing about *lazy execution* is an important in Spark, but awareness of *lazy execution* is far from an academic concern.  For you the Spark developer, it's important to understand that errors in your transformation logic may not be observable until you trigger that logic with an action.  If that action occurs far down in your code, you may have to search a bit to find the line that's causing you the problem. 
# MAGIC 
# MAGIC To demonstrate this, I've modified the logic to reference an item in our split lines that should not exist:

# COMMAND ----------

split = sightings.map( splitSighting )
filtered = split.filter(isGoodSighting ).filter( lambda fields: fields[100] == 0 )

# COMMAND ----------

# MAGIC %md Looks like Spark accepted the code with no problem.  But now trigger it's execution:

# COMMAND ----------

filtered.count()

# COMMAND ----------

# MAGIC %md It's not until I call the **take()** action to bring data to the screen that the invalid logic in my **filter()** transform is executed and the *index out of range* execption is raised. Keep the concept of *lazy execution* in mind as you trouble shoot errors in your code.

# COMMAND ----------

# MAGIC %md As indicated, **take()** is an action; it returns a specified number of elements from the RDD to the driver.  The **count()** method is also an action, forcing Spark to count the elements in an RDD and return that count as an integer value to the driver.
# MAGIC 
# MAGIC Another frequently used action is **collect()**.  The **collect()** action is very functionally similar to **take()**, returning elements from the RDD to the driver. Unlike **take()**, the **collect()** action doesn't limit the number of elements it returns and instead brings all elements from the RDD to the driver.  If you are working with a large data set, this action can bring an overwhelming amount of data back to the driver, triggering an *out-of-memory* error that crashes the driver software.  Be very careful with **collect()** and only use it when you know the amount of data you are returning is managable on a single machine.

# COMMAND ----------

# MAGIC %md **NOTE**: While the Spark documentation makes it sound like there is a clean division between transformations and actions with no code execution taking place until an action is specified. This isn't 100% accurate any more.  Some transformations like **sortBy()** benefit by having a bit of knowledge about the data flowing into it and trigger partial execution of transformation logic when submitted to the Spark driver. So keep the concept of *lazy execution* top of mind when working with Spark RDDs but don't expect the concept to be implemented consistently with each transformation.

# COMMAND ----------

# MAGIC %md Another important action to be aware of is the **saveAsTextFile()** action. As you might expect, this action saves the contents of an RDD as text files in the specified directory.  Before using this action, please be aware that data will be written from the RDD using whatever Python encoding is appropriate for the type of data stored with each element.  A common practice is to manually convert data to string and combine it with a delimiter of your choice so that the RDD is a collection of strings ready to be written to a text file prior to calling this action:

# COMMAND ----------

split = sightings.map( splitSighting )

filtered = split.filter(isGoodSighting )

sorted = filtered.sortBy(lambda fields: fields[0], ascending=False)

prepared = sorted.map( lambda fields: '\t'.join(fields) )

# this block enables multiple users to write to shared environment without conflicts
# ----------------------------------------------------------------------------------
import os
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
os.environ['DATABRICKS_USER_NAME']=user
# ----------------------------------------------------------------------------------

# delete target directory if exists
dbutils.fs.rm('/tmp/{0}/saveAsTextFile/'.format(user), recurse=True )

#prepared.take(3)
prepared.saveAsTextFile( '/tmp/{0}/saveAsTextFile/'.format(user) )

# COMMAND ----------

# MAGIC %md When we saved the RDD to text file, it's important to note we saved it to a directory.  In that directory, you will find multiple text file outputs, each aligned with a partition in the originating RDD:

# COMMAND ----------

# MAGIC %sh ls /dbfs/tmp/$DATABRICKS_USER_NAME/saveAsTextFile/

# COMMAND ----------

# MAGIC %md Before wrapping up this lab, let's take a look at a common practice for specifying multi-step logic against an RDD.  Here is the last block of code rewritten in a more compact manner:

# COMMAND ----------

prepared = (
 sc.textFile( 'wasbs://downloads@smithbc.blob.core.windows.net/ufo/ufo_awesome.tsv' )
  .map( splitSighting )
  .filter(isGoodSighting )
  .sortBy(lambda fields: fields[0], ascending=False)
  .map( lambda fields: '\t'.join(fields) )
  )

# this block enables multiple users to write to shared environment without conflicts
# ----------------------------------------------------------------------------------
import os
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
os.environ['DATABRICKS_USER_NAME']=user
# ----------------------------------------------------------------------------------

# delete target directory if exists
dbutils.fs.rm('/tmp/{0}/saveAsTextFile/'.format(user), recurse=True )

#prepared.take(3)
prepared.saveAsTextFile( '/tmp/{0}/saveAsTextFile/'.format(user) )

# COMMAND ----------

# MAGIC %md Under the covers, each transform logically defines an new RDD but you don't need to capture each of these to a variable. Instead, you can simply chain the transformation calls one after the other.  You can even tack on the action but most developers prefer to have some logical break that provides them the opportunity to inspect the results of the RDD transformations before applying an action (as demonstrated by the commented-out **take()** method call). 
# MAGIC 
# MAGIC How far you take this technique depends on you but I'd suggest keeping readability and maintainability top of mind.

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC The **wasbs://downloads@smithbc.blob.core.windows.net/nyse/** directory contains a series of text files, each line of which provides daily summary statistics for a given stock on the New York Stock Exchange for each day between 1962 and some time in 2010. 
# MAGIC 
# MAGIC Read the contents of this directory into an RDD. Split each line based on a comma-delimiter.  Knowing that the third field on each line is a date string in the format YYYY-MM-DD and the second field is the stock symbol, retrieve the line representing the first day of trading for FedEx (stock symbol: FDX).  (Answer: `['NYSE',
# MAGIC   'FDX',
# MAGIC   '1980-01-02',
# MAGIC   '44.38',
# MAGIC   '46.62',
# MAGIC   '44.38',
# MAGIC   '45.00',
# MAGIC   '369600',
# MAGIC   '2.72']`)

# COMMAND ----------

# your scenario 1 code here

nyse = sc.textFile('wasbs://downloads@smithbc.blob.core.windows.net/nyse/')
Pricing = (
  nyse.map(lambda string: string.split(','))
  .filter(lambda symbol: symbol[1] == 'FDX')
  .sortBy(lambda date: date[2], ascending=True)
  )

Pricing.first()


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
    .filter( lambda fields: fields[1]=='FDX' )
    .sortBy( lambda fields: fields[2], ascending=True)
  )

pricing.take(1)

# COMMAND ----------

