# Databricks notebook source
# MAGIC %md 
# MAGIC ###Data Lakes: Convert RDD to DataFrame
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to teach you how convert an RDD to a DataFrame.

# COMMAND ----------

# MAGIC %md Let's load our weblogs to an RDD and parse the data in preparation of its conversion to a dataframe.  This logic encapsulates everything we've done with our weblogs in the last two labs:

# COMMAND ----------

import re
from datetime import datetime

def get_fields(line):
    pattern = re.compile('^(\S*) (\S*) (\S) (-|\[.*\]) ([^\s\"]|\"[^\"]*\") (-|\d*) (-|\d*)(?: ([^\s\"]|\"[^\"]*\") ([^\s\"]|\"[^\"]*\"))+')
    match = pattern.match(line)
    if match is not None:
        return match.groups()
        
def get_http_elements( http_request ):
    pattern = re.compile('\"(\S*) (\S*) (\S*)\"')
    match = pattern.match(http_request)
    if match is not None:
        return match.groups()

def parseDateTimeString( datetime_string ):
    return datetime.strptime( datetime_string, '[%d/%b/%Y:%H:%M:%S %z]')
  
# read lines from weblog      
lines = sc.textFile('/tmp/weblogs/old/access.log')

# parse lines into fields, removing bad lines
fields = lines.map( get_fields ).filter( lambda f: f is not None)

# parse the http request field
expanded = (
  fields
    .map(lambda f: (f, get_http_elements(f[4])))  # split http request
    .filter(lambda f: f[1] is not None) # remove lines with bad http requests
    .map(lambda f: f[0] + f[1]) # restructure the data
    .map(lambda f: (f , parseDateTimeString(f[3]))) # convert string to datetime object
    .map(lambda f: f[0] + tuple([f[1]])) # restructure the data
    )

expanded.sample(False, 0.1).take(3)

# COMMAND ----------

# MAGIC %md We can now define a schema and use it to convert our RDD to a dataframe:

# COMMAND ----------

from pyspark.sql.types import *

my_schema = StructType([
    StructField('host', StringType()),
    StructField('identity', StringType()), 
    StructField('user', StringType()), 
    StructField('time_string', StringType()), 
    StructField('request', StringType()), 
    StructField('status', IntegerType()), 
    StructField('size', LongType()),
    StructField('referer', StringType()), 
    StructField('agent', StringType()), 
    StructField('verb', StringType()),
    StructField('uri', StringType()),
    StructField('protocol', StringType()),
    StructField('utc_time', TimestampType())
    ])

df = spark.createDataFrame( expanded, schema=my_schema)

df.show(10)

# COMMAND ----------

# MAGIC %md What went wrong?  Review the error details.
# MAGIC 
# MAGIC In our schema, we identified a few fields as having integer (and big integer aka long) data types.  In actuality, these fields are still strings in the RDD.  Let's convert these to the right types and then move this into a data frame:

# COMMAND ----------

cleansed = expanded.map(lambda f: 
                        f[0:5] + 
                        (int(f[5]), int(f[6])) + 
                        f[7:] 
                       )
cleansed.take(1)


# COMMAND ----------

# MAGIC %md **Another error?!** Let's review the error details to see what's going on.
# MAGIC 
# MAGIC Looks like there is a string with a value of '-' that we are trying to convert to a numerical value. If you review the logs, you will see that this occurs when test messages are sent by the server to itself.  Let's add a bit more logic to clean these values up:

# COMMAND ----------

cleansed = expanded.map(lambda f: f[0:5] + (int(f[5]), int(0 if (f[6]=='-') else f[6])) + f[7:] )
cleansed.take(1)

# COMMAND ----------

# MAGIC %md With a cleaner data set assembled, let's now re-try converting to a data frame:

# COMMAND ----------

df = spark.createDataFrame( cleansed, schema=my_schema)

#df.show(5)
display(df)

# COMMAND ----------

# MAGIC %md We are so close but there are still two fields that are bugging me: referer and agent.  The values in these fields are wrapped in quotation marks.  Let's remove those from the start and end of those strings:

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

final = df.withColumn(
        'referer', regexp_replace(df.referer, '"', '')
        ).withColumn(
        'agent', regexp_replace(df.agent, '"', '')
        )

display(final)

# COMMAND ----------

# MAGIC %md In the last example, we're converting the data from within the dataframe using the **regexp_replace** function.  This is one of several functions avilable in SparkSQL that make use of RegEx.  These functions are great when you need to make small changes to data or perform minor parsing, but I find that heavy RegEx manipulation is often easier to perform in the RDD before converting it to a dataframe.

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC Returning to the NYSE pricing dataset, we have loaded the data to an RDD, split each line on a comma-delimiter, filtered out the header line, and removed the exchange field. That leaves us with an RDD with elements that are structured like this data sample:
# MAGIC 
# MAGIC `['AEA', '2010-02-08', '4.42', '4.42', '4.21', '4.24', '205500', '4.24']`
# MAGIC 
# MAGIC Using the datetime library, **replace** the second field with a a datetime value (with time set to midnight) representing the datetime of the original date string.

# COMMAND ----------

from datetime import datetime

nyse_rdd = (
  sc.textFile('/tmp/nyse/')
    .map(lambda line: line.split(',')) # split lines
    .filter(lambda field: field[0]=='NYSE') # remove headers
    .map(lambda field: field[1:])
  )

# your scenario 1 code here
nyse_fmt = (
  nyse_rdd
    .map(lambda lst: (lst[0], datetime.strptime(lst[1], '%Y-%m-%d'), *lst[2:]))
  )

# COMMAND ----------

# MAGIC %md ####Scenario 2
# MAGIC 
# MAGIC Building on the work in Scenario 1, convert the RDD into a dataframe with the following fields and data types:
# MAGIC 
# MAGIC  |-- stock_symbol: string <br>
# MAGIC  |-- date: timestamp <br>
# MAGIC  |-- stock_price_open: double <br>
# MAGIC  |-- stock_price_high: double <br>
# MAGIC  |-- stock_price_low: double <br>
# MAGIC  |-- stock_price_close: double <br>
# MAGIC  |-- stock_volume: integer <br>
# MAGIC  |-- stock_price_adj_close: double <br>
# MAGIC  

# COMMAND ----------

# your scenario 2 code here
from pyspark.sql.types import *
nyse_schema = StructType([
  StructField('stock', StringType()),
  StructField('date', DateType()),
  StructField('stock_price_open', DoubleType()),
  StructField('stock_price_high', DoubleType()),
  StructField('stock_price_low', DoubleType()),
  StructField('stock_price_close', DoubleType()),
  StructField('stock_volume', IntegerType()),
  StructField('stock_price_adj_close', DoubleType())
  ])

nyse_fmt.sample(False, 0.2).take(2)

def type_casting(lst):
  lst_output = copy.deepcopy(lst)
  for i in range(len(lst)):
    lst_itm = lst[i]   
    if isinstance(lst_itm, datetime):
      pass    
    elif re.match(r'^\d*(?<=\d)\.(?=\d*)\d*', lst_itm):
      lst_output[i] = float(lst_itm)      
    elif re.match(r'(?<!\s\w)^\d+$(?!\w\s)',lst_itm):
      lst_output[i] = int(lst_itm)     
    else: 
      pass 
  return tuple(lst_output)

nyse_df = spark.createDataFrame(
  nyse_fmt
    .map(lambda lst: type_casting([*lst])),
  schema=nyse_schema
  )

  
#nyse_df = spark.createDataFrame(nyse_fmt, schema=nyse_schema)
nyse_df.show(5)

# COMMAND ----------

import re 
import copy

lst = ['AEA',
  datetime(2010, 2, 8, 0, 0),
  '4.42',
  '4.42',
  '4.21',
  '4.24',
  '205500',
  '4.24']

def type_casting(lst):
  lst_output = copy.deepcopy(lst)
  for i in range(len(lst)):
    lst_itm = lst[i]
    
    if isinstance(lst_itm, datetime):
      pass
    
    elif re.match(r'^\d*(?<=\d)\.(?=\d*)\d*', lst_itm):
      lst_output[i] = float(lst_itm)
      
    elif re.match(r'(?P<int>'
                   r'^\d'
                   r'((?<!(\s\w))\d(?!\s\w))+'
                   r'\d$'
                  r')',
                  lst_itm):
      lst_output[i] = int(lst_itm)
      
    else: 
      pass 
  return lst_output

type_casting(lst)
    

# COMMAND ----------

# MAGIC  %md ####Answers (scroll down)
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC   <br></p> 
# MAGIC <br></p>

# COMMAND ----------

# scenario 1
nyse_rdd = (
  sc.textFile('/tmp/nyse/')
    .map(lambda line: line.split(',')) # split lines
    .filter(lambda field: field[0]=='NYSE') # remove headers
    .map(lambda field: field[1:])
  )

from datetime import datetime

replaced_rdd = (
  nyse_rdd
    .map(lambda field: [field[0], datetime.strptime(field[1], '%Y-%m-%d')] + field[2:] )
  )
                        
replaced_rdd.take(3)


# COMMAND ----------

from pyspark.sql.types import *

nyse_schema = StructType([
  StructField('stock_symbol' , StringType()),
  StructField('date' , TimestampType()),
  StructField('stock_price_open' , DoubleType()),
  StructField('stock_price_high' , DoubleType()),
  StructField('stock_price_low' , DoubleType()),
  StructField('stock_price_close' , DoubleType()),
  StructField('stock_volume' , IntegerType()),
  StructField('stock_price_adj_close' , DoubleType())
  ])

cleansed_rdd = (
  replaced_rdd
    .map(lambda field: field[:2] + list(map(float, field[2:6])) + [int(field[-2])] + [float(field[-1])] )
  )

nyse = spark.createDataFrame( cleansed_rdd, schema=nyse_schema )
display(nyse)

# COMMAND ----------

