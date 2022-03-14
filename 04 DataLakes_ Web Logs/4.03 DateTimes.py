# Databricks notebook source
# MAGIC %md 
# MAGIC ###Data Lakes: DateTimes
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to teach you how make basic use of datetime values in Python and with PySpark.

# COMMAND ----------

# MAGIC %md In our last lab, we parsed the lines of our web log file using a regular expression. We then parsed the HTTP the request field into its consituent parts to get at exactly what was being retrieved from the webserver.  Now we will turn our attention to the datetime of the request.
# MAGIC 
# MAGIC To get started, let's read our weblog data back into an RDD:

# COMMAND ----------

import re

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
    )

expanded.sample(False, 0.1).take(3)

# COMMAND ----------

# MAGIC %md We now have an RDD within which each element is a list.  The items in these lists represent:
# MAGIC 
# MAGIC * 0 - host IP - '130.219.8.11'
# MAGIC * 1 - identity - '-'
# MAGIC * 2 - user - '-'
# MAGIC * 3 - datetime - '[27/Jul/2014:06:48:16 -0500]'
# MAGIC * 4 - http request - '"GET /cox-home-theme/images/common/news.png HTTP/1.1"'
# MAGIC * 5 - status - '304'
# MAGIC * 6 - size - '-'
# MAGIC * 7 - referer - '"-"'
# MAGIC * 8 - agent - '"Mozilla/4.0 (compatible;)"'
# MAGIC * 9 - http verb - 'GET'
# MAGIC * 10- http item - '/cox-home-theme/images/common/news.png'
# MAGIC * 11- http protocol - 'HTTP/1.1'
# MAGIC 
# MAGIC With our RDD assembled, let's examine a few of the datetime values in the datetime field (index=3):

# COMMAND ----------

( expanded
    .sample(False, 0.1)
    .map(lambda fields: fields[3]) # get just the datetime field
  ).take(3)

# COMMAND ----------

# MAGIC %md Looking at this string, we can see that it consists of a 2-digit day of the month, abbreviated month name, and 4-digit year, all separated by a forward slash. Immediately following this (with not spaces) is a colon followed by the two-digit hour (in 24-hour format), another colon, the two-digit minutes, still another colon, and then the two-digit seconds associated with the request. A space followed by the timezone offset closes out the string.  The entire string itself is wrapped in square brackets.
# MAGIC 
# MAGIC Our first reaction to converting such a string to a datetime might be to parse the different elements from it and generate a datetime value for these.  While this can be done, a shorthand method is provided using the **strptime** method of the Python datetime library:

# COMMAND ----------

from datetime import datetime

def parseDateTimeString( datetime_string ):
    return datetime.strptime( datetime_string, '[%d/%b/%Y:%H:%M:%S %z]')

sample = (expanded
          .sample(False, 0.1)
          .map(lambda fields:(fields[3], parseDateTimeString(fields[3])))
           )
sample.take(3)

# COMMAND ----------

# MAGIC %md Notice that datetime values in Python are objects for which specific date and time parts are assigned values.  For example, [27/Jul/2014:06:47:58 -0500] was interpretted using the pattern [%d/%b/%Y:%H:%M:%S %z] to produce a datetime object of `datetime.datetime(2014, 7, 27, 6, 47, 58, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400))))`. While this structure is not immediately familar to most human readers, you can see how it captures each element of the string to form its datetime representation.  While not formatted in the manner we may be accustomed to, storing the information this way allows it to perform complex datetime calculations with relative ease.  We aren't terribly interested in that in this lab but in the realworld there will be times when you need to take advantage of this and getting a datetime value read into a datetime object is the first step.

# COMMAND ----------

# MAGIC %md The **strptime** method takes a pattern which looks a little like RegEx but which operates in a more abbreviated manner. Because there are a limited number of variations in date and time strings, languages like Java and Python include shorthand parsing instructions for datetime values which can be combined to quickly interpret/parse a formatted string.  The meaning of these and other pattern elements can be found here: https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior  Take a moment to review the documentation, understand the meaning of each symbol being used here, and map those symbols back to elements in the datetime strings presented in the previous cell.  Please note that the square brackets that surround the datetime string in the weblog record are included in the pattern as literals.

# COMMAND ----------

# MAGIC %md Now that we've worked out how we might translate our datetime strings into actual datetime objects/values, let's apply this to the whole RDD:
# MAGIC 
# MAGIC NOTE: I'm providing a complete code sample here to make it easier to follow the whole data processing flow.

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

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC Let's say you have a datetime string in the form 01/02/2020.  Use the datetime.stptime() method to convert that string to a datetime value of January 2, 2020 12:00 AM.

# COMMAND ----------

# your scenario 1 code here
from datetime import datetime
time_string = '01/02/2020'
date = datetime.strptime(time_string, '%d/%m/%Y')
date

# COMMAND ----------

# MAGIC %md ####Scenario 2
# MAGIC 
# MAGIC Repeat Scenario 1 but interpret the string as Feburary 1, 2020 12:00 AM.

# COMMAND ----------

# your scenario 2 code here
date.strftime('%B %d, %Y %I:%M %p')
  

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
from datetime import datetime

time_string = '01/02/2020'
print( datetime.strptime(time_string, '%m/%d/%Y') )

# COMMAND ----------

# scenario 2
from datetime import datetime

time_string = '01/02/2020'
print( datetime.strptime(time_string, '%d/%m/%Y') )

# COMMAND ----------

