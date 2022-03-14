# Databricks notebook source
# MAGIC %md 
# MAGIC ###Data Lakes: RegEx & PySpark
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to teach you how make use of Regular Expressions in Python and with PySpark.

# COMMAND ----------

# MAGIC %md When reading text files, we commonly encounter lines of data which can't be parsed into fields using a simple delimiter. In these situations, we might find it easier to use Regular Expressions which allow us to define a generalized pattern for the structure of the line.
# MAGIC 
# MAGIC To work with RegEx in Python, we need to load the **re** library: 

# COMMAND ----------

import re

help(re)

# COMMAND ----------

# MAGIC %md To understand RegEx, let's start with a simple line of text which we will parse using a pattern.  This line comes from a weblog that's structured using the [Apache Server access combined log format](https://httpd.apache.org/docs/2.4/logs.html#accesslog):

# COMMAND ----------

line ='5.255.195.6 - - [31/Jul/2014:18:45:57 -0500] "GET /web/jdmba/essays HTTP/1.1" 200 8657 "-" "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)"'

# COMMAND ----------

# MAGIC %md Using a RegEx pattern commonly employed with this format, let's attempt to extract the fields from this line:

# COMMAND ----------

pattern = re.compile('^(\S*) (\S*) (\S) (-|\[.*\]) ([^\s\"]|\"[^\"]*\") (-|\d*) (-|\d*)(?: ([^\s\"]|\"[^\"]*\") ([^\s\"]|\"[^\"]*\"))+')

match = pattern.match(line)
print(match.groups())

# COMMAND ----------

# MAGIC %md Notice how the groups defined in the RegEx pattern are what are returned by the **groups()** method. 
# MAGIC 
# MAGIC Now that we have the ability to extract data from the lines of our weblog, let's load our data to an RDD and parse it using RegEx:

# COMMAND ----------

# MAGIC %md Note that I am randomly selecting a row to return to the screen so that you might see how a variety of rows might be parsed with each re-run without spilling too much data to the screen in one shot.

# COMMAND ----------

import re

def get_fields(line):
    pattern = re.compile(
      '^(\S*) (\S*) (\S) (-|\[.*\]) ([^\s\"]|\"[^\"]*\") (-|\d*) (-|\d*)(?: ([^\s\"]|\"[^\"]*\") ([^\s\"]|\"[^\"]*\"))+'
      )
    matches = pattern.match(line)
    return matches.groups()

lines = sc.textFile('/tmp/weblogs/old/access.log')
fields = lines.map( get_fields )
fields.sample(False, 0.1).take(3)

# COMMAND ----------

# MAGIC %md Did this work?  If not, **what error did you receive?**
# MAGIC 
# MAGIC NOTE There are some invalid lines in the weblogs but because we are randomly parsing lines with this code, not every person will hit one of these.
# MAGIC 
# MAGIC If you received an error, you can scroll down through trace to see a TypeError with a description that's not very helpful. If we play around with the code, we find that this error message is coming from the MatchObject when the groups() method is called.
# MAGIC 
# MAGIC It turns out that when a line is not successfully parsed, the MatchObject is assigned a value of None which is what is triggering the error.  A simple test for this condition will help us avoid an invalid method call:

# COMMAND ----------

def get_fields(line):
    pattern = re.compile(
      '^(\S*) (\S*) (\S) (-|\[.*\]) ([^\s\"]|\"[^\"]*\") (-|\d*) (-|\d*)(?: ([^\s\"]|\"[^\"]*\") ([^\s\"]|\"[^\"]*\"))+'
      )
    matches = pattern.match(line)
    if matches is not None:
        return matches.groups()
    
fields = lines.map( get_fields ).filter( lambda f: f is not None)
fields.sample(False, 0.1).take(3)

# COMMAND ----------

# MAGIC %md But what about the lines we are dropping with the logic above?  When dealing with large datasets like this, it is common that we will encounter some bad data.  The trick is to recognize when it really affects the results.  One quick action we might take to see how it affects us is to simply check the number of lines against the number of parsed lines:

# COMMAND ----------

print(lines.count())
print(fields.count())

# COMMAND ----------

# MAGIC %md It turns out there are eleven rows in this input dataset that are not parsing correctly.  Do you wish to see these rows? Let's write a bit of code to find these:

# COMMAND ----------

bad_rows = (
  lines
    .map( lambda line: (get_fields(line), line) )
    .filter(lambda f: f[0] is None)
  )

bad_rows.map(lambda f: f[1]).collect()

# COMMAND ----------

# MAGIC %md Do you see the problem?  Look closely at the HTTP request in each of these rows.  Notice that each has embedded doublequotes as identified by the \\".  We could adjust our RegEx pattern to deal with this or just make the decision at 11 rows out of 1.13M is not enough to be concerned with and just move on.  In the world of Big Data, we are often working with such large volumes of data that a small number of missed rows like this don't affect our outcomes.  We therefore typically ignore these cases once we understand where they are coming from.

# COMMAND ----------

# MAGIC %md Take another look at the good data rows:

# COMMAND ----------

fields.sample(False, 0.1).take(3)

# COMMAND ----------

# MAGIC %md We might wish to take these data into a data frame but before we do so we might clean up a couple data elements: the HTTP request and the datetime value.

# COMMAND ----------

# MAGIC %md Here is one of the valid HTTP requests found in weblogs file: `"GET /cox-home-theme/images/common/news.png HTTP/1.1"`
# MAGIC 
# MAGIC Notice the request consists of three fields: the HTTP verb, the resource targetted by the verb, and the HTTP version in use. We might extract these using another RegEx pattern.  As before, we will filter out any rows that don't get successfully matched to our pattern:

# COMMAND ----------

def get_http_elements( http_request ):
    regex_pattern = '\"(\S*) (\S*) (\S*)\"'
    m = re.match(regex_pattern, http_request)
    if m is not None:
        return m.groups()
    
expanded = (
  fields
    .map( lambda f: (f, get_http_elements(f[4])) )
    .filter(lambda f: f[1] is not None)
  )

expanded.sample(False, 0.1).take(3)

# COMMAND ----------

# MAGIC %md Notice the structure of our data is a little funny now.  Each element in the RDD is a tuple.  The last position in that tuple is another tuple representing the parsed elements of the HTTP request.  Let's add some logic to flatten this structure so that each element is one tuple of scalar values:

# COMMAND ----------

expanded = (
  fields
    .map(lambda f: (f, get_http_elements(f[4])))
    .filter(lambda f: f[1] is not None)
    .map(lambda f: f[0] + f[1])
  )

expanded.sample(False, 0.1).take(3)

# COMMAND ----------

# MAGIC %md Now we can turn our attention to the datetime field.  But we will do this in the next lab.

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC The referer field within the weblog identifies the page from which the client navigated to the requested page or object.  Google searches are one of the top referers to the SMU website and the referer strings associated with Google searches follow a standard pattern.  Here is a referer string from an actual Google search found in our weblogs:
# MAGIC 
# MAGIC `"http://www.google.com/search?q=oil+and+gas+management+courses&revid=1466957649&sa=X&ei=xsbVU7uRJsqKyAT6-YCQBw&ved=0CFMQ1QIoBw&biw=320&bih=508&dpr=1.5"`
# MAGIC 
# MAGIC Notice the string is wrapped in quotation marks.  The leading part of the string is either `http://` or `https://` followed by `www.google.`, the domain extension (which in this case is `com`), and then `/search`.  There is then a question mark `?` which separates the URL of the referer (which is everything we just discussed) and the query string that allowed that URL to generate the referring page.
# MAGIC 
# MAGIC Each element in the query string is seperated by an ampersand `&` character. Each element has a key separated from its value by an equal sign.  The key `q` identifies the actual information searched on by the user to generate the Google results page that led to the SMU website.
# MAGIC 
# MAGIC With all this in mind, write a RegEx pattern to retrieve just the search string, *i.e.* the value associated with the `q` parameter, from this referer string.  Encapsulate the retrieval in a Python custom function definition and print the search string to the screen.  Be sure the pattern allows for either http or https for the protocol, recognizes any possible extension, and properly handles the fact that / is a special character (along with . and ?) which must be escaped to be searched as a literal.

# COMMAND ----------

# your scenario 1 code here
import re

referer = '"http://www.google.com/search?q=oil+and+gas+management+courses&revid=1466957649&sa=X&ei=xsbVU7uRJsqKyAT6-YCQBw&ved=0CFMQ1QIoBw&biw=320&bih=508&dpr=1.5"'

def get_google_query( referer ):
  regex_pattern =  re.compile(r'[http|https]://www\.(?P<domain>.+)\.com\.*/search\?q=(?P<search>.+?)&.+')
  m =  regex_pattern.search(referer)
  if m is not None:
      return m.groups() # you should only be retrieving one group from the referer
    
print( get_google_query(referer) )

# COMMAND ----------

# MAGIC %md ####Scenario 2
# MAGIC 
# MAGIC Continuing with the previous example, apply the get_google_query() function to your expanded RDD as defined in the lab to extract Google search strings as a new field.  Don't worry about flattening the tuple you construct but do filter out all records that don't include a Google search string result:

# COMMAND ----------

# your scenario 2 code here

googled = (
  expanded
    .map(lambda log: (log, get_google_query(log[7]))) # your code here
    .filter(lambda log_nested: log_nested[1] is not None)
    .map(lambda log_nested: (*log_nested[0], *(str(data).lower() for data in log_nested[1])))
    .filter(lambda log_nested: log_nested[-2] == 'google') # your code here 
    )

googled.take(10)

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
import re

referer = '"http://www.google.com/search?q=oil+and+gas+management+courses&revid=1466957649&sa=X&ei=xsbVU7uRJsqKyAT6-YCQBw&ved=0CFMQ1QIoBw&biw=320&bih=508&dpr=1.5"'

def get_google_query( referer ):
  regex_pattern = '"https?:\/\/www\.google\.com\/search?\S*q=([^\s&]*)\S*"'
  m = re.match(regex_pattern, referer)
  if m is not None:
      return m.groups()[0]
    
print( get_google_query(referer) )

# COMMAND ----------

# scenario 2

googled = (
  expanded
    .map(lambda f: (f, get_google_query(f[7])))
    .filter(lambda f: f[1] is not None)
  )

googled.take(10)

# COMMAND ----------

