# Databricks notebook source
# MAGIC %md ###General Instructions
# MAGIC In this assignment, you will need to complete the code samples where indicated to accomplish the given objectives. **Be sure to run all cells** and export this notebook as an HTML with results included.  Upload the exported HTML file to Canvas by the assignment deadline.

# COMMAND ----------

# MAGIC %md ####Assignment
# MAGIC Complete the following Python script per the instructions provided at the top of each code block. Look for the 
# MAGIC *# MODIFY THIS LINE* comment to indicate where you need to make code modifications. Do not add or remove any lines to this code. Everything should be able to be performed with the provided number of code lines.

# COMMAND ----------

# MAGIC %md Weblogs from the smu.edu website from Oct 25 through Nov 3, 2018 are provided at /tmp/weblogs/new/.  There are 10 files for this period, one for each day, and they all employ the same format.  In the following cells, you will briefly examine these files:

# COMMAND ----------

# list the log files in the folder
display(
  dbutils.fs.ls('/tmp/weblogs/new/')
  )

# COMMAND ----------

# examine the headers and first line of data from one of these files
dbutils.fs.head('/tmp/weblogs/new/u_ex181031_x.log').split('\r\n')[:5]

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Looking at the file contents, you should notice these logs use a different format than the one we explored in class.  That's because earlier in 2018, SMU made some changes to their webservers that caused them to default to what is known as the [W3C Extended Format](https://www.w3.org/TR/WD-logfile.html), which you can read about here: https://docs.microsoft.com/en-us/windows/desktop/http/w3c-logging.
# MAGIC 
# MAGIC A sample of data from this file is found in the sample_homework.txt file included in the datasets folder found in this week's class download. Open this file in a text editor.  You should notice the file includes a number of metadata lines at the top, each of which starts with a #-symbol.  The last of these is one that names the fields in this file.
# MAGIC 
# MAGIC Using the sample_homework.txt and an online RegEx explorer such as the one we used in class, come up with a RegEx pattern that captures all the data (in the data lines). Don't worry about excluding the one metadata line that represents the header for your data lines; you can add a filter method call to remove this line later.
# MAGIC 
# MAGIC When constructing your RegEx pattern, you will want to note that the W3C Extended Format captures data with no spaces in any fields.  All fields are separated with a single space. This should allow you to specify a relatively simple RegEx pattern.
# MAGIC 
# MAGIC To keep things lighter on your machines, let's limit our work to the October 31st file identified above (u_ex181031_x.log).

# COMMAND ----------

weblog_file_name = '/tmp/weblogs/new/u_ex181031_x.log'

# COMMAND ----------

import re

def get_fields(line):
    regex_pattern = (
                    r'(?P<Date>\d{4}-\d{2}-\d{2})\s' +
                    r'(?P<Time>\d{2}\:\d{2}\:\d{2})\s' +
                    r'(?P<ServerIP>(?:\d+\.)+\d+)\s' +
                    r'(?P<csMethod>.+?)\s' +
                    r'(?P<csUriStem>.+?|\/)\s' +
                    r'(?P<csUriQuery>\-|.+?)\s' +
                    r'(?P<ServerPort>\d+?)\s' +
                    r'(?P<UserName>\-|.+?)\s' +
                    r'(?P<UserAgent>.+?)\s' +
                    r'(?P<Referer>.+?)\s' +
                    r'(?P<scstatus>\d+?)\s' +
                    r'(?P<scSub>\d+?)\s' +
                    r'(?P<scWin32>\d+?)\s' +
                    r'(?P<TimeTaken>\d+?)\s' +
                    r'(?P<Forward>(?:\d+\.)+\d+)' 
    )
    m = re.match(regex_pattern, line)
    if m is not None:
        return m.groups()

# read the raw log data
raw_lines = sc.textFile(weblog_file_name)

# remove any metadata lines 
remove_meta = raw_lines.filter(lambda line: (False if (re.match(r'^#', line)) else True))

# parse log data using get_fields and filter out any poorly parsed lines
parsed = (
  remove_meta
    .map(lambda line: get_fields(line))
    .filter(lambda rcd: rcd is not None)
  )
 
parsed.take(5)

# COMMAND ----------

# MAGIC %md Verify that less than 1% of lines were removed given previous steps.  If 1% or more lines have been removed, revisit the previous cell, verifying your RegEx pattern and any filtering logic.  Do not proceed until you are below the 1% data loss threshold.

# COMMAND ----------

line_count = raw_lines.count()
parsed_count = parsed.count()
percent_diff = (100. * (line_count - parsed_count))/line_count

print('The number of lines in the file is {0}'.format(line_count))
print('The number of lines parsed from the file is {0}'.format(parsed_count))
print('There is a {0:.4f}% difference in element counts between the two rdds'.format(percent_diff))

# COMMAND ----------

# MAGIC %md With your data properly parsed, you now need to convert your date and time fields into a single datetime field.  Use the datetime library to assist you in this.  Remember to filter out any lines that may not parse correctly.  You should expect to lose no lines in this file due to bad date and time values.  Keep in mind that in the W3C Extended format, the date and time values are recorded as separate fields.  You will need to combine them in order to construct a proper datetime value:

# COMMAND ----------

from datetime import datetime

# append a datetime field to your rdd, filtering out any poorly parsed datetime values
# be sure to leave your rdd in a state where each element is a tuple consisting of 16 values,
# the last of which is a datetime value
dt_converted = (
  parsed
    .map(lambda rcd: 
         (
           *rcd, 
           datetime.strptime(rcd[0].__add__(f' {rcd[1]}'), '%Y-%m-%d %H:%M:%S')
         )
        ) 
  )

# verify the structure of your rdd elements
dt_converted.take(1)

# COMMAND ----------

# MAGIC %md Again, we need to verify we loss less than 1% of data given the filter above.  If you exceed this threshold, return to the previous cell and make appropriate corrections to your logic:

# COMMAND ----------

dt_count = dt_converted.count()
percent_diff = (100. * (parsed_count - dt_count))/parsed_count

print('The number of parsed lines is {0}'.format(parsed_count))
print('The number of parsed lines with bad datetime values is {0}'.format(dt_count))
print('There is a {0:.4f}% difference in element counts between the two rdds'.format(percent_diff))

# COMMAND ----------

# MAGIC %md You are just about ready to convert your RDD to a DataFrame, but because you will want your sc-status and time-taken fields to be brought into the data frame as integer values, you'll need to convert those fields to integers now:

# COMMAND ----------

# convert the sc-status and time-taken fields to integer values
cleansed = dt_converted.map(lambda rcd: 
                            (
                              *rcd[:-6],
                              int(rcd[-6]),
                              *rcd[-5:-3],
                              int(rcd[-3]),
                              *rcd[-2:]
                            )
                           )

cleansed.take(5)

# COMMAND ----------

# MAGIC %md You know have a well-formed data set within your RDD.  Define a schema for this data set and convert it, using that schema, into a data frame.  Once converted to a dataframe, drop the date and time fields but keep the datetime value which you created in previous cells. Also, drop the sc_substatus and sc_win32_status fields as they are not needed.
# MAGIC 
# MAGIC When defining your schema, be sure to use field names corresponding to those in the header line of your log file.  Be sure to replace all dashes in those field names with underscores; the dashes make for some confusing SQL syntax later.
# MAGIC 
# MAGIC Also, be sure to type all fields as strings except for sc_status and time_taken which should be integers and the utc_time field (which will be mapped to the datetime value constructed in the last cell) which should be typed as a timestamp:

# COMMAND ----------

from pyspark.sql.types import *

my_schema = StructType([
  StructField('Date', StringType()),
  StructField('Time', StringType()),
  StructField('IP', StringType()),
  StructField('csMethod', StringType()),
  StructField('cs_uri_stem', StringType()),
  StructField('cs_uri_query', StringType()),
  StructField('s_port', StringType()),
  StructField('cs_username', StringType()),
  StructField('user_agent', StringType()),
  StructField('referer', StringType()),
  StructField('sc_status', IntegerType()),
  StructField('sc_substatus', StringType()),
  StructField('sc_win32_status', StringType()),
  StructField('time_taken', IntegerType()),
  StructField('StreamID', StringType()),
  StructField('utc_time', DateType())
])

# create a dataframe
df = spark.createDataFrame( cleansed, schema=my_schema)

# drop fields as instructed above
focused = df.drop('Date', 'Time', 'sc_substatus', 'sc_win32_status')

# display some results to verify code
focused.show()

# COMMAND ----------

# MAGIC %md Now that we have a DataFrame, register it as a temporary view named *logs*. 

# COMMAND ----------

focused.createOrReplaceTempView('logs')

spark.sql('show tables').show()

# COMMAND ----------

# MAGIC %md In our log data set, the cs_uri_stem represents the asset requested from the web server.  This will include a mix of web pages, images, and scripts. Write a query that returns all fields from logs where where cs-uri-stem **DOES NOT** include the following image file types:
# MAGIC * png files
# MAGIC * jpeg & jpg files
# MAGIC * gif files
# MAGIC * js files
# MAGIC * css files
# MAGIC * ico files
# MAGIC * pdf files
# MAGIC * ashx files
# MAGIC 
# MAGIC Save this as a temporary view named *pages*.

# COMMAND ----------

query = '''
SELECT *
FROM logs 
WHERE 
  cs_uri_stem NOT LIKE '%.png' AND 
  cs_uri_stem NOT LIKE '%.jpeg' AND 
  cs_uri_stem NOT LIKE '%.jpg' AND 
  cs_uri_stem NOT LIKE '%.gif' AND
  cs_uri_stem NOT LIKE '%.js' AND
  cs_uri_stem NOT LIKE '%.css' AND 
  cs_uri_stem NOT LIKE '%.ico' AND 
  cs_uri_stem NOT LIKE '%.pdf' AND 
  cs_uri_stem NOT LIKE '%.ashx'
'''

# execute the query, capturing results to a temporary view
spark.sql(query).write.saveAsTable('pages', format='parquet', mode='overwrite')

spark.sql('show tables').show()

# COMMAND ----------

# MAGIC %md Write a query that answers the question, what are the most frequently visited pages accessed from the default smu.edu web page?  Keep in mind that the default smu.edu web page has the cs_referer string of *https://www.smu.edu/*. Use the show method to display the first 20 results to the screen, sorted from most frequent to less frequent:

# COMMAND ----------

query = '''
SELECT *
FROM 
  (
    SELECT 
      cs_uri_stem as Pages,
      count(*) as TimesViewed 
    FROM pages
    GROUP BY 
      cs_uri_stem
  )
ORDER BY TimesViewed DESC
LIMIT 20
'''

spark.sql(query).show(truncate=False)

# COMMAND ----------

# MAGIC %md With each cell in this notebook executed and results (where applicable) displayed, save this notebook as an HTML file and upload it to Canvas as explained at the top of the notebook.