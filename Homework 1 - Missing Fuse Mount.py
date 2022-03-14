# Databricks notebook source
# MAGIC %md ###General Instructions
# MAGIC In this assignment, you will need to complete the code samples where indicated to accomplish the given objectives. **Be sure to run all cells** and export this notebook as an HTML with results included.  Upload the exported HTML file to Canvas by the assignment deadline.

# COMMAND ----------

# MAGIC %md ####Assignment
# MAGIC The ufo_awesome.tsv is a tab-delimited text file. It does not contain a 
# MAGIC header row.  For that reason, you will want to know that the fields in the
# MAGIC text file are (in this order):
# MAGIC 
# MAGIC * date_sighted
# MAGIC * date_reported
# MAGIC * location
# MAGIC * shape
# MAGIC * duration
# MAGIC * description
# MAGIC 
# MAGIC The data in the file is problematic so that some lines will not parse 
# MAGIC correctly.  We will ignore lines that do not contain a minimum of six fields.
# MAGIC Also, note that many fields on perfectly valid lines will be empty.
# MAGIC 
# MAGIC With that in mind, complete the following Python script to extract from this
# MAGIC data file the date_reported field along with a count of words in the description
# MAGIC that accompanies the sighting for all sightings that occured during or after 
# MAGIC the year 1990.  Look for the *# ADD YOUR CODE HERE* and *# MODIFY THIS LINE* comments to indicate where you need to make code modifications.

# COMMAND ----------

# MAGIC %sh rm /tmp/ufo_output.tsv

# COMMAND ----------

from pathlib import Path
Path("/tmp/ufo_output.tsv").touch()

# COMMAND ----------

# MAGIC %sh ls /tmp

# COMMAND ----------

def is_good_record(fields):
  '''returns true if fields list contains 6 or more fields, otherwise returns false'''
  if len(fields) >= 6:
    return True
  else:
    return False

# COMMAND ----------

def convert_list_to_string(list, sep=','):
  '''
  returns a string representing the delimited values of a list
  the delimiter is indicated by the sep argument which defaults to a comma-delimiter
  '''
  string = sep.join(list)
  return string

# COMMAND ----------

import requests

# input and output file paths
#input_file_name = '/dbfs/tmp/ufo_awesome.tsv'
output_file_name = '/tmp/ufo_output.tsv'

# open the output file
output_file = open(output_file_name, 'w')

# retrieve data from storage
link = "https://smithbc.blob.core.windows.net/downloads/ufo/ufo_awesome.tsv"
input_file = requests.get(link).text.split('\n')

# process the input file
for line in input_file:

  new_line = []

  # convert input line to list
  fields = line.split('\t')

  if is_good_record(fields):

      # capture the date reported field
      date_reported = fields[1]

      # get the year from the date reported
      year_reported = date_reported[0:4]

      # if the year is 1990 or higher:
      if  int(year_reported) >= 1990:
          new_line += [date_reported]

          # capture the description field 
          description = fields[5]

          # remove any leading or trailing white space from description
          description = description.strip()

          # convert description to list of words
          words = description.split()

          # count the number of words
          word_count = str(len(words))

          new_line += [word_count]
          
          # convert new_line list to tab-delimited string
          new_line_as_string = convert_list_to_string(new_line, sep='\t')

          print(new_line_as_string)
          #output_file.write( new_line_as_string + '\n')
        
#close the output file properly
output_file.close()

# COMMAND ----------

