# Databricks notebook source
# MAGIC %md ###General Instructions
# MAGIC In this assignment, you will need to complete the code samples where indicated to accomplish the given objectives. **Be sure to run all cells** and export this notebook as an HTML with results included.  Upload the exported HTML file to Canvas by the assignment deadline.

# COMMAND ----------

# MAGIC %md ####Assignment
# MAGIC Complete the following python script per the instructions provided at the top of each code block. Look for the 
# MAGIC *# MODIFY THIS LINE* comment to indicate where you need to make code modifications. Do not add or remove any lines to this code. Everything should be able to be performed with the provided number of code lines.

# COMMAND ----------

# set input and output file directories
input_dir_name = 'wasbs://downloads@smithbc.blob.core.windows.net/nyse/'
output_dir_name = '/tmp/ibm_2000s/'

# COMMAND ----------

# make sure output directory doesn't exist
dbutils.fs.rm(output_dir_name, recurse=True)

# COMMAND ----------

# read the nyse pricing data into an rdd named raw
raw =  sc.textFile(input_dir_name)

# split the raw rdd on a comma-separator into a new rdd named split
split =  raw.map(lambda line: line.split(','))

# using a labmda function, filter the split rdd on stock symbol IBM
# save this to an rdd named ibm_only
ibm_only =  split.filter(lambda line_list: line_list[1] == 'IBM')

# using a labmda function, filter ibm_only on year >= 2000 and year < 2010 
# send filtered data into a new rdd named filtered
filtered = ibm_only.filter(lambda line_list: 
                            (
                              (int(line_list[2][:4]) >= 2000)
                              and
                              (int(line_list[2][:4]) < 2010)
                            )
                          )

#HINT: remember to cast your years as integers

# reassemble the split records in filtered into tab-delimited lines in a new rdd named output
output = filtered.map(lambda line_lst: line_lst[0] + '\t'.join(line_lst[1:]))

# save the output rdd as text to your output directory
output.saveAsTextFile(output_dir_name)

# COMMAND ----------

# list output files
dbutils.fs.ls(output_dir_name)

# COMMAND ----------

# show first few lines of first output file
output_files = dbutils.fs.ls(output_dir_name)
filtered = filter(lambda x: x[2]>0, output_files)
for f in filtered:
  print( dbutils.fs.head(f[0]) )