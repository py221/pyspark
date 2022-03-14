# Databricks notebook source
# MAGIC %md ###General Instructions
# MAGIC In this assignment, you will need to complete the code samples where indicated to accomplish the given objectives. **Be sure to run all cells** and export this notebook as an HTML with results included.  Upload the exported HTML file to Canvas by the assignment deadline.

# COMMAND ----------

# MAGIC %md ####Assignment
# MAGIC Complete the following python script per the instructions provided at the top of each code block. Look for the 
# MAGIC *# MODIFY THIS LINE* comment to indicate where you need to make code modifications. Do not add or remove any lines to this code. Everything should be able to be performed with the provided number of code lines.

# COMMAND ----------

# set input directory name
input_dir_name = 'wasbs://downloads@smithbc.blob.core.windows.net/nyse/'

# COMMAND ----------

# create an rdd named raw to hold the nyse data, parsing each line appropriately
raw = sc.textFile(input_dir_name)

# limit the fields in your rdd to stock_symbol, date and stock_price_close (in that order)
limited = (
  raw.map(lambda line_str: line_str.split(','))
    .map(lambda lst: (lst[1], lst[2], lst[6]))
  )

# filter the rdd to focus on the IBM data 
filtered = limited.filter(lambda lst: lst[0] == 'IBM')

# parse the year from date, adding date as an integer value to each element in the rdd
with_years = filtered.map(lambda lst: (int(lst[1][:4]), 
                                       lst, 
                                       int(str(lst[1][5:7])
                                           +str(lst[1][8:10])
                                          )
                                      )
                         )

# filter the rdd to focus on data for years between 1990 and 1999
nineties = with_years.filter(lambda lst: 
                               (
                                 (lst[0] >= 1990) 
                                 and
                                 (lst[0] <= 1999)
                               )
                            )

# create a pair rdd in the form (year, stock_price_close), converting the stock_price_close value to a float
pair = nineties.map(lambda lst: (lst[0], float(lst[1][2])))

# using the max function with the reduceByKey transform, calculate the highest closing price for IBM stock in each year in the rdd
result = pair.reduceByKey(lambda a, b: max(a,b))

# in one line of code, sort your data by year in ascending order and collect it to the screen
result.sortByKey(ascending=True).collect()