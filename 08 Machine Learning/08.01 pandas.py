# Databricks notebook source
# MAGIC %md 
# MAGIC ###Machine Learning: pandas
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to provide a review of pandas DataFrames concepts and methods that we will employ in future labs.

# COMMAND ----------

# MAGIC %md [pandas](https://pandas.pydata.org/) is a Python library providing objects for the access, manipulatation and analysis of structured data. The pandas DataFrame object is the most frequently employed of these objects, allowing data to be represented in a familar, two-dimensional, table-like manner.
# MAGIC 
# MAGIC While we commonly think of a pandas DataFrame (referred to simply as a DataFrame for the remainder of this lab) as a set of rows and columns, it's more appropriate to think of a DataFrame as a collection of ordered, one-dimensional lists, each of which we refer to as a pandas Series object. With these ideas in mind, let's construct a DataFrame from base Python objects, noticing how the DataFrame produces a table-like presentation of data but it's defined through a set of otherwise independent Series:

# COMMAND ----------

import pandas as pd

# define dataset as a dictionary consisting of three similarly sized lists of data elements
data = {
  'column 1':['a','b','c','d','e','f','g','h','i','j'],
  'column 2':[ 'val 1', 'val 2', 'val 3', 'val 4', 'val 5', 'val 6', 'val 7', 'val 8', 'val 9', 'val 10'],
  'column 3':[ 10,  20,  30,  40,  50,  10,  20,  30,  40,  50]
  }

# convert dataset to pandas DataFrame
df = pd.DataFrame(data)

# return DataFrame to output
df

# COMMAND ----------

# MAGIC %md Let's take a quick look at the *type* assigned to our DataFrame object:

# COMMAND ----------

# what type is the dataframe object?
type(df)

# COMMAND ----------

# MAGIC %md And let's take a look at the *type* assigned to each of the fields that make up the DataFrame:

# COMMAND ----------

# what type is each column in the dataframe?
print( type(df['column 1']) )
print( type(df['column 2']) )
print( type(df['column 3']) )

# COMMAND ----------

# MAGIC %md In the call to **pd.DataFrame** (a few cells back), we ask pandas to create a DataFrame from three provided lists.  Each list is given a name, *i.e.* *column 1*, *column 2* and *column 3*, and pandas builds Series for each of these.  While the DataFrame looks like a unified table object, the reality is that the position of each element in its Series is what determines how the different elements come together to form what appear to be rows. 
# MAGIC 
# MAGIC Why stress this point? As we introduce more sophisticated data manipulations using pandas DataFrames, this understanding of the DataFrame as a collection of Series becomes very important to understand.

# COMMAND ----------

# MAGIC %md Let's now examine the shape of our DataFrame object:

# COMMAND ----------

df.shape

# COMMAND ----------

# MAGIC %md The tuple associated with this property informs us that the DataFrame presents data across 10 rows leveraging 3 columns (Series).  When we describe a DataFrame as a two-dimensional object, it's the rows that form the first dimension (axis=0) and the columns that form the second dimension (axis=1):
# MAGIC 
# MAGIC <img src="https://smithbc.blob.core.windows.net/downloads/images/pandas_axis.png" width=200>
# MAGIC 
# MAGIC Understanding the shape of the DataFrame in this manner is very helpful for understanding some of the data access and manipulation operations to come.
# MAGIC 
# MAGIC Before moving on to those operations, let's take a look at one more method for examining the structure of the DataFrame:

# COMMAND ----------

df.info()

# COMMAND ----------

# MAGIC %md The results of the call to the **info()** method show us that the DataFrame has three columns.  Each of those colums has 10 non-Null or not **N**ot **a** **N**umber (**NaN**) values in *pandas-speak*. Each column is typed to hold a specific kind of data. 
# MAGIC 
# MAGIC The information retreived from this method call also shows us the DataFrame also employs an index. This is probably one of the most confusing concepts surrounding pandas DataFrames.  Let's take a moment to examine this concept, looking again at the data in our DataFrame:

# COMMAND ----------

df

# COMMAND ----------

# MAGIC %md Notice the results present the data in the three Series (columns) that make up the DataFrame, but it also exposes an unlabeled column on the far-left of the returned dataset. That is the index.  We can access the index directly using the **index** property of the DataFrame:

# COMMAND ----------

df.index

# COMMAND ----------

# MAGIC %md When a DataFrame is created and no field is identified as its index, pandas will generate an index as a range of values from 0 to the one minus the number of rows in the DataFrame. This default index often leads folks to assume the index uniquely identifies the rows in a DataFrame but that is not the case.  Instead, this index simply provides a label for each row, and those labels do not need to be unique. 
# MAGIC 
# MAGIC To see this in action, let's reset the index to a constant value of 'x':
# MAGIC 
# MAGIC **NOTE** When assigning a value to the index, you must pass a collection of values equivalent in size to the number of rows in the DataFrame.  Here, I've elected to create a list of 10 'x' values in it.  The number 10 is derived from the number of rows in the df as returned by **shape**.

# COMMAND ----------

df.index = list('x' * df.shape[0])
df

# COMMAND ----------

# MAGIC %md Setting the values of the index to 'x' is kinda non-sensical, but the the point we're making here is that the index is simply a row label.  We can use that label to assist us in filtering data or performing sorts and joins. But the index presented with the DataFrame (and also with the Series data) is not a unique identifier for a row.
# MAGIC 
# MAGIC Instead, each row and Series (column) is uniquely identified by it's position in the DataFrame and cells may be extracted using combinations of values from rows and columns, much like we would do with a set of lists(or tuples) within a list.  Here are the positional values associated with our 10 x 3 DataFrame:
# MAGIC 
# MAGIC |   | 0 | 1 | 2 |
# MAGIC |---|---|---|---|
# MAGIC | 0 | 0,0 | 0,1 | 0,2 |
# MAGIC | 1 | 1,0 | 1,1 | 1,2 |
# MAGIC | 2 | 2,0 | 2,1 | 2,2 |
# MAGIC | 3 | 3,0 | 3,1 | 3,2 |
# MAGIC | 4 | 4,0 | 4,1 | 4,2 |
# MAGIC | 5 | 5,0 | 5,1 | 5,2 |
# MAGIC | 6 | 6,0 | 6,1 | 6,2 |
# MAGIC | 7 | 7,0 | 7,1 | 7,2 |
# MAGIC | 8 | 8,0 | 8,1 | 8,2 |
# MAGIC | 9 | 9,0 | 9,1 | 9,2 |
# MAGIC 
# MAGIC Leveraging the **iloc** attribute of the DataFrame, we can reference these elements using these positional values:

# COMMAND ----------

# grab an individual cell
df.iloc[4,1]

# COMMAND ----------

# MAGIC %md Notice that the first position specified corresponds with axis 0, what we might think of as the rows of the DataFrame, and the second position corresponds with axis 1, the columns or Series of the DataFrame.  We can reference the elements of each axis using some standard Python notation:

# COMMAND ----------

df.iloc[ 1:3, 1]

# COMMAND ----------

# MAGIC %md In the previous cell, we extracted values from row positions 1 through 3 and column position 1.  Now, let's grab values from row position 1 and column positions 0 through 1:

# COMMAND ----------

df.iloc[ 1, 0:1]

# COMMAND ----------

# MAGIC %md Now let's grab positions 1 through 3 on the rows and 0 through 1 on the axis:

# COMMAND ----------

df.iloc[1:3, 0:1]

# COMMAND ----------

# MAGIC %md The data returned by each of the first two calls comes back in a funny looking structure but the last call returns something that looks a little nicer.  Let's examine the type of each of the returned objects to better understand what's coming back to us:

# COMMAND ----------

print( type(df.iloc[ 1:3, 1]) )
print( type(df.iloc[ 1, 0:1]) )
print( type(df.iloc[1:3, 0:1]) )

# COMMAND ----------

# MAGIC %md Whenever we grab a one-dimensional collection of values from a DataFrame, pandas will default it to a Series type. Series and DataFrames have different functionality and not understanding what is actually coming back generates the bulk of the frustration new developers have when working with pandas DataFrames.  
# MAGIC 
# MAGIC If our goal is to retrieve a single row or column of data but return it as a DataFrame, we need to vary our syntax just a bit:

# COMMAND ----------

df.iloc[ 1:3, [1]]

# COMMAND ----------

df.iloc[[1], 0:1]

# COMMAND ----------

df.iloc[1:3,0:1]

# COMMAND ----------

# MAGIC %md Using the square brackets with scalar references informs pandas that the associated axis should be treated as a collection (of one value) which forces it to return a DataFrame.  Notice that range references such as 1:3, 1:, :3, *etc.* automatically denote collections and should not be wrapped in square brackets.
# MAGIC 
# MAGIC Notice too that regardless of whether we are retrieving a Series or a DataFrame, the index values associated with each row come along for the ride.  
# MAGIC 
# MAGIC While this seems like a lot of minutea, the sooner you can make peace with DataFrames, Series and indexes, the sooner you will be productive with pandas.

# COMMAND ----------

# MAGIC %md ####Accessing DataFrame Fields
# MAGIC 
# MAGIC Let's now start accessing the fields of a pandas DataFrame in a more traditional manner, *i.e.* by Series names.  To access a field, simply provide its unique name to the DataFrame:

# COMMAND ----------

df['column 1']

# COMMAND ----------

# MAGIC %md What type of object was returned in the last call?  Why do you think that is and what can you do if you want to retrieve that one field as a new DataFrame?:

# COMMAND ----------

df[['column 1']]

# COMMAND ----------

# MAGIC %md To access multiple columns, simply provide a list of column names:

# COMMAND ----------

df[['column 1', 'column 3']]

# COMMAND ----------

# MAGIC %md In addition to retrieving subsets of columns from a DataFrame, you can use the **drop()** method to retreive all but the identified columns in the DataFrame: 

# COMMAND ----------

df.drop(['column 2'], axis=1)

# COMMAND ----------

# MAGIC %md Two things to notice about the **drop()** method.  First, you must identify that you wish the method to operate on the columns by specifying the axis you are affecting. Axis 1 references our columns/Series.  You can specify the string *'columns'* for the axis parameter but axis=1 is the most common way columns are referenced.
# MAGIC 
# MAGIC Second, note that drop doesn't modify the underlying DataFrame but instead returns a new DataFrame with the defined structure.  Here is our original DataFrame, fully intact:

# COMMAND ----------

df

# COMMAND ----------

# MAGIC %md **NOTE** If you did want **drop()** to modify the DataFrame, you can use the *inplace* parameter set to True to force an in-place overwrite.

# COMMAND ----------

# MAGIC %md Another way to access columns is through a mask.  A mask provides a True or False value for each position it operates on.  If we have three columns, a mask would consist of three values of either True or False:

# COMMAND ----------

mask = [False, True, True]

# COMMAND ----------

# MAGIC %md The mask is then applied using the **loc** attribute.  Notice how the first column in the DataFrame is masked as False and is not returned while the other two columns are masked as True and are returned:

# COMMAND ----------

df.loc[:, mask]

# COMMAND ----------

# MAGIC %md The **loc** attribute used in the last cell looks a lot like the **iloc** attribute except that you specify masks OR labels for the rows and columns you wish to retreive. To use **loc** with columns names, simply provide a list of the columns of interest:

# COMMAND ----------

df.loc[:, ['column 2', 'column 3']]

# COMMAND ----------

# MAGIC %md The value of the **loc** attribute is that it allows you to programmatically determine which rows and columns you'd like to return.  While it does work with rows AND columns, it is typically employed for row retrieval as demonstrated in the next section of this lab.

# COMMAND ----------

# MAGIC %md ####Accessing DataFrame Rows
# MAGIC 
# MAGIC While **loc** can be applied to columns, it's more typically applied to rows.  Here we'll use some simple logic to generate a mask for each row in our DataFrame:

# COMMAND ----------

mask = df['column 3'] >= 40
mask

# COMMAND ----------

# MAGIC %md Using our mask, we can now retrieve the appropriate rows from our DataFrame:

# COMMAND ----------

mask = df['column 3'] >= 40

df.loc[mask]

# COMMAND ----------

# MAGIC %md Apply filtering to rows through the use of masks is the most frequently employed means of subsetting DataFrames in pandas, so much so that a shorthand notation for doing this is available.  
# MAGIC 
# MAGIC Using the shorthand notation, we can explicitly assign a mask to our rows without **loc** as demonstrated here:

# COMMAND ----------

mask = df['column 3'] >= 40

df[mask]

# COMMAND ----------

# MAGIC %md And if we combine our mask generation logic with its application to the DataFrame, we could write our logic in one line of code like this:

# COMMAND ----------

df[ df['column 3']>=40 ]

# COMMAND ----------

# MAGIC %md Mask generation can employ complex Boolean logic.  For example, we might use the & (AND), | (OR), and ~ (NOT) operators to affect the mask we generate:

# COMMAND ----------

df[
  (df['column 3'] >= 20) &
  (df['column 1'].isin(['a','d','h','i'])) &
  ~(df['column 2']=='val 8')
]

# COMMAND ----------

# MAGIC %md Notice in the last example that the **isin()** method was used to perform an evaluation of column 1.  pandas provides a [large number of methods](https://pandas.pydata.org/pandas-docs/stable/reference/frame.html) that can be used to evaluate values in a DataFrame.

# COMMAND ----------

# MAGIC %md ####Generating New Columns of Data
# MAGIC 
# MAGIC pandas DataFrames allow developers to easily construct new columns of data.  For example, we might create a new column called *column 4* by dividing *column 3* by 10:

# COMMAND ----------

df['column 4'] = df['column 3']/10
df

# COMMAND ----------

# MAGIC %md The division operation is applied to each value in column 3 through a technique referred to as *broadcasting*.  The details of how broadcasting works isn't too important to us here but just note that it is very efficient and allows us to generate new values in a succint manner without looping through the rows of a DataFrame (or the values of a Series).
# MAGIC 
# MAGIC As we write more and more complex logic for generating values, we may wish to move our value generation logic to a user defined function:

# COMMAND ----------

def abs_from_mean( val ):
  return (val - val.mean()).abs()

df['column 5'] = abs_from_mean( df['column 3'] )

df

# COMMAND ----------

# MAGIC %md The trick to writing functions like this is remembering that the values being passed to the user-defined function are themselves Series (or DataFrames).  As such, you must apply methods to the incoming values (assigned to the function argument) with an awareness of what methods and attributes they support.

# COMMAND ----------

# MAGIC %md ####Analyzing DataFrame Data
# MAGIC 
# MAGIC The pandas library was built to allow developers to quickly perform analysis of their data. A popular method of this is to apply the **describe()** method to calculate summary statistics on all numeric fields in the DataFrame:

# COMMAND ----------

df.describe()

# COMMAND ----------

# MAGIC %md Of course, these can be calculated against individual columns using the appropriate method calls:

# COMMAND ----------

print( df['column 3'].count() )
print( df['column 3'].mean() )
print( df['column 3'].std() )
print( df['column 3'].min() )
print( df['column 3'].quantile(0.25) )
print( df['column 3'].quantile(0.50) ) # or print( df['column 3'].median() )
print( df['column 3'].quantile(0.75) )
print( df['column 3'].max() )

# COMMAND ----------

# MAGIC %md Where things get a little weird is when we apply these functions to the whole DataFrame:

# COMMAND ----------

df.median()

# COMMAND ----------

# MAGIC %md Remember earlier when we stated that a DataFrame is nothing more than a collection of Series.  This becomes a little more apparent here where we've applied a function to the DataFrame.  pandas applies that function to each Series in the DataFrame to produce the result you see here.
# MAGIC 
# MAGIC This can become a bit frustrating for developers new to pandas who simply want to count the number of rows in the DataFrame.  They are a little unclear as to why **count()** returns the count of non-NULL (non-NaN) values for each indiviaul Series instead of a singular number for the whole DataFrame:

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md When working with pandas, it's important to always remember the DataFrame is a collection of Series and many operations applied to the DataFrame are in fact applied to each individual Series in the DataFrame.
# MAGIC 
# MAGIC BTW, to get the total count of rows in a DataFrame, you can either apply count() to a single Series (being mindful that any NaN values will be ignored), extract the length of the index, or simply take the first value in the tuple associated with the shape property:

# COMMAND ----------

df['column 1'].count()

# COMMAND ----------

len(df.index)

# COMMAND ----------

df.shape[0]

# COMMAND ----------

# MAGIC %md With this heavy emphasis on Series-level calculations, it's important to note that some statistics are calculated between series.  For example, we might examine the correlation of numeric values with the **corr()** method:
# MAGIC 
# MAGIC **NOTE** The correlations between the fields are meaningless here.  This is simply a demonstration that some calculations are performed across the numeric Series in a DataFrame.

# COMMAND ----------

df.corr()

# COMMAND ----------

# MAGIC %md ####Extracting Numpy Arrays
# MAGIC 
# MAGIC Finally, it's important to note that pandas Series and DataFrames are backed by numpy arrays. [numpy](https://numpy.org/) is a specialized library for performing high-performance numerical calculations in Python. Quite often, we will load data into a pandas DataFrame, manipulate it using pandas funtionality, and then export the underlying numpy arrays for use in Machine Learning and other computationally intensive scenarios which expect the data in that particular format.
# MAGIC 
# MAGIC To extract the numpy array underlying a pandas DataFrame or Series, simply call the **values** property on it:

# COMMAND ----------

# exract an array from a dataframe
df_array = df.values
df_array

# COMMAND ----------

# extract an array from a series
series_array = df['column 5'].values
series_array

# COMMAND ----------

# MAGIC %md It's important to note that arrays extracted from a DataFrame and those extracted from a Series have different shapes:

# COMMAND ----------

df_array.shape

# COMMAND ----------

series_array.shape

# COMMAND ----------

# MAGIC %md Notice that the array extracted from a Series is a 1-dimensional array.  In scenarios where we need to convert this to a 2-dimensional array (as some ML algorithms will require), we can simply extract the Series data to a single-column DataFrame before extracting the values or apply **reshape()** to the 1-d array, specifying a value of 1 for the second axis:

# COMMAND ----------

# extract series as a 1d array
df['column 5'].values.shape

# COMMAND ----------

# extract series as a 1d array and then reshape to a 2d array
df['column 5'].values.reshape(-1, 1).shape 

# COMMAND ----------

# MAGIC %md Notice in the last cell that we used a -1 to indicate that the rows axis (axis 0) should not be modified with the **reshape** method call.

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC Given the df DataFrame created earlier in this lab, calculated a field named 'column 6' which calculates the modulo of 'column 4' by the value of 2.  This will force column 6 to contain a 0 when the value in column 3 is even and a 1 when the value in column 3 is odd.

# COMMAND ----------

#scenario 1 code here

# COMMAND ----------

# MAGIC %md ####Scenario 2
# MAGIC 
# MAGIC Building on Scenario 1, return 'column 1' and 'column 2' for those rows in the DataFrame for which 'column 6' represents an odd number.

# COMMAND ----------

#scenario 2 code here

# COMMAND ----------

# MAGIC   %md ####Answers (scroll down)
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

#scenario 1
df['column 6'] = df['column 4'] % 2
df

# COMMAND ----------

#scenario 2
df[ df['column 6']==1 ][['column 1', 'column 2']]