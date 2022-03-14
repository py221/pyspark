# Databricks notebook source
# MAGIC %md 
# MAGIC ###MLOps: Categorical Values
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to review how to handle categorical values leveraging SciKit-Learn.

# COMMAND ----------

# install the most recent version of sklearn to avoid a problem with OHE
dbutils.library.installPyPI('scikit-learn', version='0.22.1')
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md Quite often we will encounter categorical data in our datasets.  Take for example the Gap Minder dataset employed in earlier exercises:

# COMMAND ----------

import pandas as pd

df = (
  spark
    .read
    .csv(
      'wasbs://downloads@smithbc.blob.core.windows.net/gapminder/gapminder.tsv',
      sep='\t',
      header=True,
      inferSchema=True
      )
  ).toPandas()

df = df.drop('country', axis=1)

df.head()

# COMMAND ----------

# MAGIC %md Here, we have two columns, *i.e.* Country and Continent, which hold categorical information.  Most Machine Learning algorithms cannot work with this kind of data.  But ignoring this data may mean we are missing an opportunity to learn of a pattern associated with it. For example, consider how life expectancy differs by continent. Do we really want to drop the continent field from our dataset and lose the information the field provides?:

# COMMAND ----------

# life expectancy by continent
df.boxplot('lifeExp', 'continent', rot=60)
display()

# COMMAND ----------

# MAGIC %md But how might we leverage categorical information when our ML algorithms require numerical data?  
# MAGIC 
# MAGIC One strategy is to "partition" our dataset on the categorical values and then build separate models for each value. For example, if we have 5 continents in our GapMinder dataset, we might partition the data by continent and build separate models for each one:
# MAGIC 
# MAGIC **NOTE** In a Spark cluster, if we had a large number of models to generate, we'd distribute the work to the workers, much like what we did with the hyperparameter tuning exercise.

# COMMAND ----------

from sklearn.linear_model import LinearRegression
from sklearn.model_selection import cross_val_score

import numpy as np
import pandas as pd

# get unique values from continent field
continents = df['continent'].unique()

# for each continent
for continent in continents:
  
  # extract that continent's data
  continent_df = df[ df['continent']==continent ]
  
  # grab features and labels
  X = continent_df[['gdpPercap','pop']].values
  y = continent_df['lifeExp'].values

  # build a regression model for the continent
  reg = LinearRegression()
  cv_scores = cross_val_score(reg, X, y, cv=5)

  # print the score for this model
  print("Average 5-Fold CV Score for {0}: {1}".format(continent, np.mean(cv_scores)))

# COMMAND ----------

# MAGIC %md This strategy works when we have a manageable number of category values and each value itself has sufficient data on which we can train our model.  Still, we have to evaluate, tune, deploy, *etc.* each model independent of the others so that manageability often becomes an overriding concern.
# MAGIC 
# MAGIC Another way we can tackle categorical data such as these is to convert the category values into numerical values.  Here is a simplistic approach to this problem within which we assign each continent an unqiue, ordinal value:
# MAGIC 
# MAGIC **NOTE** You can allow the OrdinalEncoder to automatically determine the categorical values in a field and an ordinal assignment for each.  Here, I've provided the encoder an explicit list of values in my preferered order in order to ensure results are consistent across the lab.

# COMMAND ----------

from sklearn.preprocessing import OrdinalEncoder

# train encoder on continent field
encoder = OrdinalEncoder()

#X = df['continent'].values.reshape(-1,1)
#encoder.fit(X)
encoder.fit([
  ['Africa'], 
  ['Americas'],
  ['Asia'],
  ['Europe'],
  ['Oceania']
])

# create encoded continent field
df['continent_encoded'] = encoder.transform(
    df['continent'].values.reshape(-1,1) 
  )

df.head(20)

# COMMAND ----------

# MAGIC %md As many Machine Learning algorithms explore the "distance" between variables, this strategy of assigning arbitrary ordinal values can often lead to some meaningless results.  For example, if we assign 'Asia' a value of 2 and 'Americas' a value of 3, does a difference of 1 between these two continents really mean anything?
# MAGIC 
# MAGIC With this in mind, a more common approach to tackling categorical data is to create a new field for each unique value in a categorical field and assign those fields values of 0 or 1 if the field corresponds to the categorical value on a given row.  Having worked with pandas, you've likely come across this through the **get_dummies** method:

# COMMAND ----------

pd.get_dummies(
  df, 
  columns=['continent']
  ).head(20)

# COMMAND ----------

# MAGIC %md In the get_dummies example above, we have 5 values for continent: Africa, Americas, Asia, Europe, & Oceania.  This leads to the creation of 5 dummy variable fields, the values of which are assigned based on this lookup pattern:
# MAGIC 
# MAGIC |continent| continent_Africa  |  continent_Americas | continent_Asia  |  continent_Europe | continent_Oceania |
# MAGIC |---|---|---|---|---|
# MAGIC |Africa   |  1 | 0  | 0  | 0  | 0 |
# MAGIC |Americas   |  0 |  1 |0   |  0 | 0 |
# MAGIC |Asia   |  0 | 0  | 1 |   0| 0 |
# MAGIC |Europe   |  0 | 0  | 0  | 1 | 0 |
# MAGIC |Oceania   |  0 |  0 |  0 | 0  | 1 |
# MAGIC 
# MAGIC If you think carefully about the data, you might recognize that only 4 fields are actually needed to represent the 5 values in the continent column:
# MAGIC 
# MAGIC |continent|  continent_Americas | continent_Asia  |  continent_Europe | continent_Oceania |
# MAGIC |---|---|---|---|
# MAGIC |Africa   |  0 |0   |  0 | 0 |
# MAGIC |Americas   |  1 |0   |  0 | 0 |
# MAGIC |Asia   |  0  | 1 |   0| 0 |
# MAGIC |Europe   |  0  | 0  | 1 | 0 |
# MAGIC |Oceania   |  0 |  0 | 0  | 1 |
# MAGIC 
# MAGIC Dropping the first dummy variable field, *i.e.* continent_Africa, does not cause us to lose any information.  Rows assigned the continent value of 'Africa' will have 0s across all the remaining dummy variables.  Removing this first dummy variable saves us memory but more importantly elimates co-linearity between the dummy variables.

# COMMAND ----------

# MAGIC %md **NOTE** The get_dummies method creates an alphabetically sorted list of category values and drops the first which is why continent_Africa is dropped in the data set below.

# COMMAND ----------

pd.get_dummies(
  df, 
  columns=['continent'],
  drop_first=True
  ).head(20)

# COMMAND ----------

# MAGIC %md Encoding our categorical data this way allows us now to calculate the "distance" between categorical values in a meaningful way that doesn't skew our results.
# MAGIC 
# MAGIC With that said, you are encouraged not to use **get_dummies** with your datasets if you are intended to train your data using SciKit-Learn.  Why this is the case should become more apparent when we introduce the concept of a Pipeline, but for now, just note that you are encouraged to use what SciKit-Learn (and the rest of the ML community) refers to as a "One-Hot Encoder" (OHE).
# MAGIC 
# MAGIC Before implementing the OHE, let's review the structure of our DataFrame:

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md Let's now build a one-hot encoding for the content_encoded field:

# COMMAND ----------

from sklearn.preprocessing import OneHotEncoder

# train OHE on first feature in the features array
ohe = OneHotEncoder(
  drop='first',  
  sparse=False)
ohe.fit(df[['continent_encoded']].values)

# apply encoding
X_ohe = ohe.transform(df[['continent_encoded']].values)

# COMMAND ----------

X_ohe

# COMMAND ----------

# MAGIC %md Why did we apply the OHE to the continent_encoded field and not continent directly?  The sklearn OHE requires that categorical fields be numeric.  With this in mind, you typically will apply an Ordinal Encoder to your categorical data and then apply the OHE to that encoded field.
# MAGIC 
# MAGIC Notice too that we had to explicitly state we wanted to drop one of the fields to avoid the colinearlity issue described earlier.  The OHE allows you to drop the first categorical value or a specific one that you specify.  Here, we've elected to drop the first encountered.

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC Using the Melbourne Housing dataset, build a one-hot encoding for the Type field.  This field identifies houses by three values of type: u, h & t.  You will need to perform an ordinal encoding on the field before performing the one-hot encoding. Your result should return two one-hot encoded fields for the three unique values in the Type field.

# COMMAND ----------

# make data available at /dbfs/tmp/melbourne/melb_data.csv
df = spark.read.csv(
  'wasbs://downloads@smithbc.blob.core.windows.net/melbourne_housing/melb_data.csv', 
  sep=',', 
  header=True,
  inferSchema=True
  ).toPandas()

# COMMAND ----------

#scenario 1

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

# scenario 1 code
import pandas as pd
import numpy as np

from sklearn.preprocessing import OrdinalEncoder
from sklearn.preprocessing import OneHotEncoder


# configure & apply the ordinal encoder
encoder = OrdinalEncoder()
encoder.fit(df[['Type']].values)
df['Type_encoded'] = encoder.transform( df[['Type']].values )

# configure & apply the OHE
ohe = OneHotEncoder(drop='first', sparse=False)
ohe.fit(df[['Type_encoded']].values)
X_ohe = ohe.transform(df[['Type_encoded']].values)

X_ohe

# COMMAND ----------



# COMMAND ----------

