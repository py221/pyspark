# Databricks notebook source
# MAGIC %md 
# MAGIC ###JSON: Querying JSON Data
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to explore how query the nested structures of a Spark dataframe derived from JSON.

# COMMAND ----------

# MAGIC %md Given the potential complexity of JSON documents, you may be curious how we can query these using a SQL paradigm. SQL expects sets of data consisting of rows of fields. With JSON, we are dealing with rows of fields but some of those fields contain their own rows of fields. The translation of SQL syntax to JSON structures is remarkably straightforward despite these differences.  Let's reload the profile data used in the last lab to explore this.  Take a quick moment to review the schema, noting the name field is a structure containing fields first, last and middle:

# COMMAND ----------

profiles = spark.read.json('/tmp/json/profiles/profiles.json')
profiles.createOrReplaceTempView('profiles')

display(profiles)

# COMMAND ----------

# MAGIC %md With our profile JSON data loaded into a Spark dataframe and that dataframe registered as a temporary view, we can query fields much like we did before.  In this query, we retrieve the id and name fields from our profile data:

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select
# MAGIC     p.id,
# MAGIC     p.name
# MAGIC from profiles p

# COMMAND ----------

# programmatic SQL version of previous query
display( 
  profiles.select( 
    profiles.id, 
    profiles.name
    ) 
  )

# COMMAND ----------

# MAGIC %md Notice that the name field is a structure, presented in our results as a dictionary-like format.  If we refer to the fields within this structure using an extended dot notation, we can generate a flattened set:

# COMMAND ----------

# MAGIC %sql 
# MAGIC select
# MAGIC     p.id,
# MAGIC     p.name.first as first_name,
# MAGIC     p.name.middle as middle_name,
# MAGIC     p.name.last as last_name
# MAGIC from profiles p

# COMMAND ----------

# programmatic SQL version of previous query

display( 
  profiles.select( 
    profiles.id, 
    profiles.name.first, 
    profiles.name.middle, 
    profiles.name.last
    ) 
  )

# COMMAND ----------

# MAGIC %md The trick to accessing fields within fields is simply to provide the name of the parent field, append a dot, and then provide the name of the child field.  This works whether the nesting is one or multiple levels deep.  Just add a dot and provide the name of the subordinate field until you've navigated to the element of interest.
# MAGIC 
# MAGIC This dot-notation works whether we are referencing fields in the SELECT list or any other part of the query:

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select
# MAGIC     p.address.stateprovince as state,
# MAGIC     count(*) as profiles
# MAGIC from profiles p
# MAGIC where 
# MAGIC     p.address.country='US' AND
# MAGIC     p.demographics.education='Graduate Degree'
# MAGIC group by p.address.stateprovince
# MAGIC order by profiles DESC

# COMMAND ----------

# programmatic SQL version of previous query

display( 
  profiles
    .filter( (profiles.address.country == 'US') & (profiles.demographics.education=='Graduate Degree') )
    .groupBy( profiles.address.stateprovince )
    .count()
    .withColumnRenamed('count', 'profiles')
    .orderBy('profiles', ascending=[0])
  )

# COMMAND ----------

# MAGIC %md In a SQL query, we can use functions to create new fields in our result set.  Consider this query where we grab a few dates from our profiles data and use the current_timestamp function to determine the current date and time:

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select
# MAGIC     p.id,
# MAGIC     p.firstpurchase,
# MAGIC     p.modified_dt,
# MAGIC     current_timestamp()
# MAGIC from profiles p

# COMMAND ----------

# programmatic SQL version of previous query

from pyspark.sql.functions import current_timestamp

display(
  profiles
    .withColumn('current_timestamp', current_timestamp())
    .select( profiles.id, profiles.firstpurchase, profiles.modified_dt, 'current_timestamp')
  )

# COMMAND ----------

# MAGIC %md Using functions to create fields in our result set, we are not limited to the creation of scalar values.  Using the named_struct function, we can create a field that itself represents a structure.  Consider this query where we take the three date time values shown in the previous query to assemble a field called dates that contains each of these values:

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select
# MAGIC     p.id,
# MAGIC     named_struct(
# MAGIC         'first_purchase', p.firstpurchase, 
# MAGIC         'modified', p.modified_dt, 
# MAGIC         'now', current_timestamp()
# MAGIC         ) as dates
# MAGIC from profiles p

# COMMAND ----------

# programmatic SQL version of previous query

from pyspark.sql.functions import current_timestamp, struct

display(
  profiles
    .withColumn('dates', 
        struct(
          (profiles.firstpurchase).alias('first_purchase'),
          (profiles.modified_dt).alias('modified'),
          (current_timestamp()).alias('now')
          )       
        )
    .select( profiles.id, 'dates')
  )

# COMMAND ----------

# MAGIC %md The dates field is a structure containing three subordinate fields: first_purchase, modified, and now.  To reference one of these, we simply use our dot-notation:

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select
# MAGIC     x.id,
# MAGIC     x.dates.now
# MAGIC from (
# MAGIC     select
# MAGIC         p.id,
# MAGIC         named_struct(
# MAGIC             'first_purchase', p.firstpurchase, 
# MAGIC             'modified', p.modified_dt, 
# MAGIC             'now', current_timestamp()
# MAGIC             ) as dates
# MAGIC     from profiles p
# MAGIC     ) x

# COMMAND ----------

# programmatic SQL version of previous query

from pyspark.sql.functions import current_timestamp, struct

profile_dates = (
  profiles
    .withColumn('dates', 
        struct(
          (profiles.firstpurchase).alias('first_purchase'),
          (profiles.modified_dt).alias('modified'),
          (current_timestamp()).alias('now')
          )       
        )
    .select( profiles.id, 'dates')
  )

display(
  profile_dates.select(profile_dates.id, profile_dates.dates.now)
  )

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC Using the profiles DataFrame as defined in this lab, write a query (using either SQL or the programmatic SQL API) to answer the following question:
# MAGIC 
# MAGIC What is the average annual income level of customers by country and state?  (Yearly income is found in the demographics data. Note that it is typed as a String due to the manner in which the JSON file was encoded upon extract.  While SQL can perform implicit conversions, the programmatic SQL API will require you to explicitly cast the field to an integer or float before allowing you to perform an average.)

# COMMAND ----------

# your Scenario 1 code here

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

# MAGIC %sql -- scenario 1 code
# MAGIC 
# MAGIC SELECT
# MAGIC   address.country as country,
# MAGIC   address.stateprovince as state,
# MAGIC   AVG(demographics.yearlyincome) as avg_income
# MAGIC FROM profiles
# MAGIC GROUP BY profiles.address.country, profiles.address.stateprovince
# MAGIC ORDER BY country, state;

# COMMAND ----------

# scenario 1
from pyspark.sql.types import *

display (
  profiles
    .select(
      profiles.address.country.alias('country'), 
      profiles.address.stateprovince.alias('state'), 
      profiles.demographics.yearlyincome.cast(FloatType()).alias('income')
      )
    .groupBy('country','state')
    .avg('income').alias('avg_income')
    .orderBy('country', 'state')
  )