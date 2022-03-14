# Databricks notebook source
# MAGIC %md 
# MAGIC ###JSON: Saving JSON Data
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to explore how JSON is written from a Spark dataframe.

# COMMAND ----------

# MAGIC %md Consider this data set generated from our JSON profile data.  Notice that I'm combining keys and values into a map to make the reference of data in the **named_struct** function easier. This pattern is being used because a *PIVOT* statement is not available in Hive, nor is an aggregation function that merges individual maps or structures into a single structure.  This SQL is more advanced than you will need to know for your homework assignment but I figured it be good to demonstrate a solution to a problem you might encounter in the wild as we prepare our dataset:

# COMMAND ----------

profiles = spark.read.json('/tmp/json/profiles/profiles.json')
profiles.createOrReplaceTempView('profiles')

# reassemble data as {state:state, education:{education_level:profile_count}}
sql = '''
select
    y.state,
    named_struct(
        'partial_high_school', coalesce(y.education['Partial High School'],0),
        'high_school', coalesce(y.education['High School'],0),
        'partial_college', coalesce(y.education['Partial College'],0),
        'bachelors', coalesce(y.education['Bachelors'],0),
        'graduate_degree', coalesce(y.education['Graduate Degree'],0)
        ) as education
from (
    select
        x.state,
        map(x.education, x.profiles) as education
    from ( 
        select
            p.address.stateprovince as state,
            p.demographics.education as education, 
            count(*) as profiles
        from profiles p
        where p.address.country='US'
        group by p.address.stateprovince, p.demographics.education
        ) x
    ) y
            
order by state
'''

education = spark.sql(sql)
education.printSchema()

# COMMAND ----------

display(education)

# COMMAND ----------

# MAGIC %md I now have assembled a result set with a nested structure. If you didn't follow how we go here, don't sweat it.  The important thing is to understand the structure of education DataFrame as it exists now. 
# MAGIC 
# MAGIC Now, to save the data in a JSON format, we do so using the [JSON writer](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter.json):

# COMMAND ----------

education.write.json('/tmp/education_json/', mode='overwrite')

# COMMAND ----------

# MAGIC %md With the data now saved to JSON, we should be able to read it back into Spark with structural integrity:

# COMMAND ----------

ed_from_json = spark.read.json('/tmp/education_json/')
ed_from_json.printSchema()
ed_from_json.show(2, truncate=False)

# COMMAND ----------

# MAGIC %md The same is true if we save the data to Parquet:

# COMMAND ----------

education.write.parquet('/tmp/education_parquet/', mode='overwrite')

# COMMAND ----------

ed_from_parquet = spark.read.parquet('/tmp/education_parquet/')
ed_from_parquet.printSchema()
ed_from_parquet.show(2, truncate=False)

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC From the profiles DataFrame, save the customer's id and demographic information as JSON at /tmp/scenario_1_json/.

# COMMAND ----------

 # your scenario 1 code here

# COMMAND ----------

# MAGIC %md ####Scenario 2
# MAGIC 
# MAGIC Using the profiles DataFrame, retrieve the customer's id and demographic information.  Flatten the data so that id and all items under demographics are nested at the same level (as shown below).  Save the output **as tab-delimited text** to /tmp/scenario_2_json/.
# MAGIC 
# MAGIC **HINT**  You can use demographics.* to extract all the fields under demographics without having to specify each one individually.

# COMMAND ----------

# your scenario 2 code here

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

# scenario 1
(
  profiles
    .select(profiles.id, profiles.demographics)
    .write
    .json('/tmp/scenario_1_json/', mode='overwrite')
  )

# COMMAND ----------

# scenario 2
display(
  profiles
    .select( 'id', 'demographics.*' )
    .write
    .csv(
      '/tmp/scenario_2_json/', 
      sep='\t', 
      header=True,
      mode='overwrite'
      )
  )

# COMMAND ----------

