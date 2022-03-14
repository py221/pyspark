# Databricks notebook source
# MAGIC %md 
# MAGIC ###Data Lakes: Partitioned Tables
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to introduce you to table partitioning in SparkSQL.

# COMMAND ----------

# MAGIC %md You now know a bit about the structure of an HTML document, how to retrieve one from a public website and how to programmatically pull information from it.  Now, let's use this knowledge to load data from a public webpage into a queriable Spark table. 
# MAGIC 
# MAGIC Let's start by returning to the FDA Approvals data we investigated in the last exercise. Let's pull that data down into a Python list:

# COMMAND ----------

dbutils.library.installPyPI('beautifulsoup4')
dbutils.library.restartPython()

# COMMAND ----------

from urllib import request as url
from bs4 import BeautifulSoup

def cleanse_text( text ):
    return ' '.join(text.split())

# retrieve the FDA approvals web page for parsing
request = url.Request('https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=report.page')
response = url.urlopen(request)
html = response.read()
soup = BeautifulSoup(html)

# navigate to the block that holds the tables
div1 = soup.find('div', attrs={'class':'tab-content'})
div2 = div1.find('div', attrs={'role':'tabpanel', 'class':'tab-pane fade in active'})

# grab the tables 
tables = div2.find_all('table')

# grab the dates that serve as table titles
dates = div2.find_all('h4')

# initialize table data collection variables
table_data = []
t = 0  # keep track of which table we are on

# for each table:
for table in tables:
    
    # get date for this table
    date = dates[t].text   
    
    # get table rows
    table_rows = table.find('tbody').find_all('tr')
    
    # for each row in table
    for table_row in table_rows:
        
        # reset row data list
        row_data = [date]
        
        # for each cell in row
        for row_cell in table_row.find_all('td'):
            
            # add the cells text to the row data
            row_data += [cleanse_text(row_cell.text)]
            
            # see if there is a link for this cell
            link = row_cell.find('a')
            
            # if there is a link:
            if link is not None:
                # get the link's href
                row_data += ['https://www.accessdata.fda.gov'+link['href']]
            
        # append row data to table data 
        table_data += [row_data]
    
    # update index for next table in loop
    t += 1
    
print(table_data[0:5])

# COMMAND ----------

# MAGIC %md Looking closely at the data (and at the webpage), we can see that each approval in the table has the following information:
# MAGIC 
# MAGIC <ol start="0">
# MAGIC <li>Approval Date in a MonthName, Day, Year format</li>
# MAGIC <li>Combined Drug, Application Type & Application Number string</li>
# MAGIC <li>A link to the application document</li>
# MAGIC <li>Active Ingredient</li>
# MAGIC <li>Dose & optional Route information string</li>
# MAGIC <li>Submission</li>
# MAGIC <li>Company</li>
# MAGIC <li>Submission Classification</li>
# MAGIC <li>Submission Status</li>
# MAGIC </ol>
# MAGIC 
# MAGIC Some of these elements can be used as is, and others need to be cleaned up a bit:

# COMMAND ----------

from datetime import datetime

def clean_data( d ):
    # initialize return list
    clean = []
    
    # transform date to standard format
    clean += [datetime.strptime(d[0], '%B %d, %Y')]
    
    # separate Drug, Application Number, & Application Type fields
    # NDA and BLA are the only known application types in this code
    if d[1].find('NDA #') != -1:
        clean += d[1].split('NDA #')
        clean += [u'NDA']
    elif d[1].find('BLA #') != -1:
        clean += d[1].split('BLA #')
        clean += [u'BLA']
    else:
        raise 'Application Type not recognized not found in Drug/AppNum field: {0}'.format(d[1])
    
    # link to application document
    clean += [d[2]]
    
    # active ingredient
    clean += [d[3]]
    
    # separate dose & route; route may not be present
    dose_route = d[4].split(';')
    dose = dose_route[0].strip()
    if len(dose_route)==2:
        route = dose_route[1].strip()
    else:
        route = ''
    clean += [dose]
    clean += [route]
    
    # submission
    clean += [d[5]]
    
    # company
    clean += [d[6]]
    
    # submission classification
    clean += [d[7]]
    
    # submission status
    clean += [d[8]]

    return clean

raw = sc.parallelize( table_data )
clean = raw.map(clean_data)
clean.take(1)

# COMMAND ----------

# MAGIC %md With the data pretty well cleaned up, let's flip this into a Spark dataframe:

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([
    StructField('ApprovalDate', DateType()),
    StructField('Drug', StringType()),
    StructField('ApplicationNumber', StringType()),
    StructField('ApplicationType', StringType()),
    StructField('ApplicationURL', StringType()),
    StructField('ActiveIngredient', StringType()),
    StructField('Dosage', StringType()),
    StructField('Route', StringType()),
    StructField('Submission', StringType()),
    StructField('Company', StringType()),
    StructField('SubmissionClassification', StringType()),
    StructField('SubmissionStatus', StringType()),
    ])

df = spark.createDataFrame( clean, schema=schema )
#df.show()
df.count()

# COMMAND ----------

# MAGIC %md Before moving forward with this data, it's important to note that the FDA often publishes duplicate entries in their approval tables.  We need to perform a DISTINCT on this data to ensure we don't have redundant records being recorded.  (This is specific to this data source.)

# COMMAND ----------

df = df.distinct()
df.count()

# COMMAND ----------

# MAGIC %md There is a lot of data we might wish to record from the FDA website.  Let's assume we wish to collect all this and make it persistently queriable. To facilitate this, we might create a database and persist our dataframe to a perminent table within it:

# COMMAND ----------

spark.sql('create database if not exists fda')

df_repart = df.repartition(2 ,df.ApprovalDate)

df_repart.write.saveAsTable('fda.approvals', partitionBy='ApprovalDate', mode='overwrite', format='parquet')

# COMMAND ----------

# MAGIC %sql 
# MAGIC --show tables from fda
# MAGIC describe formatted fda.approvals

# COMMAND ----------

# MAGIC %md There's a lot going on in that last code block that we need to discuss.  First, Spark SQL allows us to define one or more virtual databases.  When objects are placed in these databases, the database itself becomes a folder in our filesystem, with most of the folders representing it's tables associated with that database under it. 
# MAGIC <p>
# MAGIC Skip to the last line of code where we save our data to a table. Our table will be named approvals and will sit in the fda database. If the table already exists, we will overwrite it.  The storage format for our table will be Parquet. 
# MAGIC     
# MAGIC This data will also be partitioned around ApprovalDate.  What this means is that under our folder that represents the table, there will be subfolders, one for each ApprovalDate value.  Within these subfolders, data for that ApprovalDate will be stored in one or more parquet files. Partitioning data in this way can help us in managing data which we may wish to delete as it gets old. But the more important reason to partition our data this way is to boost query performance.  If we know that most queries will filter data based on ApprovalDate values, the Spark SQL engine is smart enough to recognize that only a subset of folders needs to be read in order for the query to be resolved.  Subfolders for approval dates not needed will simply be skipped.
# MAGIC <p>
# MAGIC If you are curious about how the persisted table is stored, you can get more information about it by describing it. Notice the Location property which tells us where in the file system the data is actually stored:

# COMMAND ----------

spark.sql('describe formatted fda.approvals').show(50, truncate=False)

# COMMAND ----------

# MAGIC %md With our data persisted, we can query it as we did before:

# COMMAND ----------

spark.sql('show tables in fda').show()

# COMMAND ----------

sql = '''
select
   approvaldate,
   count(*) as approvals
from fda.approvals
group by approvaldate
order by approvaldate
'''

spark.sql( sql ).show()

# COMMAND ----------

# MAGIC %md A final thought about this exercise .... Web scrapping routines such as this are often executed on a pre-defined schedule aligned with the frequency of updates on the targetted website.  If we know the FDA updates this dataset daily, we might run this script once a day to ensure we have up to date data at our disposal.
# MAGIC 
# MAGIC With the saveAsTable leveraging the overwrite mode, each time the script is run, the old version of the approvals table will be dropped and recreated. We will then have a dataset that reflects what has been scrapped from the FDA website for the current or previous day but as that data only goes 14 days back, we will no longer have access to data from dates prior to that.  If we change the mode to append, we preserve all the data but duplicate information on partitions that we previously created and which are still reflected on the website.
# MAGIC 
# MAGIC In a production version of this routine, we would implement logic to preserve historical data without confusingly duplicating information.  One simple way to doing this is to partition our data not on approval date but on scrape date.  We will have dupicates for various approval dates but these could be separated based on scrape date.  This would allow us to simply keep historical data and also to look for things like changes to drug approvals after initial publication.  Of course, this would trip up our more casual users so that we'd need to put a view in place to help them just see the latest version of any record.  Such a routine might look like this:

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS fda.approvals_history;
# MAGIC -- this is needed because of the use of the delta lake format at the bottom of the lab

# COMMAND ----------

from pyspark.sql.functions import current_date

df_with_scrape_date = df_repart.withColumn('ScrapeDate', current_date())
df_with_scrape_date.write.saveAsTable(
    'fda.approvals_history', 
    partitionBy=['ScrapeDate', 'ApprovalDate'], 
    mode='append', 
    format='parquet'
    )

sql_statement = '''
create view if not exists fda.approvals_current as
    select a.*
    from fda.approvals_history a
    join (
        select
            a.applicationnumber,
            max(a.scrapedate) as scrapedate
        from fda.approvals_history a
        group by a.applicationnumber
        ) b
        on  a.applicationnumber=b.applicationnumber AND
            a.scrapedate=b.scrapedate
    '''

spark.sql(sql_statement)

# COMMAND ----------

spark.sql('show tables in fda').show()

# COMMAND ----------

spark.sql('describe formatted fda.approvals_history').show(50, truncate=False)

# COMMAND ----------

spark.sql('describe formatted fda.approvals_history').show(50, truncate=False)

# COMMAND ----------

spark.sql('select * from fda.approvals_current').show()

# COMMAND ----------

# MAGIC %md If that looks like a lot of work, it is.  And for that reason, a variant of Parquet called [Delta Lake](https://delta.io/) has been introduced into the open source space. Delta Lake is fundamentally Parquet but it implements a transaction log which allows it to bring forward some advanced functionality such as [updates, inserts and deletes along with a merge](https://docs.delta.io/latest/delta-update.html#table-deletes-updates-and-merges) which serves as a combination of all three data modification steps in a single statement.
# MAGIC 
# MAGIC Delta Lake is still pretty new and there are alternatives competing for the same position in the Hadoop/Big Data stack, but Delta Lake is pretty impactful. To see this in action, let's save our fda.approvals_history table as a Delta Lake table:

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS fda.approvals_history;
# MAGIC -- drop the table so that it can be re-created as a delta table (delta will not overwrite non-delta tables)

# COMMAND ----------

from pyspark.sql.functions import current_date

df_with_scrape_date = df_repart.withColumn('ScrapeDate', current_date())

# create original table
df_with_scrape_date.write.saveAsTable(
    'fda.approvals_history', 
    partitionBy=['ApprovalDate'], 
    mode='overwrite', 
    format='delta'
    )

# COMMAND ----------

# MAGIC %md Not all that much different from what we did originally.  But now, let's pretend it's the next day and we need to update this table with new and potentially overlapping approvals data:

# COMMAND ----------

# MAGIC %md **NOTE** The FDA uniquely identifies approvals as a combination of Application Number, Application Type, Drug, Dosage and Submission. 

# COMMAND ----------

df_with_scrape_date.createOrReplaceTempView('todays_scrape')

merge_sql = '''
  merge into fda.approvals_history as target
  using todays_scrape as source
    on 
      target.applicationnumber=source.applicationnumber and 
      target.applicationtype=source.applicationtype and
      target.drug=source.drug and
      target.dosage=source.dosage and
      target.submission=source.submission
  when matched then update set *
  when not matched then insert *
'''

spark.sql(merge_sql)

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC Revisit the NYSE Pricing data found at /tmp/nyse/. Load that data into a DataFrame using schema inferrence. Using the year() function available through pyspark.sql.functions, calculate a new column (using the withColumn method of the DataFrame) to hold the year derived from the DataFrame's date field. 
# MAGIC 
# MAGIC Save the DataFrame as a Parquet-backed table named 'nyse.pricing_partitioned'. Be sure to partition the data on the newly calculated year field. 

# COMMAND ----------

 # your scenario 1 code here

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

# scenario 1 code here
from pyspark.sql.functions import year

# read the data to a dataframe
pricing = spark.read.csv(
  'wasbs://downloads@smithbc.blob.core.windows.net/nyse/', 
  header=True, 
  inferSchema=True,
  dateFormat='yyyy-MM-dd\'T\'HH:mm:ss.SSSXXX'
  )

# calculate the year column
pricing_with_year = pricing.withColumn('year', year(pricing['date']))

# persist the dataframe as a table
spark.sql('CREATE DATABASE IF NOT EXISTS nyse ')

pricing_with_year.write.saveAsTable(
    name='nyse.pricing_partitioned',
    partitionedBy='year',
    format='parquet',
    mode='overwrite'
  )

display( spark.sql('SELECT * FROM nyse.pricing_partitioned') )

# COMMAND ----------

