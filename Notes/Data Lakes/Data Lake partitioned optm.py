# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### partitin by specific cols

# COMMAND ----------

# MAGIC %md 
# MAGIC This data will also be partitioned around ApprovalDate.  What this means is that under our folder that represents the table, there will be subfolders, one for each ApprovalDate value.  Within these subfolders, data for that ApprovalDate will be stored in one or more parquet files. Partitioning data in this way can help us in managing data which we may wish to delete as it gets old. But the more important reason to partition our data this way is to boost query performance.  If we know that most queries will filter data based on ApprovalDate values, the Spark SQL engine is smart enough to recognize that only a subset of folders needs to be read in order for the query to be resolved.  Subfolders for approval dates not needed will simply be skipped.

# COMMAND ----------

spark.sql('create database if not exists fda')

df_repart = df.repartition(2 ,df.ApprovalDate)

df_repart.write.saveAsTable('fda.approvals', partitionBy='ApprovalDate', mode='overwrite', format='parquet')

# COMMAND ----------

# MAGIC %sql 
# MAGIC --show tables from fda
# MAGIC describe formatted fda.approvals

# COMMAND ----------

# MAGIC %md
# MAGIC With the saveAsTable leveraging the overwrite mode, each time the script is run, the old version of the approvals table will be dropped and recreated. We will then have a dataset that reflects what has been scrapped from the FDA website for the current or previous day but as that data only goes 14 days back, we will no longer have access to data from dates prior to that.  If we change the mode to append, we preserve all the data but duplicate information on partitions that we previously created and which are still reflected on the website.
# MAGIC 
# MAGIC In a production version of this routine, we would implement logic to preserve historical data without confusingly duplicating information.  One simple way to doing this is to partition our data not on approval date but on scrape date.  We will have dupicates for various approval dates but these could be separated based on scrape date.  This would allow us to simply keep historical data and also to look for things like changes to drug approvals after initial publication.  Of course, this would trip up our more casual users so that we'd need to put a view in place to help them just see the latest version of any record.  Such a routine might look like this:

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


