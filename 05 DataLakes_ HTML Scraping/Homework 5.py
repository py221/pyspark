# Databricks notebook source
# MAGIC %md ###General Instructions
# MAGIC In this assignment, you will need to complete the code samples where indicated to accomplish the given objectives. **Be sure to run all cells** and export this notebook as an HTML with results included.  Upload the exported HTML file to Canvas by the assignment deadline.

# COMMAND ----------

# MAGIC %md ####Assignment
# MAGIC Unlike previous exercises, you will not be provided any sample code from which to work.  You are given a starting point and an expected outcome.  Use what you have learned in class and in labs to complete the exercise as outlined below. Break your logic up into cells that tackle webpage retrieval, HTML parsing, and DataFrame assembly and persistence.

# COMMAND ----------

# MAGIC %md Board Game Geek is an online site that provides rankings for various boardgames.  A complete listing of ranked games is provided here: https://boardgamegeek.com/browse/boardgame
# MAGIC 
# MAGIC The data on this page is organized in a simple table format. The Title field is divided into the game's title and the year it was published. The title itself provides a link to more information on the game.
# MAGIC 
# MAGIC Write a routine to scrape this data from the page and persist it as a partitioned Parquet table named games.board_games.  Use the current date as the partitioning field.
# MAGIC 
# MAGIC The fields required in the final table are:
# MAGIC 
# MAGIC * Rank - the integer value from the Board Game Rank column
# MAGIC * ImageURL - the URL (as provided) of the image presented in the second column 
# MAGIC * Title - the title of the game
# MAGIC * YearPublished - the year the game was published
# MAGIC * GeekRating - the float value presented in the Geek Rating field
# MAGIC * AvgRating - the float value presented in the Avg Rating field
# MAGIC * NumVoters - the integer value presented in the NumVoters field
# MAGIC 
# MAGIC (You can ignore the Shop field)
# MAGIC 
# MAGIC While the Board Game Geek site has multiple pages of rankings, limit your scraping to the games presented on the page referenced above.  (In other words, don't hit the other 1000+ pages of rankings.)
# MAGIC 
# MAGIC Once you've populated your table, run the two SQL statements presented at the bottom of this notebook.  Save your notebook (with the results) as an HTML file and load it to Canvas.

# COMMAND ----------

# run this code to make sure beautiful soup is installed
dbutils.library.installPyPI('beautifulsoup4')
dbutils.library.restartPython()

# COMMAND ----------

# retrieve the web page
from urllib import request as url
from bs4 import BeautifulSoup

url_desired = 'https://boardgamegeek.com/browse/boardgame'
response = url.urlopen(url.Request(url_desired))

print(f'parsed=desired?\t{url_desired == response.geturl()}\nurl: {url_desired}')
bs = BeautifulSoup(response)

# COMMAND ----------

print(bs)

# COMMAND ----------

# extract the data from the web page      
## extracting data into list under list 
game_lst =  []
for tbl_row in bs.find_all('tr', attrs = {'id':'row_'}):
  
  rank = tbl_row.find('td', attrs={'class':'collection_rank'}).text.strip()
  
  img_url = tbl_row.find('img')['src']
  
  title = tbl_row.find('a', attrs={'class':'primary'}).text
  
  year_published = tbl_row.find('span', attrs={'class':'smallerfont dull'}).text.strip(f'\(\)')

  ratings = [rating.text.strip() for rating in tbl_row.find_all('td', attrs={'class':'collection_bggrating'})]
  geek_rating = ratings[0]
  avg_rating = ratings[1]
  num_voters = ratings[2]
  
  temp_lst = [rank, img_url, title, year_published, geek_rating, avg_rating, num_voters]
  game_lst += [temp_lst]
                                  
    
      
## rdd conversion
rdd_game_geek = sc.parallelize(game_lst)
rdd_game_geek.sample(False, 0.1).take(8)

# COMMAND ----------

# convert the data to a Spark SQL table
from pyspark.sql.types import * 
from datetime import datetime 
import copy
import re 


    
# type casting function for correct dtype
def type_casting(lst):
  lst_output = copy.deepcopy(lst)
  for i in range(len(lst)):
    lst_itm = lst[i]   
    if isinstance(lst_itm, datetime):
      pass    
    elif re.match(r'^\d*(?<=\d)\.(?=\d*)\d*', lst_itm):
      lst_output[i] = float(lst_itm)     
    elif re.match(r'(?<!\s\w)^\d+$(?!\w\s)',lst_itm):
      lst_output[i] = int(lst_itm)     
    else: 
      pass 
  return lst_output



# type casting to rdd 
rdd_game = (
  rdd_game_geek
    .map(lambda field: type_casting(field))
  )

# rdd_game.sample(False, 0.1).take(5)



# Structurizing spark.df field 
game_schema = StructType([
  StructField('rank', IntegerType()),
  StructField('img_url', StringType()),
  StructField('title', StringType()),
  StructField('year_published', IntegerType()),
  StructField('geek_ranking', FloatType()),
  StructField('avg_ranking', FloatType()),
  StructField('num_voters', IntegerType())
  ])

# Converting rdd to spark.df
df_game = spark.createDataFrame(rdd_game, schema=game_schema)
df_game.show(5)



# creating database & tmp tbl
spark.sql('create database if not exists games')
# set df_game to be partitioned by year_published but in ordered rank 
df_games = df_game.repartition(df_game.year_published)
# write into parquet fmt tbl
df_games.write.saveAsTable('games.board_games', partitionBy='year_published', mode='overwrite', format='parquet')

# COMMAND ----------

# MAGIC %md Execute the following SQL statements to validate your results:

# COMMAND ----------

# MAGIC %sql -- verify row count
# MAGIC 
# MAGIC SELECT COUNT(*) FROM games.board_games;

# COMMAND ----------

# MAGIC %sql -- verify values
# MAGIC 
# MAGIC SELECT * FROM games.board_games ORDER BY rank ASC;

# COMMAND ----------

# MAGIC %sql -- verify partitioning
# MAGIC DESCRIBE EXTENDED games.board_games;