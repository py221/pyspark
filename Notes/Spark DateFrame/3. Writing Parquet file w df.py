# Databricks notebook source
spark.conf.set("spark.sql.parquet.compression.codec", "gzip")
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY")

# COMMAND ----------

df.write.format('parquet')\
        .mode('Overwrite')\
        .save("<path_for_file>")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables from rm_workspace

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe table extended rm_workspace.comp_anomaly_join

# COMMAND ----------


