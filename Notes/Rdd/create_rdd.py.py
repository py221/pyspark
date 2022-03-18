# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### rdd w sc.range()

# COMMAND ----------

import numpy as np
values = sc.range(0, 10000000)
print(type(values))

# COMMAND ----------

values.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### rdd w sc.parallelize(<lst_obj>)

# COMMAND ----------

lst = ['p1', 'p2', 'p3', 1, 2, 3, {'p4': 4}]
rdd_parallelize = sc.parallelize(lst)
rdd_parallelize.map(lambda item: type(item)).collect()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


