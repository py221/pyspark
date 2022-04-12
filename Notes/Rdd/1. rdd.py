# Databricks notebook source
# MAGIC %md
# MAGIC #### rdd intro

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <b> Spark context </b>
# MAGIC 
# MAGIC <div class="highlight"><pre><span></span>  <span class="k">class</span> <span class="nc">pyspark</span><span class="o">.</span><span class="n">SparkContext</span> <span class="p">(</span>
# MAGIC      <span class="n">master</span> <span class="o">=</span> <span class="kc">None</span><span class="p">,</span>
# MAGIC      <span class="n">appName</span> <span class="o">=</span> <span class="kc">None</span><span class="p">,</span> 
# MAGIC      <span class="n">sparkHome</span> <span class="o">=</span> <span class="kc">None</span><span class="p">,</span> 
# MAGIC      <span class="n">pyFiles</span> <span class="o">=</span> <span class="kc">None</span><span class="p">,</span> 
# MAGIC      <span class="n">environment</span> <span class="o">=</span> <span class="kc">None</span><span class="p">,</span> 
# MAGIC      <span class="n">batchSize</span> <span class="o">=</span> <span class="mi">0</span><span class="p">,</span> 
# MAGIC      <span class="n">serializer</span> <span class="o">=</span> <span class="n">PickleSerializer</span><span class="p">(),</span> 
# MAGIC      <span class="n">conf</span> <span class="o">=</span> <span class="kc">None</span><span class="p">,</span> 
# MAGIC      <span class="n">gateway</span> <span class="o">=</span> <span class="kc">None</span><span class="p">,</span> 
# MAGIC      <span class="n">jsc</span> <span class="o">=</span> <span class="kc">None</span><span class="p">,</span> 
# MAGIC      <span class="n">profiler_cls</span> <span class="o">=</span>
# MAGIC   <span class="p">)</span>
# MAGIC </pre></div>

# COMMAND ----------

# MAGIC %md
# MAGIC ##### spark func

# COMMAND ----------

sc.version

# COMMAND ----------

sc.pythonVer

# COMMAND ----------

sc.defaultMinPartitions

# COMMAND ----------

sc.defaultParallelism

# COMMAND ----------

# appName is the application name used when logging information in the Spark logs
sc.appName

# COMMAND ----------


