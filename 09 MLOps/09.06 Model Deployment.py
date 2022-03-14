# Databricks notebook source
# MAGIC %md 
# MAGIC ###MLOps: Model Deployment
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to introduce you to how to turn models into functions you can employ in your environment.

# COMMAND ----------

dbutils.library.installPyPI("mlflow")
dbutils.library.installPyPI("azureml-sdk", extras="databricks") 
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md Businesses do not pay Data Scientists to produce machine learning models.  They pay Data Scientists to transform business processes and to drive different, better outcomes. The models Data Scientists produce are simply a means to those ends.
# MAGIC 
# MAGIC But how do we take a machine learning model and turn it into something useable, like a function or an application?  That's the challenge we will tackle in this lab.
# MAGIC 
# MAGIC To get started, let's recreate our pipeline for clustering customers based on their departmental spend with a regional wholesaler:

# COMMAND ----------

from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import RobustScaler
from sklearn.compose import ColumnTransformer
from sklearn.cluster import KMeans
from sklearn.pipeline import Pipeline

import pandas as pd
import numpy as np

# read data to pandas
spend = spark.read.csv(
  'wasbs://downloads@smithbc.blob.core.windows.net/wholesale_customers/', 
  header=True, 
  inferSchema=True
  )
spend_df = spend.toPandas()

# write transformation logic
transformer = ColumnTransformer([
  ('ohe_encoder', OneHotEncoder(categories='auto'), [0, 1]),
  ('robust_scaler', RobustScaler(), [2,3,4,5,6,7])
  ])

# configure kmeans learner
km = KMeans(
  n_clusters=4, 
  init='random',
  n_init=1000
  )

#assemble pipeline
clf = Pipeline(steps=[
  ('transform', transformer),
  ('clustering', km)
  ])  

# fit the model
_ = clf.fit(spend_df)  # use _ to supress output

# COMMAND ----------

# MAGIC %md We now have a model, assigned to the clf variable, to which we can pass a record. When we pass a record (with the predict method), the data in the record are transformed and a cluster assignment is returned:

# COMMAND ----------

data = {
  'Channel':[1],
  'Region':[2],
  'Fresh':[ 1000 ],
  'Milk':[ 1000 ],
  'Grocery':[ 200000 ],
  'Frozen':[ 1000 ],
  'Detergents_Paper':[ 1000 ],
  'Delicassen':[ 1000 ]
  }

data_df = pd.DataFrame(data)

predict = clf.predict(data_df)

print(predict)

# COMMAND ----------

# MAGIC %md Cluster assignment is useful in a business scenario.  Let's say that customers of this wholesaler have to pay an annual membership fee.  After a given customer's first year of spending, we can assign that customer to a cluster and based on cluster assignment make targetted offers to encourage membership renewal. 
# MAGIC 
# MAGIC There are many more ways cluster assignment could be useful to the business so that it becomes important that we turn this cluster assignment capability into a function we can employ in our environment.
# MAGIC 
# MAGIC As cluster assignment in this scenario depends on us having a year's worth of data in hand, it seems reasonable that we can process customer data on a monthly, weekly or even nighly basis, identifying customers at or nearing their one-year anniversary and assigning them to clusters at that time.  That cluster assignment data may then be persisted within the Data Lake or event sent to a relational database for utilization by our applications. 
# MAGIC 
# MAGIC In this scenario, our model serves as a capability in an batch ETL pipeline.  So let's examine how we might persist the model between pipeline runs and employ it with each cycle. 

# COMMAND ----------

# MAGIC %md The simpliest way to persist our model is to take it's in-memory image and simply write it to storage.  In Python, this is done with the **pickle** library.  The in-memory representation of the model is written to storage as a **pickle** file which can be re-read later, placed back into memory, and re-used in the state it was previously:

# COMMAND ----------

import pickle

# save pipeline-model to pickel file
filename = '/tmp/clf.pickle'
pickle.dump(clf, open(filename, 'wb'))

# COMMAND ----------

# read model from pickle file
clf2 = pickle.load(open(filename, 'rb'))

# use model to make predictions
clf2.predict(data_df)

# COMMAND ----------

# MAGIC %md Persisting models to pickle files works when the developer simply wants to store the model and return it to its previous state.  The developer needs to understand how the model is employed which implies some basic knowledge of SciKit-Learn.  While this is great for the Data Scientist, Data Engineers and traditional Application Developers may not have this knowledge.  Instead, they may prefer to wrap the model in a function interface that works in a more standardized manner.  Within Spark, this is handled through a **pandas user-defined function (pandas_udf)**, but because writing a custom pandas_udf is highly complex, new capabilities such as **mlflow** have emerged to make function deployment much simpler:

# COMMAND ----------

import mlflow
import mlflow.sklearn
import mlflow.pyfunc

# record/save model with mlflow 
with mlflow.start_run() as run:
  mlflow.sklearn.log_model(clf, 'cluster_model')

# define function based on mlflow recorded model
assign_cluster_mlflow = mlflow.pyfunc.spark_udf(
  spark, 
  'runs:/{0}/cluster_model'.format(run.info.run_id), 
  result_type='int'
  )

# COMMAND ----------

# MAGIC %md With our model now deployed as a function, we can call it in a relatively simple manner:

# COMMAND ----------

display(
  spend
    .withColumn('cluster', 
                assign_cluster_mlflow(
                  spend.Channel,
                  spend.Region,
                  spend.Fresh,
                  spend.Milk,
                  spend.Grocery,
                  spend.Frozen,
                  spend.Detergents_Paper,
                  spend.Delicassen
                  )
               )
      )

# COMMAND ----------

# MAGIC %md We can even register it for use in a SQL statement:

# COMMAND ----------

spend.createOrReplaceTempView('spend')
_ = spark.udf.register('assign_cluster_sql', assign_cluster_mlflow)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   *,
# MAGIC   assign_cluster_sql(Channel,Region,Fresh,Milk,Grocery,Frozen,Detergents_Paper,Delicassen) as `cluster`
# MAGIC FROM spend

# COMMAND ----------

# MAGIC %md Using a pickle file or registering a model as a function is a great way to enable batch (and even real-time) scoring of data from within the Spark environment, but what about scenarios where we wish to deploy a model as service that can be used by external, operational applications?  In these scenarios, we need to recognize that Spark is not built to be integrated into an operational infrastructure.  Instead, we should look to take our model and deploy it as a stand-alone application that can be run in a fault-tolerant and scaleable infrastructure.
# MAGIC 
# MAGIC Today, this means deploying our model as an application in a light-weight Docker container.  A Docker container can be thought of as a lightweight virtual machine which includes our application code and its dependencies, *e.g.* SciKit-Learn, etc., but which runs on top of other virtual machines or physical machines, piggy-backing on the code installed there.
# MAGIC 
# MAGIC Within the Docker container, we typically present our model as an application with REST API.  A REST API is a universally accessible programmatic layer which can be called over the Internet.  External applications simply send data to our REST API, the REST API passes the data to our model, the model scores the data and hands that score back to the REST AP, and the REST API sends the data back to the calling application.  Security, of course, is implemented through the REST API to ensure the calling application should have the rights to leverage our functionality, but otherwise this is a simple architecture for application integration.
# MAGIC 
# MAGIC There are several ways to deploy your model to a Docker container and to assemble the REST API around it, but again, we will leverage **mlflow** to simplify this whole process.  It's important to note that the Docker image it creates must be persisted somewhere.  We will make use of the image repository associated with the Microsoft Azure ML Service for this purpose.  
# MAGIC 
# MAGIC **NOTE** You do not have access to this service so that the following code is for demonstration purposes only.

# COMMAND ----------

# import azureml
# from azureml.core import Workspace
# import mlflow.azureml

# # select model from mlflow respository
# model_uri = 'runs:/{0}/cluster_model'.format(run.info.run_id)

# # connect to azureml workspace
# workspace = Workspace.create(name = "b.......l",
#                             location = "eastus",
#                             resource_group = "b.......l",
#                             subscription_id = "3f............................8b",
#                             exist_ok=True)

# # build an azureml image using my model
# model_image, azure_model = mlflow.azureml.build_image(model_uri=model_uri, 
#                                                      workspace=workspace, 
#                                                      model_name="cluster-assignment",
#                                                      image_name="cluster-assignment",
#                                                      description="My Model for Assignment of Customers to Clusters based on Spend",
#                                                      synchronous=False)
# # wait for image deploy to complete
# model_image.wait_for_creation(show_output=True)

# COMMAND ----------

# MAGIC %md OUTPUT:<br>
# MAGIC Registering model cluster-assignment<br>
# MAGIC Succeeded<br>
# MAGIC Image creation operation finished for image cluster-assignment:1, operation "Succeeded"<br>
# MAGIC Command took 6.51 minutes -- by smithbc@smu.edu at 2/15/2020, 12:08:34 PM on test

# COMMAND ----------

# MAGIC %md With our image deployed, we can now launch the image in a container service.  While Kubernetes is the most popular container service available today, Microsoft makes avaialble a lower-cost service called Azure Container Instances that is good for testing.  We'll deploy to this service instead of the more expensive Azure Kubernetes Service for this demonstration:

# COMMAND ----------

# # now deploy webservice
# from azureml.core.webservice import AciWebservice, Webservice

# # deploy image as a service
# dev_webservice_name = "clustering-model"

# dev_webservice_deployment_config = AciWebservice.deploy_configuration(cpu_cores = 1, memory_gb = 1)

# dev_webservice = Webservice.deploy_from_image(
#    name=dev_webservice_name, 
#    image=model_image, 
#    deployment_config=dev_webservice_deployment_config, 
#    workspace=workspace
#  )

# # wait for deployment to complete
# dev_webservice.wait_for_deployment()
# print()

# # retrieve the URL for the REST API
# print( dev_webservice.scoring_uri )

# COMMAND ----------

# MAGIC %md ACI service creation operation finished, operation "Succeeded"<br>
# MAGIC http://ae4e0061-24f6-4105-9bc6-c6a00779fc86.centralus.azurecontainer.io/score<br>
# MAGIC Command took 3.40 minutes -- by smithbc@smu.edu at 2/15/2020, 12:17:01 PM on test

# COMMAND ----------

# MAGIC %md Now, let's call our service to see how it responds to a unit of data being passed to it:

# COMMAND ----------

# import requests
# import json

# # input data sets
# query_input = {
#  'columns': ['Channel', 'Region', 'Fresh', 'Milk', 'Grocery', 'Frozen', 'Detergents_Paper', 'Delicassen'],
#  'data': [[1,1,1000,1000,1000,1000,1000,1000]]
#  }

# # post input and get response
# headers = {"Content-Type": "application/json"}

# response = requests.post(dev_webservice.scoring_uri, data=json.dumps(query_input), headers=headers)

# # parse and print response
# preds = json.loads(response.text)
# print(preds)

# COMMAND ----------

# MAGIC %md OUTPUT:<br>
# MAGIC [1]<br>
# MAGIC Command took 0.43 seconds -- by smithbc@smu.edu at 2/15/2020, 12:21:13 PM on test

# COMMAND ----------

# MAGIC %md ** The sample output in the last cell may differ from what was shown above but the concept of passing a JSON structure to the API to return a cluster assignment should be apparent.