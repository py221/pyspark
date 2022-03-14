# Databricks notebook source
# MAGIC %md 
# MAGIC ###JSON: An Intro to JSON
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to introduce you to JSON data structures.

# COMMAND ----------

# MAGIC %md JavaScript Object Notation (JSON, pronounced *JAY-sawn*) is a lightweight format for the exchange of data. It evolved out the the need to save and send the state of objects between browsers and web servers as JavaScript emerged as a popular scripting language for interactive web pages. Today, JSON has taken off as a popular way to encode information as each JSON *document* self-describes its structure.
# MAGIC 
# MAGIC The full JSON standard is available at https://www.json.org/, but it really boils down to a few basic ideas:
# MAGIC 
# MAGIC 1. All data is encapsulated as a structure containing one or more key-value pairs
# MAGIC 2. Values may be simple, scalar values or complex values such as arrays or a nested JSON structure
# MAGIC 3. Nested JSON structures give rise to a hierarchical document structure
# MAGIC 3. Keys must be unique given their position in the JSON document hierarchy
# MAGIC 
# MAGIC Let's start with the first of thes concepts, the key-value pair.

# COMMAND ----------

# MAGIC %md Here is a simple JSON document representing the basic information we might capture about a customer on an e-commerce site:
# MAGIC 
# MAGIC ```
# MAGIC     {
# MAGIC        "id":"AW00011000",
# MAGIC        "first_purchase":"2005-07-22",
# MAGIC        "email":"jon24@adventure-works.com",
# MAGIC        "first_name":"Jon",
# MAGIC        "last_name":"Yang"
# MAGIC     }```
# MAGIC     
# MAGIC You might recognize the JSON looks an awful lot like a Python dictionary. That's a great way to think about JSON.
# MAGIC 
# MAGIC If we think about JSON as something akin to a Python dictionary, the basic rules for encoding data as JSON are self-explainatory.  The one key thing to note is that while strings can be encoded with either double-quotes or single-quotes in Python, double-quotes are always used in JSON.  Also, True, False and None values are encoded as (lower-case) *true*, *false*, and *null* in JSON.

# COMMAND ----------

# MAGIC %md The value assigned to a key in a JSON document can be a simple scalar value.  But it can also be another JSON structure (much like a Python dictionary can include another dictionary as a value). Nesting a child JSON structure within a parent JSON structure provides a simple way to capture related data in a single JSON *document*. To illustrate this, consider this JSON document representing a customer profile as might be captured on an ecommerce website:
# MAGIC ```
# MAGIC     {
# MAGIC        "id":"AW00011000",
# MAGIC        "firstpurchase":"2005-07-22",
# MAGIC        "name":{
# MAGIC           "first":"Jon",
# MAGIC           "middle":"V",
# MAGIC           "last":"Yang"
# MAGIC        },
# MAGIC        "contact":{
# MAGIC           "email":"jon24@adventure-works.com",
# MAGIC           "phone":"1 (11) 500 555-0162"
# MAGIC        },
# MAGIC        "address":{
# MAGIC           "line1":"3761 N. 14th St",
# MAGIC           "city":"Rockhampton",
# MAGIC           "stateprovince":"QLD",
# MAGIC           "country":"AU"
# MAGIC        },
# MAGIC        "demographics":{
# MAGIC           "maritalstatus":"M",
# MAGIC           "gender":"M",
# MAGIC           "yearlyincome":"90000",
# MAGIC           "totalchildren":"2",
# MAGIC           "numberchildrenathome":"0",
# MAGIC           "education":"Bachelors",
# MAGIC           "occupation":"Professional",
# MAGIC           "houseownerflag":"1",
# MAGIC           "numbercarsowned":"0",
# MAGIC           "commutedistance":"1-2 Miles",
# MAGIC           "region":"Pacific",
# MAGIC           "age":"52"
# MAGIC        },
# MAGIC        "modified_dt":"2019-01-21T16:40:53.772Z"
# MAGIC     }```
# MAGIC     
# MAGIC This one JSON document provides all the information we might know about a customer as part of a profile. Top-level key-value pairs provide access to critical profile information such as an id and a last modified date. Nested structures such as address, contact and demographics allow us to organize subsets of data in an easy to follow manner. 

# COMMAND ----------

# MAGIC %md The previous JSON document presents customer profile data in a nested structure that in many ways looks like Python dictionaries within Python dictionaries.  But what if one of our values was like a Python list? Take a look at the last_five_purchases item just below the firstpurchase key-value pair:
# MAGIC 
# MAGIC ```
# MAGIC     {
# MAGIC        "id":"AW00011000",
# MAGIC        "firstpurchase":"2005-07-22",
# MAGIC        "last_five_purchases":[
# MAGIC          {"date":"2019-10-01","product_id":939002},
# MAGIC          {"date":"2019-10-01","product_id":38840},
# MAGIC          {"date":"2019-05-17","product_id":939002},
# MAGIC          {"date":"2019-01-21","product_id":1227},
# MAGIC          {"date":"2018-11-04","product_id":139403}
# MAGIC        ],
# MAGIC        "name":{
# MAGIC           "first":"Jon",
# MAGIC           "middle":"V",
# MAGIC           "last":"Yang"
# MAGIC        },
# MAGIC        "contact":{
# MAGIC           "email":"jon24@adventure-works.com",
# MAGIC           "phone":"1 (11) 500 555-0162"
# MAGIC        },
# MAGIC        "address":{
# MAGIC           "line1":"3761 N. 14th St",
# MAGIC           "city":"Rockhampton",
# MAGIC           "stateprovince":"QLD",
# MAGIC           "country":"AU"
# MAGIC        },
# MAGIC        "demographics":{
# MAGIC           "maritalstatus":"M",
# MAGIC           "gender":"M",
# MAGIC           "yearlyincome":"90000",
# MAGIC           "totalchildren":"2",
# MAGIC           "numberchildrenathome":"0",
# MAGIC           "education":"Bachelors",
# MAGIC           "occupation":"Professional",
# MAGIC           "houseownerflag":"1",
# MAGIC           "numbercarsowned":"0",
# MAGIC           "commutedistance":"1-2 Miles",
# MAGIC           "region":"Pacific",
# MAGIC           "age":"52"
# MAGIC        },
# MAGIC        "modified_dt":"2019-01-21T16:40:53.772Z"
# MAGIC     }```
# MAGIC     
# MAGIC The square brackets associated with the last_five_purchases value indicate the value is an array (list).  Each item in the list is a self-contained JSON snippet.  Much like Python lists, JSON arrays are ordered.

# COMMAND ----------

# MAGIC %md JSON provides an easy to follow, relatively compact way of organizing data, but it is rarely as nicely formatted as the previous example.  Instead, most JSON is organized as a flattened string like this:
# MAGIC ```
# MAGIC     {"id":"AW00011000","firstpurchase":"2005-07-22","name":{"first":"Jon","middle":"V","last":"Yang"},"last_five_purchases":[{"date":"2019-10-01","product_id":939002},{"date":"2019-10-01","product_id":38840},{"date":"2019-05-17","product_id":939002},{"date":"2019-01-21","product_id":1227},{"date":"2018-11-04","product_id":139403}],"contact":{"email":"jon24@adventure-works.com","phone":"1 (11) 500 555-0162"},"address":{"line1":"3761 N. 14th St","city":"Rockhampton","stateprovince":"QLD","country":"AU"},"demographics":{"maritalstatus":"M","gender":"M","yearlyincome":"90000","totalchildren":"2","numberchildrenathome":"0","education":"Bachelors","occupation":"Professional","houseownerflag":"1","numbercarsowned":"0","commutedistance":"1-2 Miles","region":"Pacific","age":"52"},"modified_dt":"2019-01-21T16:40:53.772Z"}
# MAGIC ```
# MAGIC 
# MAGIC While the information in this JSON document is identical to that of the previous document, the formatting makes it difficult for humans to read.  Thankfully, there are several freely available JSON formatters, such as https://jsonformatter.curiousconcept.com/ and https://jsonformatter.org/, available which make navigating a JSON document much easier. Alternatively, if you are working offline, you can simply save a JSON document to a text file with a *.json* extention and then open that file with a browser such as Chrome or Firefox for a prettified representation.

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC Try crafting a JSON document representing your information as a student at SMU. Include in your document information about your first, last and preferred name as well as information on your current course load.  Validate your JSON using one of the formatters identified in the last cell.

# COMMAND ----------

# your JSON here

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

{
  "id":"01234567",
  "name":{
    "first":"Kathryn",
    "last":"Smith",
    "preferred":"Kate"
    },
  "matriculation_date":"2019-08-08",
  "expected_graduation_date":"2020-05-06",
  "current_course_load":[
    {"id":"ITOM 6269", "name":"Web and Social Media Analytics"},
    {"id":"ITOM 6258", "name":"Big Data Development"},
    {"id":"MNGT 6101", "name":"Managing Your Career"}
    ] 
}