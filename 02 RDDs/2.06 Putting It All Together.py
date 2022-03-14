# Databricks notebook source
# MAGIC %md 
# MAGIC ###RDDs & PairRDDs: Putting It All Together
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to reinforce what you've learned about RDDs and pair RDDs by presenting you with more complex data processing challenges.

# COMMAND ----------

# MAGIC %md Access the airline flights data in **wasbs://downloads@smithbc.blob.core.windows.net/flights/flights.csv**.  This data is row-delimited text with commas used as the field separators. Assemble an RDD where each element is represents one line from this file split so that each field is an item in a list:

# COMMAND ----------

flights = (
  sc.textFile('wasbs://downloads@smithbc.blob.core.windows.net/flights/flights.csv')
    .map(lambda flight: flight.split(',') )
  )

flights.take(1)

# COMMAND ----------

flights.take(1)

# COMMAND ----------

# MAGIC %md Looking at the data, you can probably guess what some of the fields are, but some others are harder to understand.  So with that in mind, here are the  fields, in order and with some simple descriptions:</p>
# MAGIC  
# MAGIC  0.month  - the month number of the year, *i.e.* 1-12<br>
# MAGIC  1.dayofmonth - the day number of the month, *i.e.* 1-31<br>
# MAGIC  2.dayofweek - the day number of the week, *i.e.* 1-7<br>
# MAGIC  3.deptime - the time of departure in hhmm notation<br>
# MAGIC  4.arrtime - the time of arrival in hhmm notation<br>
# MAGIC  5.uniquecarrier - the FAA carrier code<br>
# MAGIC  6.flightnum - the carrier's flight number<br>
# MAGIC  7.tailnum - the tail number of the plane<br>
# MAGIC  8.elapsedtime - the time from departure to arrival<br>
# MAGIC  9.airtime - the time in the air<br>
# MAGIC 10.arrdelay - the minutes of arrival delay<br>
# MAGIC 11.depdelay - the minutes of departure delay<br>
# MAGIC 12.origin - the airport of orgin<br>
# MAGIC 13.dest - the airport of destination<br>
# MAGIC 14.distance - the flight distance in miles<br>
# MAGIC 15.taxiin - the minutes of landing to arrival at the terminal gate<br>
# MAGIC 16.taxiout - the minutes from departure from terminal gate to take off<br>
# MAGIC 17.cancelled - 0 = not cancelled, 1 = cancelled<br>
# MAGIC 18.cancellationcode - the FAA code indicating the reason for canecellation<br>
# MAGIC 19.diverted - 0 = not diverted, 1 = diverted

# COMMAND ----------

# MAGIC %md Now, let's access information about carriers found in **wasbs://downloads@smithbc.blob.core.windows.net/flights/carriers.csv**.  As before, this is a CSV textfile:

# COMMAND ----------

carriers = (
  sc.textFile('wasbs://downloads@smithbc.blob.core.windows.net/flights/carriers.csv')
    .map(lambda c: c.split(','))
  )
carriers.take(5)

# COMMAND ----------

# MAGIC %md The structure of the carriers data set is fairly simple with the carrier code identified in the first field and the friendly name of the carrier identified in the second.  The carrier code should align with values in the uniquecarrier field found in the flights dataset.
# MAGIC 
# MAGIC Now, let's explore one last comma-separated text file, **wasbs://downloads@smithbc.blob.core.windows.net/flights/airports.csv**:

# COMMAND ----------

airports = (
  sc.textFile('wasbs://downloads@smithbc.blob.core.windows.net/flights/airports.csv')
    .map(lambda a: a.split(','))
    .filter(lambda a: a[0] != 'iata') # remove header line
    .map(lambda a: [a[0], ', '.join(a[1:5])] + a[1:])
  )
airports.take(1)

# COMMAND ----------

# MAGIC %md The original airports dataset consists of these fields:</p>
# MAGIC 
# MAGIC 0.airport (iata) code<br>
# MAGIC 1.short airport name<br>
# MAGIC 2.city name<br>
# MAGIC 3.state/province abbreviation<br>
# MAGIC 4.country abbreviation<br>
# MAGIC 5.latitude<br>
# MAGIC 6.longitude<br>
# MAGIC 
# MAGIC The iata code field should map to the orgin and dest fields in the flights data set.
# MAGIC 
# MAGIC To make things a bit easier later, we've filtered out a header line and added a long airport name to the field list:</p>
# MAGIC 
# MAGIC 0.airport (iata) code<br>
# MAGIC 1.long airport name (short airport name, city name, state abbrev, country abbrev)<br>
# MAGIC 2.short airport name<br>
# MAGIC 3.city name<br>
# MAGIC 4.state/province abbreviation<br>
# MAGIC 5.country abbreviation<br>
# MAGIC 6.latitude <br>
# MAGIC 7.longitude<br>
# MAGIC 
# MAGIC With these datasets in-hand, let's answer some questions:

# COMMAND ----------

# MAGIC %md ####Question 1a
# MAGIC 
# MAGIC During the time period associated with this data set, which airlines had the longest average arrival delays?

# COMMAND ----------

# assemble pair RDD in the form (carrier, (arrdelay, distance, count))
data = flights.map(lambda flight: (flight[5], ( int(flight[10]), int(flight[14]), 1) ) )

# should we filter out non-delays? 
# should we set negative delay to zero?
# maybe, but for our exercise, let's ignore these concerns.

# calculate total arrival delay, total distance, total flight count by carrier
totals = data.reduceByKey( lambda value, accum: ( 
    value[0] + accum[0],  
    value[1] + accum[1],  
    value[2] + accum[2] 
    ))

# calculate total arrival delay, avg delay per flight, average delay per mile flown
delays = totals.mapValues( lambda delay: ( 
    delay[0], 
    delay[0]/float(delay[2]), 
    delay[0]/float(delay[1]) 
    ))

# rekey on total arrival delay, sort by total arrival delay in descending order, and then extract original k-v pair
ordered = (
  delays.map(lambda d: (d[1][0], d))
    .sortByKey(ascending=False)
    .values()
  )

# (carrier, (total arrival delay, avg delay per flight, avg delay per mile flown))
ordered.take(10)

## because so many have asked, here is one technique for flattening the structure of each element:
## carrier, total arrival delay, avg delay per flight, avg delay per mile flown
#(
#  ordered
#    .map( lambda d: [d[0]] + [x for x in d[1]])
#    .take(10)
#)

# COMMAND ----------

# MAGIC %md ####Question 1b
# MAGIC 
# MAGIC Given the answer to the last question, convert the carrier code to friendly names so that we better understand which carriers had the worst arrival delays.

# COMMAND ----------

# inputs
#   delays: (carrier, (total arrival delay, avg delay per flight, avg delay per mile flown))
#   carriers: carrier, name

# (carrier, ((tot arrival delay, avg delay per flight, avg delay per mile flown), carrier_name))
merged = delays.leftOuterJoin( carriers.map( lambda c: (c[0], c[1]) ) )
#merged.take(5)

# flatten the structure for output
out = merged.map(lambda m: [m[1][1], m[1][0][0], m[1][0][1], m[1][0][2]])
out.take(10)

# COMMAND ----------

# MAGIC %md ####Question 2a
# MAGIC During the time period associated with the flights data set, which routes had the fastest travel speeds?

# COMMAND ----------

# assemble dataset in the form ((orig, dest) ,(airtime, miles))
# note: airtime is in minutes
routes = flights.map(lambda f: ((f[12],f[13]), ( int(f[9]), int(f[14]) )) )

# assemble ((orig, dest), (avg mph))
speeds = (
  routes
    .reduceByKey( lambda v, a: (v[0]+a[0], v[1]+a[1]) ) #sum airtime & miles by route
    .mapValues( lambda s: s[1]/(s[0]/60.) ) #divide total miles by total airtime (minutes->miles) for each route
  )

# re-key using avg speed (avg_mph, ((orig, dest), (avg mph))), 
# sort by avg mph descending, and then return ((orig, dest), (avg mph))
sorted = speeds.keyBy( lambda s: s[1] ).sortByKey(ascending=False).values()
sorted.take(10)

# COMMAND ----------

# MAGIC %md ####Question 2b
# MAGIC During the time period associated with the flights data set, what was the longest arrival delay experienced on a given route?

# COMMAND ----------

route_delays = ( 
  flights
    .map(lambda f: ( (f[12],f[13]), int(f[10])) ) # -->((orig, dest), arrival_delay)
    .reduceByKey( lambda v, a: max(v, a) ) # -->((orig, dest), max_arrival_delay)
    .keyBy( lambda f: f[1] )  # -->(max_arrival_delay, ((orig, dest), max_arrival_delay))
    .sortByKey(ascending=False)
    .values() # -->((orig, dest), max_arrival_delay)
  )

route_delays.take(10)

# COMMAND ----------

# MAGIC %md ####Question 2c
# MAGIC 
# MAGIC Take the results of the last question and convert airport codes into friendly names using data in the airports dataset.

# COMMAND ----------

flat = route_delays.map( lambda d: (d[0][0], d[0][1], d[1]) ) # -->(orig, dest, max_arrival_delay)

(  flat
    .keyBy(lambda d: d[0]) # -->(orig, (orig, dest, max_arrival_delay))
    .leftOuterJoin( airports.map( lambda a: (a[0], a[1]) ) ) # -->(orig, ((orig, dest, max_arrival_delay), orig_name))
    .values() # -->((orig, dest, max_arrival_delay), orig_name)
    .map(lambda d: ( d[0][1], (d[0][2], d[1])) ) # -->(dest, (max_arrival_delay, orig_name))
    .leftOuterJoin( airports.map( lambda a: (a[0], a[1]) ) ) # -->(dest,((max_arrival_delay, orig_name), dest_name))
    .values() # -->((max_arrival_delay, orig_name),dest_name)
    .map(lambda d: ( d[0][0], (d[0][1], d[1]) ) ) # -->(max_arrival_delay, (orig_name,dest_name))
    .sortByKey( ascending=False )
  ).take(10)