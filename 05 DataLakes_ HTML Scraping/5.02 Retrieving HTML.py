# Databricks notebook source
# MAGIC %md 
# MAGIC ###Data Lakes: Retrieving HTML
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to introduce you to the basics of retrieving HTML webpages using Python.

# COMMAND ----------

# MAGIC %md The **urllib** library provides a simple interface for requesting content from web servers. Let's make a simple request to an anonymous website, *i.e.* a website that does not require a login credential:

# COMMAND ----------

from urllib import request as url 

# assemble the request
request = url.Request('http://www.smu.edu')

# execute the request and gather the response
response = url.urlopen(request)

# COMMAND ----------

# MAGIC %md  Let's breakdown what's happening in the code above. In the first line, we import the request object from the **urllib** library.  So that we don't have to type urllib.response.Request and urllib.response.urlopen, we import this using the shortcut *url*.
# MAGIC Next, we assemble a request to a web server. Most requests, like the one
# MAGIC shown here, are simple pointers to web pages. Others require us to pass data
# MAGIC to the web server both in the body of our request and in what are known as 
# MAGIC *request headers*. The **Request** method call allows us to assemble all of these
# MAGIC elements for the request as a request object which we then actually send 
# MAGIC to the web server with the **urlopen** method call. For simple page requests, 
# MAGIC this code could be shortened so that you just pass the url of the webpage to the **urlopen** 
# MAGIC method, but i'd rather you be familiar with the two step approach as we 
# MAGIC will return to this later.
# MAGIC 
# MAGIC When we send a request to a web server, we get a response. If we'd like to 
# MAGIC read the body of the response, which should contain the web page or other asset we requested from the targetted website, we call the **read()** method on the response object to return the response as string:

# COMMAND ----------

print(response.status)

html = response.read().strip()
print(type(html))

# COMMAND ----------

# MAGIC %md It's not clear why but many times the HTML in the response contains a lot of 
# MAGIC leading and trailing white space.  It's that white space that we are optionally
# MAGIC removing with the **strip()** method call.

# COMMAND ----------

# MAGIC %md You should be aware that some websites change their content based on which
# MAGIC requesting client is being used.  The **urllib** library presents itself to the web server as 
# MAGIC *Python-urllib/x.y* where x and y are the major and minor versions of Python
# MAGIC that's in use. If you wish to tell the website that you are using a different
# MAGIC browser, simply grab the agent information for the client you wish to imitate and pass it along
# MAGIC with the request as demonstrated here:
# MAGIC 
# MAGIC **NOTE** https://deviceatlas.com/blog/list-of-user-agent-strings provide good listings of popular agent strings.

# COMMAND ----------

# tell the website we are the google crawler
user_agent = 'Googlebot/2.1 (+http://www.googlebot.com/bot.html)'
headers = {'User-Agent': user_agent}
request = url.Request('http://www.smu.edu', headers=headers)

response = url.urlopen(request)
html2 = response.read().strip()

print('The content is the same as before: {0}'.format(html==html2) )

# COMMAND ----------

# MAGIC %md There is a ton more we could say about making HTTP requests, but you have what you need to scrape most public websites. That said, we need to know what to do when we make a request and don't get back the response we were expecting.
# MAGIC 
# MAGIC Consider this scenario where we request a page that doesn't exist on a popular website:

# COMMAND ----------

import urllib

request = url.Request('https://github.com/git/junk')
try:
    response = url.urlopen(request)
except urllib.error.HTTPError as e:
    print('The HTTP status code returned is {0}'.format(e.getcode()) )
    print('The message returned is \'{0}\'\n'.format(e.msg) )
    print('The headers are the response are:\n{0}\n'.format(e.headers) )
    print('The content actually returned is:\n{0}'.format(e.fp.read().strip()))

# COMMAND ----------

# MAGIC %md When a website sends back an error to a request, we can capture it as an exception.  The HTTP error code is typically the most useful bit of information as web servers send back a fairly [standard set of error codes](https://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html).  Some websites return this error code along with an HTML error page which you can access using the **fp** object of the exception (as demonstrated above).

# COMMAND ----------

# MAGIC %md Sometimes we request a page that's actually an alias for another page.  When we make the page request, the web server returns an error code in the 300 range along with information on the page it would like us to request instead.  Some clients, such as **urllib**, will attempt to automatically request the redirected page, hiding the exception from  the client code.  When this occurs, the code doesn't see an exception raised even though a different page from the one originally requested has been returned:

# COMMAND ----------

redirect_url = 'http://cox.smu.edu/msba/'
request = url.Request(redirect_url)
try:
    response = url.urlopen(request)
    print( 'We received an HTTP status code of {0}, which tells us we got a good response'.format(response.status))
    print( 'Still, we requested {0} but actually received \'{1}\''.format(redirect_url , response.geturl()) )
except urllib.error.URLError as e:
    print( 'The HTTP status code returned is {0}'.format(e.getcode()) )
    print( 'The message returned is \'{0}\''.format(e.msg) )
    print( 'The reason returned is \'{0}\''.format(e.reason) )
    print( 'The actual resource returned is \'{0}\''.format(e.geturl()) )

# COMMAND ----------

# MAGIC %md If you need to more easily catch redirects, there are other techniques in Python for requesting web pages.  Redirects are not inherently bad but getting redirected because you are calling a page alias does add overhead for both your code and the web server so that in some situations it's best to make sure you are not getting redirected.  It can also cause you to get directed from a once valid and specific resource to a more generic resource that may not provide the information you wanted (as demonstrated here).

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Scenario 1
# MAGIC 
# MAGIC Using techniques explored in class, retreive this webpage from the SMU website: http://smu.edu/cox/msba. What page was actually returned?  

# COMMAND ----------

# your scenario 1 code here
from urllib import request as url 

request_url = 'http://smu.edu/cox/msba'
request = url.Request(request_url)

try:
  response = url.urlopen(request)
  print(f'HTTP status code of {response.status}')
  print(f'Requested url: {request_url}\n'
        f'actual url: {response.geturl()}')
except urllib.error.URLError as e:
  print(f'HTTP status code of {e.getcode()}')
  print(f'Msg returned: {e.msg} \n\n')
  print(f'Reason returned: {e.reason} \n\n')
  print(f'Actual resources returned: {e.geturl()}')

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

# scenario 1
import urllib

requested_url = 'http://smu.edu/cox/msba'
request = url.Request(requested_url)

response = url.urlopen(request)
html = response.read().strip()

print('I requested {0} and received {1}'.format(requested_url, response.geturl()))
print('\n', html)