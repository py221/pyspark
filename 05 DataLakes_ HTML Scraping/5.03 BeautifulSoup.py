# Databricks notebook source
# MAGIC %md 
# MAGIC ###Data Lakes: BeautifulSoup
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to introduce you to the BeautifulSoup library, popularly used to parse HTML webpages in Python.

# COMMAND ----------

# MAGIC %md Now that we know how to retrieve HTML from a website, how might we extract data from the HTML itself?  Various elements, *i.e.* meta tags in headers, tables and lists in the body, and other tags scattered throughout a webpage, may be a valuable source of information.  The challenge is parsing it from the HTML document.
# MAGIC 
# MAGIC Of course, HTML is nothing more than a string so that we could just parse it using basic search and extract patterns, but writing the complex parsing code required to do so is time-consuming and prone to error.  Luckily, a Python library called BeautifulSoup has been built to do just this.
# MAGIC 
# MAGIC BeautifulSoup is not available by default in Python so that we must install it manually.  If we were installing this manually on a server, we'd just issue this command:
# MAGIC 
# MAGIC ```pip install beautifulsoup4```
# MAGIC 
# MAGIC Of course, we'd have to do this on every node of our cluster which is a pain.  In this environment, we'll take advantage of a proprietary shortcut for this operation:

# COMMAND ----------

dbutils.library.installPyPI('beautifulsoup4')
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md With BeautifulSoup installed, let's grab the FDA's webpage listing publications authored by staff researchers: https://www.accessdata.fda.gov/scripts/publications/. The list of publications on the page and the content they link to could be valueable sources of insight, if we can extract the data from the page:

# COMMAND ----------

from urllib import request as url

request = url.Request('https://www.accessdata.fda.gov/scripts/publications/')
response = url.urlopen(request)

html = response.read().strip()
print(html)

# COMMAND ----------

# MAGIC %md With BeautifulSoup installed, we can load the HTML we've retrieved from our website to enable parsing. Notice that right away, BeautifulSoup "prettifies" the HTML making it much easier to read:

# COMMAND ----------

from bs4 import BeautifulSoup

soup = BeautifulSoup(html)
print(soup)

# COMMAND ----------

# MAGIC %md With our data loaded into BeautifulSoup, we can search for html tags, extracting the data associated with those tags into easy to use objects.  For example, let's say we wished to access the publications noted on this page.  If we navigate through the HTML document, we can see that these are found in paragraph, *i.e.* `p`, tags that have been assigned a class attribute of `homepage`.  Here is one such tag (with many of the blank lines and spaces removed):
# MAGIC ```
# MAGIC    <p class="homepage">
# MAGIC         <b>Animal and Veterinary</b>
# MAGIC         <ul class="homepage">
# MAGIC           <li>  Res Vet Sci 2019 Feb;122:186-8
# MAGIC                 <br>
# MAGIC               <a class="lightwindow" params="lightwindow_width=700" href="search_result_record.cfm?id=61945">   
# MAGIC               <b>Identification of swine protein biomarkers of inflammation-associated pain.</b></a>
# MAGIC             </li>
# MAGIC             <li><a href="more.cfm?center=CVM&center_name=Animal and Veterinary">More...</a></li>
# MAGIC         </ul>
# MAGIC     </p>```
# MAGIC 
# MAGIC We might find tags like this in the document by searching for all `p` tags with a class attribute set to `homepage`. BeautifulSoup makes this easy, returning each `p` tag as an element in a Python list:

# COMMAND ----------

pubs = soup.find_all('p', attrs={'class':'homepage'})
print('{0} pubs found in the document\n'.format(len(pubs)))
print(pubs)

# COMMAND ----------

# MAGIC %md Each `p` tag contains list items as indicated by the `li` tags. The last list item in each `p` block is a link to more publications.  We want all but the last list item, *i.e.* the one identified with the *More...* text:
# MAGIC 
# MAGIC ```
# MAGIC <p class="homepage">\n<b>Animal and Veterinary</b>\n<ul class="homepage">\n<li>\r\n \r\nRes Vet Sci 2019 Feb;122:186-8\r\n<br/>\n<a class="lightwindow" href="search_result_record.cfm?id=61945" params="lightwindow_width=700"><b>Identification of swine protein biomarkers of inflammation-associated pain.</b></a>\n</li>\n<li><a href="more.cfm?center=CVM&amp;center_name=Animal and Veterinary">More...</a></li>\n</ul>\n</p>
# MAGIC ```
# MAGIC 
# MAGIC Notice too that each list items contains a link as indicated by the `a` tag.  If you look at the `a` tag, you can see there is a relative URL associated with it.  If you click on one of these, you'll see that the base for this relative URL is https://www.accessdata.fda.gov/scripts/publications/.  Let's get the text for each list item (except the last one each block) as well as the link associated with it:

# COMMAND ----------

# this function removes redundant white 
# space and line breaks from within a string
def cleanse_text( text ):
    return ' '.join(text.split())

data = []

# iterate through all pubs found in earlier cell
for pub in pubs:
    
    # iterate over list items, except last one
    for li in pub.find_all('li'):
        
        # empty tuple for li content
        li_content = []
        
        # get text for list item
        li_text = cleanse_text(li.text)
        
        if li_text.lower() != 'More...'.lower():
            li_content += [li_text]

            # get any link for this list item
            li_link = li.find('a')

            # if link found:
            if li_link is not None:
                # get the href portion of the link
                li_href = 'https://www.accessdata.fda.gov/scripts/publications/' + li_link['href']
                li_content += [li_href]
        
        # if data captured for this li:
        if len(li_content) > 0:
            data += [li_content]
    
print(data)

# COMMAND ----------

# MAGIC %md There are a few things in this last block of code that we need to discuss. First, each block has one or more list items. Because the number of `li` tags varies, we need to use find_all to get all the `li` tags within the `p` block (pub) and then loop over these.  
# MAGIC 
# MAGIC Next, for each `li` item, we need to capture two kinds of content: its text and its link.  We first extract the `li` tag's text.  If that text is 'More...', we are skipping it. Notice that we compare the text to 'more...' by setting both to lower-case. This helps insure that case-sensitive string comparisons don't trip us up.
# MAGIC 
# MAGIC Assuming we are not dealing with the `li` tag which contains the *More ...* string, we assign the text to an empty li_content list variable. We use this empty list variable to hold the list of values we extract for this `li` tag.
# MAGIC 
# MAGIC The next value we want to find for the `li` tag is a link as identified by the `a` (anchor) tag.  If this tag is found, it's attributes are presented as a dictionary.  Using this feature, we can easily grab the `href` component that holds the actual linked page.  That linked page is a relative link, so we append it to the hard-coded URL we understand to be the base for relative links on this page.  We add this `href` value to our li_content list.
# MAGIC 
# MAGIC Now that we are done with this `li`, we take its content (captured in the li_content list) and append it to the data list which will hold li_content data across all the pubs over which we are iterating.
# MAGIC 
# MAGIC Why capture data this way?  Well, if we have data in an iterable structure, we can use Spark to flip it into an RDD.  Once in an RDD, if each element in the RDD is itself an iterable item, either a list or a tuple, we can then apply a schema to convert the RDD into a queriable dataframe.  We'll get to that in a later lab, but for now, let's look at how we might parse a table.

# COMMAND ----------

# MAGIC %md </p>This page contains FDA drug approvals in a series of tables, one for each day of the current week: https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=report.page

# COMMAND ----------

request = url.Request('https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=report.page')
response = url.urlopen(request)

html = response.read()

soup = BeautifulSoup(html)
print( soup )

# COMMAND ----------

# MAGIC %md The structure of this document is a bit tricky.  If we review the HTML, what we find is that two `div` tags enclose a set of otherwise independent tables. We need to focus BeautifulSoup on the relavant sections defined by the `div` tags and extract data from the tables within it.
# MAGIC 
# MAGIC In addition, each table is preceeded by an `H4` header which announces the date associated with each table.  If we flatten all this data into a single table-like structure, we will need to capture this information and assign it to each "row" of data.
# MAGIC 
# MAGIC Here is the top of this HTML block:
# MAGIC 
# MAGIC ```
# MAGIC <div class="tab-content">
# MAGIC     <div role="tabpanel" class="tab-pane fade in active" id="example2-tab1">
# MAGIC         <h4 style="color: #0a3984;font-weight:bold">January 10, 2019</h4>
# MAGIC         <script type='text/javascript'>//<![CDATA[
# MAGIC         $(document).ready(function(){
# MAGIC         $('#example2_01-10-tab1-dt').DataTable({
# MAGIC             responsive: true,
# MAGIC             "bFilter": false,
# MAGIC             "bPaginate": false,
# MAGIC             });
# MAGIC         });//]]> 
# MAGIC         </script>
# MAGIC 
# MAGIC         <table id="example2_01-10-tab1-dt" 
# MAGIC             class="table table-striped table-bordered table-condensed" 
# MAGIC             cellspacing="0" width="100%">
# MAGIC         <thead>
# MAGIC         <tr>
# MAGIC             <th title="Drug Name and Application Number">Drug Name and<br> Application Number</th>
# MAGIC             <th title="Sort by Active Ingredient of January 10, 2019">Active Ingredient</th>
# MAGIC             <th title="Dosage Form/ Route">Dosage Form/ Route</th>
# MAGIC             <th title="Submission">Submission</th>
# MAGIC             <th title="Company">Company</th>
# MAGIC             <th title="Submission Classification">Submission Classification</th>	
# MAGIC             <th title="Submission Classification">Submission Status</th>
# MAGIC         </tr>
# MAGIC         </thead>
# MAGIC         <tbody>
# MAGIC         <tr>
# MAGIC             <td><a href="/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=209015" 
# MAGIC                 title="Click to view Olopatadine Hydrochloride  &#13;ANDA #209015">Olopatadine 
# MAGIC                 Hydrochloride <br>ANDA  #209015</a></td>
# MAGIC             <td>Olopatadine Hydrochloride </td>
# MAGIC             <td>Unknown </td>
# MAGIC             <td>SUPPL-1</td>
# MAGIC             <td>Apotex Inc </td>
# MAGIC             <td></td>
# MAGIC             <td>Tentative Approval</td> 
# MAGIC         </tr>
# MAGIC         </tbody>
# MAGIC         </table>```

# COMMAND ----------

table_data = []

# navigate to the block that holds the tables
div1 = soup.find('div', attrs={'class':'tab-content'})
div2 = div1.find('div', attrs={'role':'tabpanel', 'class':'tab-pane fade in active'})

# grab the tables 
tables = div2.find_all('table')

# grab the dates that serve as table titles
dates = div2.find_all('h4')

# for each table:

i = 0  # keep track of which table we are on

for table in tables:
    
    # get date for this table
    date = dates[i].text   
    
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
                row_data += [link['href']]
            
            # append row data to table data 
            table_data += [row_data]
    
    # update index for next table in loop
    i += 1
    
print(table_data)

# COMMAND ----------

# MAGIC %md Carefully review the code above to understand how it is we parse this particular block of HTML. The odd way that the table titles present data information outside the table requires that we carefully think about how we identify the date for a given table.
# MAGIC 
# MAGIC Notice too that as we grab the second `div` tag, we specify two attributes to search on. The attrs argument in find() and find_all() is a dictionary.  We can specify multiple attributes to help it make a precise search through our HTML code.
# MAGIC 
# MAGIC When considering the attrs argument in the find and find_all methods, it is important to understand that the values for the keys in the attrs dictionary can be either fixed strings or regular expressions.  Using regular expressions gives us a lot of flexibility in situations where our data is likely to change.  For example, you may have noticed that the tables in the last web page had id attributes which contained part of the date with which they were associated:
# MAGIC 
# MAGIC ```
# MAGIC <table id="example2_01-10-tab1-dt" 
# MAGIC     class="table table-striped table-bordered table-condensed" 
# MAGIC     cellspacing="0" width="100%">
# MAGIC ```
# MAGIC 
# MAGIC We could have used this to navigate directly to these tables without going first to the two `div` tags that proceeded them as demonstrated in this next cell:

# COMMAND ----------

import re

# compile regex for "example2_" followed by 2-digits, dash, 2-digits and then "-tab1-dt"
table_id = re.compile('example2_\d{2}-\d{2}-tab\d-dt')

tables = soup.find_all('table', attrs={'id':table_id} )

print( '{} tables found in the document'.format(len(tables)))

# COMMAND ----------

# MAGIC %md While we are looking for data within the body of the HTML document, it's important to remember there can be rich data in the header as well.  Consider the default FDA web page: https://www.fda.gov/default.htm
# MAGIC ```
# MAGIC     <head>	
# MAGIC         <meta http-equiv="X-UA-Compatible" content="IE=EDGE, chrome=1" />
# MAGIC         <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
# MAGIC 
# MAGIC         <title>U S Food and Drug Administration Home Page</title>
# MAGIC         <meta name="viewport" content="width=device-width, initial-scale=1.0" />
# MAGIC         <meta name="apple-mobile-web-app-title" content="FDA" /> 
# MAGIC         <meta name="apple-mobile-web-app-capable" content="yes" />
# MAGIC         <meta name="Description" content="Home Page for the Food and Drug Administration (FDA)" />
# MAGIC 
# MAGIC         <meta http-equiv="Content-language" content="en" />
# MAGIC         <meta name="keywords" content="FDA, Food and Drug Administration, foods, 
# MAGIC             drugs, biologics, veterinary, cosmetics, devices, industry, medwatch, radiology" />
# MAGIC         <meta name="edited" content="" />
# MAGIC         <meta name="verify-v1" content="sv+ID5+7RbHgki+Nm89Mj2pmQPMt4BLbOw27nsp2Hmo="�/> 
# MAGIC         <META name="y_key" content="a07c56fd35f73d3b">
# MAGIC         <meta name="google-site-verification" content="wxQkNTrqmvYnwVcNzA2mspBp7EDErzIhAzyX-YvvRWI" /> 
# MAGIC         <meta name="google-site-verification" content="JTQlLQ5-geDsQnF1jxUZ1aTumCfA1TQ4OVgN4yWHdJo" />
# MAGIC         <meta name="google-site-verification" content="tWxlDhm4ANdksJZPj7TBmHgNoMqZCnecPp0Aa2vC9XA" /> ```

# COMMAND ----------

request = url.Request('https://www.fda.gov/default.htm')
response = url.urlopen(request)
html = response.read()
soup = BeautifulSoup(html)

# get HTML document head
head = soup.find('head')
head_data = []

# get title from head
title = head.find('title')
head_data += [title.text]

# get meta tags from head
metas = head.find_all('meta')

# read key-value info from meta tags into a dictionary
meta_data = {}
for meta in metas:
    # get name or http-equiv for our key
    if 'name' in meta.attrs:
        key = meta['name']
    elif 'http-equiv' in meta.attrs:
        key = meta['http-equiv']
    elif 'property' in meta.attrs:
        key = meta['property']
    else: key = None

    # get content for our value
    if key is not None:
      meta_data[key] = meta['content']

# add meta dictionary to head record
head_data += [meta_data]

print(head_data)

# COMMAND ----------

# MAGIC %md ####Try It Yourself
# MAGIC 
# MAGIC Consider the following scenarios.  Write a simple block of code to answer the question(s) associated with each.  The answer to each question is provided so that your challenge is to come up with code that is logically sound and produces the required result. Code samples answering each scenario are provided by scrolling down to the bottom of the notebook.

# COMMAND ----------

# MAGIC %md ####Section 1
# MAGIC 
# MAGIC The SMU Cox Faculty profiles are presented at https://www.smu.edu/cox/Our-People-and-Community/Faculty. If you navigate to that page and inspect the HTML behind it, you will see that profiles are presented as list items in an unordered list that has a class attribute of `cards-container` that itself is contained within a section with a class attribute of `component cards-grid-profiles`.  The raw HTML looks like this:
# MAGIC 
# MAGIC ```
# MAGIC <section class="component cards-grid-profiles">
# MAGIC   <div class="grid-container">
# MAGIC     <ul class="cards-container">
# MAGIC       <li>
# MAGIC       <a href="/cox/Our-People-and-Community/Faculty/Mea-Ahlberg">
# MAGIC         <div class="image" style="background-image: url('/-/media/Site/Cox/Faculty/Photo/Profile/Linked/Mea-Ahlberg.JPG?mh=355&mw=237&hash=FCA5FD40F4E8E74062F4579F986F8AD6');"></div>
# MAGIC         <div class="text">
# MAGIC           <h3>
# MAGIC             <span class="fname">Mea</span>
# MAGIC             <span class="lname">Ahlberg</span>
# MAGIC           </h3>
# MAGIC           <p class="summary">
# MAGIC             <span>Adjunct Professor</span><br />
# MAGIC             <span><br>PhD, Public Policy & Political Economy - International Conflict and Security, University of Texas at Dallas<br>MBA, International Marketing</span><br />
# MAGIC             <span></span>
# MAGIC           </p>
# MAGIC           <p class="tags"> Management and Organizations<br /> </p>
# MAGIC         </div>
# MAGIC       </a>
# MAGIC       </li>
# MAGIC       <li> ....
# MAGIC         ```
# MAGIC         
# MAGIC Using the skills you've built in this lab, retrieve this web page and extract the following items for each profile on this page:
# MAGIC 
# MAGIC * First Name - this should be the text of the span item with a class of 'fname'
# MAGIC * Last Name - this should be the text of the span item with a class of 'lname'
# MAGIC * Title - this should be the text of the first span item encountered with a class of *None*
# MAGIC * Individual Web Page - this should be the href attribute of the first anchor encountered in the profile
# MAGIC 
# MAGIC Assemble these elements as a tuple for each Faculty member and make each member's tuple an item in a larger list called faculty.

# COMMAND ----------

from urllib import request as url 
from bs4 import BeautifulSoup()

# COMMAND ----------

section = soup.find('section', attrs={'class':'component cards-grid-profiles'})
name = section.find('li').find_all('span') 

# COMMAND ----------

# section 1 code here
url_desired = 'https://www.smu.edu/cox/Our-People-and-Community/Faculty'
request = url.Request(url_desired)
response = url.urlopen(request)
html = response.read()
soup = BeautifulSoup(html)

print(f'org url = open_url?\t{url_desired == response.geturl()}')
print(f'{soup}')

# -------------------------------------------------------------------

# COMMAND ----------

# sequence to parse html

section = soup.find('ul', attrs={'class': 'cards-container'})
name = section.find('li').find_all('span')
page = section.find('a')
#section
name # must loop through 
span_lst = [n.text for n in name]
print(span_lst)

url_desired.__add__(page.attrs['href'])

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

# section 1 code here
from urllib import request as url
from bs4 import BeautifulSoup

# retrieve the web page
request = url.Request('https://www.smu.edu/cox/Our-People-and-Community/Faculty')
response = url.urlopen(request)
html = response.read()

# parse the html for the webpage
soup = BeautifulSoup(html)

# navigate to the profiles
profile_section = soup.find('section', attrs={'class':'component cards-grid-profiles'})
profiles = profile_section.find_all('li')

# setup list to hold profiles 
faculty = []

# for each profile
for profile in profiles:
  
  # individual web page
  faculty_page = profile.find('a')['href']
  
  # first & last names
  fname = profile.find('span', attrs={'class':'fname'}).text
  lname = profile.find('span', attrs={'class':'lname'}).text
  
  #title
  title = profile.find_all('span', attrs={'class':None} )[0].text
  
  # assemble member attributes as a tuple
  faculty_member = (fname, lname, title, faculty_page)
  
  # append the tuple to the faculty list
  faculty += [faculty_member]

# review faculty profile info
print(faculty)

# COMMAND ----------

