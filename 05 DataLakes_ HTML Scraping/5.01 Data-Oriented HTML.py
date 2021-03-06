# Databricks notebook source
# MAGIC %md 
# MAGIC ###Data Lakes: Data-Oriented HTML
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to introduce you to Hyper-Text Markup Language (HTML) and its elements that often hold useful information for us as data developers, collecting useful data from a webpage. While this is being presented in a Notebook, please note that this lab consists of instructions that will be executed on your client machine.

# COMMAND ----------

# MAGIC %md HTML is the language used to define webpages. Web pages can often be a useful source of data.  In order to retrieve data from a web page, we must become familiar with the HTML that defines it.
# MAGIC 
# MAGIC To get started, let's create a simple HTML file and open it in a web browser:
# MAGIC 
# MAGIC 1. Open a text editor on your desktop
# MAGIC 2. Save a blank file as a document named *webpage.html*
# MAGIC 3. Leave the text editor open for future steps
# MAGIC 4. Open a browser
# MAGIC 5. From the browser's menu, select Open File (typically available through the Ctrl+O shortcut)
# MAGIC 6. Open the *webpage.html* file you just created
# MAGIC 
# MAGIC The browser should present you with a blank screen.

# COMMAND ----------

# MAGIC %md At the top of each webpage is a document type declaration.  This tells the browser that the document retreived is an HTML document (as opposed to other content types we might access via HTTP): `<!DOCTYPE html>`
# MAGIC 
# MAGIC Just below the document type is the opening html tag: `<html>`  Between this opening html tag and the closing html tag, `</html>` (found at the very end of the document) we will later place our actual webpage content.
# MAGIC 
# MAGIC 
# MAGIC <ol>
# MAGIC <li>Return to your text editor
# MAGIC <li>Enter the following elements into your document:</li>
# MAGIC 
# MAGIC ```
# MAGIC   <!DOCTYPE html>
# MAGIC   <html></html>
# MAGIC ```
# MAGIC 
# MAGIC <li>Save the document</li>
# MAGIC <li>Refresh your browser (typically avialable through the F5 shortcut)</li>
# MAGIC </ol>
# MAGIC 
# MAGIC At this point, the webpage should not look much different than before.  Saving and refreshing the webpage is simply our means to check that we don't have an error in our HTML.  If you have an error, fix your HTML before proceeding.

# COMMAND ----------

# MAGIC %md **NOTE** From here forward, I will not be providing step by step instructions.  When asked to add HTML to your webpage, add it through the text editor, save your file and refresh your web browser to see the effect on the webpage.

# COMMAND ----------

# MAGIC %md An HTML document is typically divided into head and body sections.  The head section
# MAGIC is where information about the document resides.  It is also where we may place 
# MAGIC scripts and function definitions which may be called in the body of the document. 
# MAGIC Most of head content is not rendered by the web browser though some elements like 
# MAGIC `<title>` may be used as in a title bar or to name a saved bookmark.
# MAGIC 
# MAGIC Let's write a head section with some basic information about our page. These tags
# MAGIC are placed between the opening and closing html tags:
# MAGIC ```
# MAGIC <!DOCTYPE html>
# MAGIC <html>
# MAGIC   <head>
# MAGIC   <title>ITOM 6258 HTML Crash Course</title>
# MAGIC   </head>
# MAGIC </html>```
# MAGIC 
# MAGIC Save and refresh your webpage as before.  While the page hasn't changed, notice that the browser displays the title either on the tab containing the webpage or on the browser's primary window bar.

# COMMAND ----------

# MAGIC %md In addition to the title, the header of an HTML page may contain some useful information in the form of meta tags.  The meta tags are typically hand-coded by developers and/or auto-generated by the tools used by the developer to create the webpage. Some can be interpretted by the browser to adjust the page's presentation but most simply provide interesting details about the web page itself:
# MAGIC 
# MAGIC ```
# MAGIC <html>
# MAGIC   <head>
# MAGIC   <title>ITOM 6258 HTML Crash Course</title>
# MAGIC   <meta name="description" content="This is a handbuilt page, constructed to help us learn a little bit about HTML">
# MAGIC   <meta name="keywords" content="ITOM,Big Data,Web Scrapper">
# MAGIC   <meta name="author" content="Bryan Smith">
# MAGIC   <meta name="viewport" content="width=device-width, initial-scale=1.0">
# MAGIC   </head>
# MAGIC </html>
# MAGIC   ```
# MAGIC   
# MAGIC Notice that the meta tag has name and content attributes.  The meta tag generically 
# MAGIC denotes that metadata is being provided.  The name attribute identifies the specific 
# MAGIC metadata being provided, and the content attribute is the actual metadata.  Notice too
# MAGIC that the meta tag does not have a closing tag; it is a self-contained HTML tag.

# COMMAND ----------

# MAGIC %md There are many, many other tags which we can place in an HTML header but most are 
# MAGIC of no interest to us as data scrapers.  The one exception is a base tag which we will come back to when we discuss links.

# COMMAND ----------

# MAGIC %md The body element identifies the part of the HTML document that your browser 
# MAGIC will render to the screen.  This section of the document is identified using 
# MAGIC the body tag, placed just after the closing head tag:
# MAGIC 
# MAGIC ```
# MAGIC <body></body>
# MAGIC ```
# MAGIC Between these two tags, we place a wide range of content.  If we wish to 
# MAGIC include free-form text, we may do so, using the `<br>` and `</p>` (or possibly 
# MAGIC the `<p></p>`) tags to denote simple line breaks or larger paragraph breaks, 
# MAGIC respectively, in the text:
# MAGIC ```
# MAGIC <body>
# MAGIC This is free-form text that we might find in a web page.<br>The use of
# MAGIC a line break ensures that this sentence starts on a new line.</p>Paragraph 
# MAGIC breaks introduce a larger paragraph break to the text.
# MAGIC 
# MAGIC Breaks that we might place in the text that are not represented by the 
# MAGIC line-break or paragraph break tags are ignored by our browers.
# MAGIC </body>```

# COMMAND ----------

# MAGIC %md Within blocks of text, we often find various elements that control formatting. 
# MAGIC As data scrapers, we often are not concerned with these elements.  However, 
# MAGIC the anchor element is one we may need to pay careful attention to.
# MAGIC 
# MAGIC The anchor tag denotes a link. This may be a link to an item on the current website, 
# MAGIC a point within the current document, or a page or other asset that exists outside 
# MAGIC of the website. The opening anchor tag denotes the start of the link and the 
# MAGIC closing tag denotes its ending. Any text between the opening and closing anchor tags is "clickable", 
# MAGIC linking to the resource identified by the href attribute associated with the tag:
# MAGIC 
# MAGIC ```Click <a href="http://smu.edu" target="_blank">here</a> to visit the SMU website.```
# MAGIC 
# MAGIC The href attribute may point to any number of resources. When a URL is provided
# MAGIC which includes protocol information, *e.g.* http, https, ftp, mailto, etc., the 
# MAGIC link is said to be fully-qualified, *i.e.* it doesn't require any additional 
# MAGIC information to be resolved.
# MAGIC 
# MAGIC If that information is missing, then the href link is said to be a relative link.  A relative link navigates to a resource on the current webserver, positioned relative to the current page.  For example, this anchor points to a the *moreinfo.html* document in the *docs* subfolder positioned "under" the current HTML page:
# MAGIC 
# MAGIC ```Click <a href="docs/moreinfo.html" target="_blank">here</a> for more information.```
# MAGIC 
# MAGIC Relative links are by default positioned relative to the current web page.  However, you can modify this behavior by placing a base tag in the header of the HTML document.  The base tag identifies the "base URL" for any relative links in the page.
# MAGIC 
# MAGIC In addition to fully qualified and relative links, the anchor tag can be used to link to a reference point found within the existing web page. These reference points are known as anchors and are created by assiging id attributes to some commonly used HTML tags.  For example, you may wish to create an anchor point on a header like this:
# MAGIC 
# MAGIC ```<h1 id="top_of_page">This is my header</h1>```
# MAGIC 
# MAGIC To link to this point in the page, all that is needed is an anchor tag with a specialized href value that prefixes a hashmark to the front of the id of the anchor:
# MAGIC 
# MAGIC ```Click <a href="#top_of_page">here</a> to navigate to the header.```
# MAGIC 
# MAGIC To point to an anchor on another page, provide the URL to the page followed by 
# MAGIC the hash symbol and then the id of the element you are targetting on that page.

# COMMAND ----------

# MAGIC %md With links out of the way, let's look at lists.  HTML lists come in three flavors:
# MAGIC 
# MAGIC 1. Ordered Lists
# MAGIC 2. Unordered Lists
# MAGIC 3. Definition Lists
# MAGIC 
# MAGIC Ordered lists are numbered lists.  These are formed by using the `<ol>` and `</ol>` 
# MAGIC tags to denote the start and end of the list and `<li></li>` tags to denote the 
# MAGIC items in that list. Here is an example:
# MAGIC ```
# MAGIC HTML lists come in three flavors:
# MAGIC <ol>
# MAGIC <li><b>Ordered Lists</b></li>
# MAGIC <li>Unordered Lists</li>
# MAGIC <li>Defintion Lists</li>
# MAGIC </ol>
# MAGIC ```
# MAGIC Unordered lists are bulleted lists and are identified using the `<ul>` and `</ul>` tags.  Items in the list are again identified using the `<li></li>` tags:
# MAGIC ```
# MAGIC HTML lists come in three flavors:
# MAGIC <ul>
# MAGIC <li>Ordered Lists</li>
# MAGIC <li><b>Unordered Lists</b></li>
# MAGIC <li>Defintion Lists</li>
# MAGIC </ul>```
# MAGIC 
# MAGIC Defintion lists allow us to create lists of definitions as shown here:
# MAGIC ```
# MAGIC HTML lists come in three flavors:
# MAGIC <dl>
# MAGIC <dt>Ordered Lists</dt><dd>A list where each element is numbered</dd>
# MAGIC <dt>Unordered Lists</dt><dd>A list where each element is bulletted</dd>
# MAGIC <dt><b>Defintion Lists</b></dt><dd>A list where each element consists of a term and a definition</dd>
# MAGIC </dl>
# MAGIC ```
# MAGIC Definition lists are the least frequently used of the three list types, but you may occassionally encounter them.

# COMMAND ----------

# MAGIC %md Another popular element for presenting data in an HTML page is the table element. This 
# MAGIC element is enclosed in `<table></table>` tags. Individual rows are enclosed in `<tr></tr>`
# MAGIC tags. Within the header row, typically the first set `<tr></tr>` tags, individual cells 
# MAGIC are identified using `<th></th>` tags. Within other rows, cells are identified using the 
# MAGIC `<td></td>` tags.  Here is a simple 3 x 4 HTML table within which the first row is a header row:
# MAGIC 
# MAGIC ```
# MAGIC <table border="1">
# MAGIC   <tr><th>Col 1</th><th>Col 2</th><th>Col 3</th></tr>
# MAGIC   <tr><td>1</td><td>A</td><td>test001</td></tr>
# MAGIC   <tr><td>2</td><td>B</td><td>test002</td></tr>
# MAGIC   <tr><td>3</td><td>C</td><td>test003</td></tr>
# MAGIC </table>
# MAGIC ```
# MAGIC 
# MAGIC Here is that same table but with some `colspan` and `rowspan` attributes set. `colspan` and `rowspan` attributes may be found in `<th>` and `<td>` tags to 
# MAGIC indicate that the cell represented by that tag spans more than one columns or rows,
# MAGIC respectively.  Noticehow when rendered the colspan and rowspan attributes force the cell to span multiple
# MAGIC vertical and horizontal cells, respectively:
# MAGIC ```
# MAGIC <table border="1">
# MAGIC   <tr><th>Col 1</th><th>Col 2</th><th>Col 3</th></tr>
# MAGIC   <tr><td colspan="2">1</td><td>test001</td></tr>
# MAGIC   <tr><td rowspan="2">2</td><td>B</td><td>test002</td></tr>
# MAGIC   <tr><td>C</td><td>test003</td></tr>
# MAGIC </table>
# MAGIC ```
# MAGIC 
# MAGIC To make table definitions more sophisticated, HTML5 introduced the idea of a
# MAGIC table header, body and footer denoted using a `<thead>`, `<tbody>` and `<tfoot>`
# MAGIC tags, respectively.  While this doesn't affect the presentation of the table in 
# MAGIC the browser, you may want to have awareness of this variant as you develop web 
# MAGIC scrappers.  Here is the original 3 x 4 HTML table using this newer syntax:
# MAGIC 
# MAGIC ```
# MAGIC <table border="1">
# MAGIC   <thead>
# MAGIC     <tr><th>Col 1</th><th>Col 2</th><th>Col 3</th></tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr><td>1</td><td>A</td><td>test001</td></tr>
# MAGIC     <tr><td>2</td><td>B</td><td>test002</td></tr>
# MAGIC     <tr><td>3</td><td>C</td><td>test003</td></tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC ```
# MAGIC And here is our table using the `rowspan` and `colspan` attributes as in the prior example:
# MAGIC ```
# MAGIC <table border="1">
# MAGIC   <thead>
# MAGIC     <tr><th>Col 1</th><th>Col 2</th><th>Col 3</th></tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr><td colspan="2">1</td><td>test001</td></tr>
# MAGIC     <tr><td rowspan="2">2</td><td>B</td><td>test002</td></tr>
# MAGIC     <tr><td>C</td><td>test003</td></tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC ```

# COMMAND ----------

# MAGIC %md One last element you may want to be aware of is the div element. This element 
# MAGIC is used to label sections of an HTML document using the `<div>` and `</div>` tags. 
# MAGIC When parsing an HTML document, identifying the labeled section of document 
# MAGIC through these tags can be very useful.  
# MAGIC 
# MAGIC The div elements don't directly affect how a page is rendered in the browser.
# MAGIC However, instructions in what are known as cascading style sheets which control 
# MAGIC colors, fonts, etc. can be targetted to specific div tags, affecting the rendering
# MAGIC of the labeled sections of the document. 
# MAGIC 
# MAGIC There are many, many more HTML elements you will encounter as you develop web scrapers. 
# MAGIC To learn more about individual tags, I recommend checking out: https://www.w3schools.com/tags/