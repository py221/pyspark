# Databricks notebook source
# MAGIC %md 
# MAGIC ###Data Lakes: RegEx
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to introduce you to RegEx. While this is being presented in a Notebook, please note that this lab consists of instructions that will be executed on your client machine.

# COMMAND ----------

# MAGIC %md ####Setup the RegEx Tool

# COMMAND ----------

# MAGIC %md In this lab, we will use a few lines of data from a sample weblog to explore the concept of regular expressions. To get us started, perform the following steps:
# MAGIC 
# MAGIC 1. Using TextPad, open the sample_web_log.txt file found at https://smithbc.blob.core.windows.net/downloads/weblogs/sample/sample_web_log.txt
# MAGIC 2. Open a browser to https://regex101.com/
# MAGIC 3. In your browser, replace the sample text in the TEST STRING text box with the lines from the file
# MAGIC 4. Clear the REGULAR EXPRESSION text box
# MAGIC 5. On the far right of the REGULAR EXPRESSION text box, set the flags to *gm* if they are not already done so for you
# MAGIC 6. Under FLAVOR on the far left fo the page, select *Python*
# MAGIC 
# MAGIC You are now ready to proceed with this lab.

# COMMAND ----------

# MAGIC %md ####Capture Text Elements with RegEx

# COMMAND ----------

# MAGIC %md
# MAGIC Review the text you pasted into the TEST STRING box. Take a close look at the first entry in each line. It is the IP address of the client (host) requesting an asset from the web server. IP addresses are represented as four numerical values ranging from 0 to 255 and separated from each other by a period. 
# MAGIC   
# MAGIC In the REGULAR EXPRESSION box, enter the value `1` and notice how numerical values of 1 in the TEST STRING box are highlighted.  You might observe that values not just in the IP address field are being highlighted.
# MAGIC 
# MAGIC Expand your REGULAR EXPRESS to `157`. Notice you have fewer matches as the numerical sequence 157 occurs less frequently in our text. 

# COMMAND ----------

# MAGIC %md Typically, our goal is not to find a specific numerical sequence but numbers in general.  Clear the REGULAR EXPRESSION box and enter `[0-9]`.  What is captured (highlighed) by the expression now?

# COMMAND ----------

# MAGIC  %md The range 0-9 enclosed in square brackets is a Regular Expression that captures any valid numerical value between 0 and 9.  We can adjust the range to limit which specific numerical values we might capture, *e.g.* [2-5], but for our
# MAGIC  purposes, capturing the full range of digits works.  In situations like this  where we want any valid digit, we might use the shorthand notation \d to represent
# MAGIC  the range [0-9]: `\d`

# COMMAND ----------

# MAGIC %md Notice that we are capturing each digit individually.  What if we wanted to capture two digits, one following the other?: `\d\d`
# MAGIC  
# MAGIC Three digits?: `\d\d\d`
# MAGIC  	
# MAGIC At some point, writing out a series of \d elements becomes laboreous.  To help us with this, we might identify that we are attempting to capture digits and follow the digit shorthand with a pair of curly braces containing the number of digits we want: `\d{3}`
# MAGIC  
# MAGIC Hard coding three digits finds plenty of three-digit seqeunces, but what if we wanted to capture 1 to 3 digit sequences which are more typically of the values in our IP address field: `\d{1,3}`

# COMMAND ----------

# MAGIC %md What if we just wanted one or more digits?: `\d{1,}`
# MAGIC  	
# MAGIC This last expression leaves the max number of digits open ended.  This pattern is so common that we have a special notation for it: `\d+`

# COMMAND ----------

# MAGIC %md The plus symbol requires one or multiple instances of the specified character, *e.g.* a digit, in order to return a match.  If our goal was zero or one match, we could use a question mark: `\d?` And if our goal was zero or more of these characters, we can use another special notation to denote this: `\d*`

# COMMAND ----------

# MAGIC %md We started this exercise by noting the structure of the IP addresses in the sample records. Each IP address consists of four 1 to 3 digit values sepearated by a period.  Let's see if we can capture a string of digits followed by a period: `\d*.`

# COMMAND ----------

# MAGIC %md If you aren't getting the pattern match that you wanted, it's because the period character in a regular expression is shorthand for "any character", including white space. To have the regular expression read the period as nothing more than a standard period character, try adding a backslash (escape character) just in front of it: `\d*\.`  You should now be capturing digits followed by a period.

# COMMAND ----------

# MAGIC %md Now that we have the basic patttern down, let's see if we can capture the whole IP address: `\d*\.\d*\.\d*\.\d*`

# COMMAND ----------

# MAGIC %md Or another way we could express this same thing is to specify the number of repeats we are 
# MAGIC looking for.  Notice in this example that we wrap the repeating pattern in
# MAGIC parantheses and apply the { } repeat element to it to specify the number of repeats requred: `(\d*\.){3}\d*`

# COMMAND ----------

# MAGIC %md Within our sample dataset, the IP addresses found with this pattern exist at the start of each record. If an IP address was located in another field, our pattern would capture it to.  If we want to ensure our pattern match must be found at the start of a line of text, we can use the uptick (carrot) character at the start of the pattern: `^(\d*\.){3}\d*`
# MAGIC 
# MAGIC Now we have our IP address properly captured.  The IP address field is separated from the next field through a space character.  Using `\s`, we can add that space (or any other white space character such as a tab) to our pattern: `^(\d*\.){3}\d*\s`

# COMMAND ----------

# MAGIC %md The pattern we've provided captures our IP address along with its field terminating space.  That said, we could make it a bit more succint, maybe at the expense of being so precise.  
# MAGIC 
# MAGIC If we change our pattern to search for any valid non-whitespace characters from the start of the line until a whitespace charater is actually encountered, would we capture the same information?: `^[^\s]*\s`

# COMMAND ----------

# MAGIC %md This less precise pattern captures the same information as before.  The decision to use more precise or less precise patterns is really up to the developer who needs to understand the range of values that could be matched and what realistically will be encountered in the strings being searched. As you develop more RegEx patterns, you will find there are any number of different ways to capture the information you are after and will often need to wrestle with precision, brevity and readability/supportability.
# MAGIC 
# MAGIC With readability in mind, did you notice we've introduced another uptick (carrot) character into the pattern? The first uptick denotes the start of a line of text.  The second uptick is used as a NOT symbol.  How do we distinguish between the two uses of this one character?
# MAGIC 
# MAGIC Within square brackets, some shorthand characters have special meanings.  The uptick character is one of these.  When used within square brackets, it's read as applying NOT to any other characters specified with it.  In the pattern above the uptick changes \s to NOT-\s (not a whitespace character).  
# MAGIC 
# MAGIC It probably will not suprise you at this point that RegEx has another shorthand for specifying "not a whitespace character."  It's \S, with a capital S.  Using this, you can rewrite the last pattern as: `^\S*\s`

# COMMAND ----------

# MAGIC %md In regular expressions, we often denote a shorthand element with a backslash and a lowercase character.
# MAGIC You have seen \s and \d but there are others. If we flip the character to upper-case we are indicating
# MAGIC we wish to capture those characters that are not indicated by the lower case shorthand.  With that in
# MAGIC mind, \s is a whitespace character while \S is not whitespace character, \d is a digit while \D is not a digit, and so on and so on.

# COMMAND ----------

# MAGIC %md Look closely at the capture in the browser.  Notice the \s is being captured as part of the pattern.
# MAGIC Our goal will be eventually to extract elements from this string. We need to use the \s as a field terminator
# MAGIC for our host (IP address) field, but we don't need to capture it for extraction.  Here is where the concept
# MAGIC of a group comes in.

# COMMAND ----------

# MAGIC %md If we look at the previously used pattern, our IP address characters should be the ones associated with \S\*
# MAGIC up to the \s character that terminates that string.  Let's wrap the \S\* expression in parantheses to indicate
# MAGIC that this portion of the match will be identifieable as a group: `^(\S*)\s`
# MAGIC 
# MAGIC As you will see later in this class, groups in RegEx strings can be referenced by numbers.  Using groups, you can match a larger string based on a complex pattern and then extract subsets of the pattern.  With our weblogs, this will become the basis by which we will match a valid line in the weblog and then extract specific fields from it.

# COMMAND ----------

# MAGIC %md With all this in mind, consider how this pattern matches the first three fields of our weblog lines: `^([^ ]*) ([^ ]*) ([^ ]*)`
# MAGIC 
# MAGIC Notice in this pattern that actual spaces are being used instead of \s to capture the field terminators.  You could rewrite this as: `^(\S*)\s(\S*)\s(\S)\s`
# MAGIC 
# MAGIC Between the two, which is more easily readable to you?  Are there any concerns about loss of precision when using \s?

# COMMAND ----------

# MAGIC %md We now have a pattern that captures the first three fields in a weblog record.  Our next field to capture is the datetime part associated with the HTTP request.  All the datetime entries in our sample are
# MAGIC enclosed in square brackets. If we could capture text from the opening square bracket to the closing square bracket, we should be able to capture this datetime information. 
# MAGIC 
# MAGIC Of course, the square bracket is a special character, so to ensure it's read as a standard square bracket character, we'll need to escape it with a backslash.  Let's try this on a small pattern that ignores the preceding fields in the string: `(\[.*\])\s`

# COMMAND ----------

# MAGIC %md The one concern we have with this pattern is that if we look through the millions of entries in our weblogs, we will
# MAGIC come across instances where we have a datetime value that is missing and is represented by nothing
# MAGIC more than a short dash -.  To handle this, we need to tell our RegEx engine to use one of two patterns: either a dash OR the information contained within the square brackets.  To do this, we can use the pipe character which is shorthand for a Boolean OR operator: `(-|\[.*\])\s`

# COMMAND ----------

# MAGIC %md Combining this pattern with our previous expression, we get this: `^(\S*)\s(\S*)\s(\S)\s(-|\[.*\])\s`

# COMMAND ----------

# MAGIC %md Now we need to capture our HTTP request.  Looking at this entry, it looks like we can pattern match off
# MAGIC the quotation marks that start and terminate this field:`(\"[^\"]*\")\s`
# MAGIC 
# MAGIC As before, you can see that quotation marks are special characters which must be escaped.  From the first quotation mark we capture, we should continue capturing all non-quotation marks up until the existance of a quotation mark followed by a white space character.

# COMMAND ----------

# MAGIC %md There is a scenario within the weblogs where the HTTP request entry is a simple string with no quotation marks and no spaces.  Using our | operator, we can modify the pattern to capture either of these string patterns:`([^\s\"]|\"[^\"]*\")\s`
# MAGIC 
# MAGIC When reading this pattern, be sure to remember that the uptick character within square brackets applies NOT to any other characters identified within those same brackets.  Wih that in mind, [^\s\"] is read as NOT whitespace and not quotation mark.

# COMMAND ----------

# MAGIC %md Now, let's put these together with our earlier pattern: `^(\S*)\s(\S*)\s(\S)\s(-|\[.*\])\s([^\s\"]|\"[^\"]*\")\s`
# MAGIC 
# MAGIC Our next two fields to capture are the status and bytes fields. These both contain integer values but can also use short dashes when no value is provided for the field. The simple pattern (-|\d\*) should be suffient to capture information such as this and so we add these patterns to our larger pattern: 
# MAGIC `^(\S*)\s(\S*)\s(\S)\s(-|\[.*\])\s([^\s\"]|\"[^\"]*\")\s(-|\d*)\s(-|\d*)\s`

# COMMAND ----------

# MAGIC %md The next field is our referer. As with the HTTP request, this field can be represented by a short dash or a
# MAGIC long string.  If it's a long string, it is always wrapped in quotation marks. That leads us to this pattern: `([^\s\"]|\"[^\"]*\")`
# MAGIC 
# MAGIC Can you read this pattern?  It states that we are looking for anything not whitespace and not a quotation mark OR anything that starts with a quotation mark, through non-quotation marks, until a quotation mark is encountered.  It turns out this same pattern can be used to grab the next field, the agent information, so we'll tack two of these patterns to our larger pattern to complete our overall record match: `^(\S*) (\S*) (\S) (-|\[.*\]) ([^\s\"]|\"[^\"]*\") (-|\d*) (-|\d*) ([^\s\"]|\"[^\"]*\") ([^\s\"]|\"[^\"]*\")`
# MAGIC 
# MAGIC Notice that I've replaced the \s charaters that serve as field terminators with actual space charaters.  I've done this to improve the readability of the string.

# COMMAND ----------

# MAGIC %md The use of web server logs as data sources is very common.  so much so that if you do a quick search, you
# MAGIC will uncover a commmonly used pattern for parsing these that looks a little something like this: `^(\S*) (\S*) (\S) (-|\[.*\]) ([^\s\"]|\"[^\"]*\") (-|\d*) (-|\d*)(?: ([^\s\"]|\"[^\"]*\") ([^\s\"]|\"[^\"]*\"))+`

# COMMAND ----------

# MAGIC %md Notice this expression wraps the last two fields, the referer and the agent information fields, within a (?: )+ pattern.  The (?: ) characters denote a non-capturing group.  A non-capturing group specifies a pattern that we want to capture with RegEx but which we do not wish to return as a group.  While it's a bit confusing here, this non-capturing group is searching for two strings (separated by a space) but it doesn't want the two string returned as a single group.  
# MAGIC 
# MAGIC Notice that these two strings are themselves wrapped in parantheses making them each a group.  So while we want each string to be returned as a separate group, we don't want the two strings together returned as a single group.
# MAGIC 
# MAGIC Still confusing?  Notice the plus + symbol at the end of the non-capturing group. That symbol says that we expect one or multiple of these patterns.  That clues us in a bit better as to what's going on here.
# MAGIC 
# MAGIC This longer RegEx pattern is trying to anticipate a situation within which additional fields may be added to the weblog.  It's basically saying look for two fields at the end of this.  If you find multiples, the + sign will cause the RegEx engine to "walk" the string until the last occurance of the pattern at which point it will have found the last two fields on the row.  It will then return those two fields as the referer and agent info fields.
# MAGIC 
# MAGIC If you are still scratching your head on this one, don't sweat it.  This pattern is pretty advanced and not one that a typical RegEx developer would come up with. Still, it can be helpful to see how others anticipate and attempt to tackle some advance string parsing issues with RegEx.

# COMMAND ----------

import re
text1 = "alphanumeric@example.org"
text2 = "alphanumeric@long.subdomain.more.subdomain.domain.org"

r1 = re.compile(r'[a-zA-Z0-9]+@[a-zA-Z]+\.[a-zA-Z]+(\.[a-zA-Z]+)*')

print(r1.match(text1).group())
print(r1.match(text2).group())

print("part 2 - compare with and without (?:")
r2 = re.compile(r'[a-zA-Z0-9]+@[a-zA-Z]+\.[a-zA-Z]+(?:\.[a-zA-Z]+)*')

print(r1.findall(text2))
print(r2.findall(text2))

print("part 3 - udacity assignment")

def addresses(haystack): 
    emails = re.findall(r'\w+@[a-zA-Z]+\.[a-zA-Z]+(?:\.[a-zA-Z]+)*', haystack)
    return [re.sub("NOSPAM", "", email) for email in emails]

input1 = """louiseNOSPAMaston@germany.de (1814-1871) was an advocate for
democracy. irmgardNOSPAMkeun@NOSPAMweimar.NOSPAMde (1905-1982) wrote about
the early nazi era. rahelNOSPAMvarnhagen@berlin.de was honored with a 1994
deutsche bundespost stamp. seti@home is not actually an email address."""

output1 = ['louiseaston@germany.de', 'irmgardkeun@weimar.de', 'rahelvarnhagen@berlin.de']

print (addresses(input1) == output1)

# COMMAND ----------

test_str = 'abc123abc1234abc1235abc1236abc1237abc1238abc1239abc1230abc12311abc12312abc12313abc12314abc12315abc12316'

tst = re.findall(r'(?:(abc\d+))', test_str)
print([match for match in tst])

# COMMAND ----------

