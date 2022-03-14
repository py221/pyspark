# Databricks notebook source
# MAGIC %md 
# MAGIC ###Images: Images as Data Sources
# MAGIC 
# MAGIC **Objective:** The objective of this lab is to demonstrate how data can be retrieved from popular image formats.

# COMMAND ----------

# MAGIC %sh 
# MAGIC rm -R ./images 
# MAGIC mkdir images
# MAGIC cd images
# MAGIC wget https://smithbc.blob.core.windows.net/downloads/images/flower.gif >/dev/null 2>&1
# MAGIC wget https://smithbc.blob.core.windows.net/downloads/images/flower.jpg >/dev/null 2>&1
# MAGIC wget https://smithbc.blob.core.windows.net/downloads/images/flower.png >/dev/null 2>&1
# MAGIC wget https://smithbc.blob.core.windows.net/downloads/mnist/train/2/10009.png -O digit.png>/dev/null 2>&1
# MAGIC wget https://smithbc.blob.core.windows.net/downloads/images/bird.jpg >/dev/null 2>&1
# MAGIC cd ..
# MAGIC ls -l -R ./images 

# COMMAND ----------

# MAGIC %md In our last lab, we explored the concept of an image as a collection of pixels.  These pixels were read from a CSV text file and then interpretted as an image, hopefully removing some of the mystery of the pixel and image concept in the process. Still, images are not commonly transmitted as text files.  Instead, they are often encoded in formats such as JPEG, GIF and PNG. The purpose of this lab is to show you how data in these (and other common) formats can be read as data sources in Python.
# MAGIC 
# MAGIC To get started, we will load the Pillow library.  Pillow is the Python 3 implementation of the Python Image Library (PIL). PIL was actively developed for Python 2 but fell out of support with it's last update in 2011. Today, Pillow is the standard library for base image handling in Python but you will often see it referred to as PIL which can make this part of the Python literature a little confusing:

# COMMAND ----------

# load libraries used throughout this lab
dbutils.library.installPyPI('Pillow')
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md To get started, let's load one of our MNIST images using the Pillow library:

# COMMAND ----------

from PIL import Image
import numpy as np
import matplotlib.pyplot as plt

# COMMAND ----------

image_pil = Image.open('./images/digit.png')

# COMMAND ----------

# MAGIC %md With our image loaded, we can now examine it's properties to extract some info about it:

# COMMAND ----------

# summarize some details about the image
print('Format:\t{0}'.format(image_pil.format))
print('Image Size:\t{0}'.format(image_pil.size))
print('Mode:\t{0}'.format(image_pil.mode)) # mode of L means the image is black and white with 256 possible shades available
print(image_pil.info) # no info on this image

# COMMAND ----------

# MAGIC %md To examine the pixels that make up an image, you can use the image's getdate method. This returns a 1-dimensional sequence of pixels.  We need to explicitly convert this into a numpy array:

# COMMAND ----------

image_np = np.array(image_pil.getdata())
image_np.shape

# COMMAND ----------

# MAGIC %md Because we know the intended shape of this image, we can use the reshape method on the numpy array created in the last cell to flip the array from a 1D to a 2D structure as was done in the last lab.  Alternatively, we can just read the image directly into numpy without a method call and let PIL do this for us.  This and the last method are shown in this notebook as you will commonly see both techniques in image handling code samples:

# COMMAND ----------

image_np = np.asarray( image_pil )
image_np.shape

# COMMAND ----------

# MAGIC %md To visualize this image, we can return to pyplot's imshow function.  We use this function to view the array instead of directly calling the show method on the PIL image as this second appraoch depends on the compatibility of the image viewers employed by PIL with our notebook.  Both Jupyter and Databricks notebooks do not support PIL viewers:

# COMMAND ----------

plt.imshow(image_np, cmap='gray')
plt.show()

display()

# COMMAND ----------

# MAGIC %md If our goal is simply to load an image for viewing in a notebook, you might wish to make use of matplotlib's image module which will allow you to skip the process of reading an image using Pillow, extracting the array, and then rendering the array:

# COMMAND ----------

import matplotlib.image as mpimg

image_np = mpimg.imread('./images/digit.png')
plt.imshow(image_np, cmap='gray')
plt.show()

display()

# COMMAND ----------

# MAGIC %md With some basics under our belts, let's examine some more complex images and their formatting, starting with the PNG format.
# MAGIC 
# MAGIC The Portable Network Graphics (PNG) format supports grayscale and color images with up to 16 million possible colors.  It also supports pixel-level transparency settings.  This can add up to quite a bit of information packed into an image. For this reason, PNG employs compression but does so in a manner that perfectly preserves the image quality upon rendering, *i.e.* lossless compression:

# COMMAND ----------

# load the image as a PIL image object
image_pil = Image.open('./images/flower.png')

# summarize some details about the image
print('Format:\t{0}'.format(image_pil.format))
print('Image Size:\t{0}'.format(image_pil.size))
print('Mode:\t{0}'.format(image_pil.mode))
print(image_pil.info)

# COMMAND ----------

image_np = np.array(image_pil)
plt.imshow(image_np)

display()

# COMMAND ----------

# MAGIC %md Notice the mode of this image is identified as RGBA.  This means that each pixel in the image stores a value for the red, green, blue and alpha, *i.e* transparency, channels. This means that unlike a grayscale image (which identified itself with a mode of L), a color PNG image packs 4-values per pixel.  You can see this when you examine the shape of the numpy array associated with the image, which is now a 3-dimensional array:

# COMMAND ----------

image_np.shape

# COMMAND ----------

# MAGIC %md When we say *3-dimensinsional*, it's important not to think of the array like you would a 3-dimensional object in the physical world.  Instead, think of each pixel in the array as containing a list of values.  To make this a bit more clear, let's take a look at these values for the pixel at the center of this 168x168 image, *i.e.* the pixel at row 84, column 84:

# COMMAND ----------

image_np[84,84,:] # Red, Green, Blue, Alpha (degree of transparency)

# COMMAND ----------

# MAGIC %md Notice this one pixel contains an array.  That array contains 4 values.  Each identifies the amount of red, green, blue and transparency (with 255 indicating no transparency and 0 indicating complete transparency), respectively.  To visualize these separately, you can separate each channel and display with an appropriate color map:
# MAGIC 
# MAGIC **NOTE** The transparency channel displayed as a grayscale map for visualization purposes only.

# COMMAND ----------

image_np = np.array(image_pil)

plt.figure(figsize=(25,5))

plt.subplot(1,5,1)
plt.imshow(image_np)

plt.subplot(1,5,2)
plt.imshow(image_np[:,:,0], cmap='Reds_r')

plt.subplot(1,5,3)
plt.imshow(image_np[:,:,1], cmap='Blues_r')

plt.subplot(1,5,4)
plt.imshow(image_np[:,:,2], cmap='Greens_r')

plt.subplot(1,5,5)
plt.imshow(image_np[:,:,3], cmap='Greys_r')

display()

# COMMAND ----------

# MAGIC %md The Joint Photographic Experts Group (JPEG) format is very similar to that of PNG.  However, it originated at a time when newly emerging digital cameras had limited storage space.  As such, it uses a more aggressive form of compression that significantly reduces image size but causes the loss of image fidelity, *i.e.* lossy compression.  As storage technology has evolved, the consequences of the lossy compression within JPEG has caused many to begin moving away from this format.  Still, it is very frequently encountered today:

# COMMAND ----------

# load the image as a PIL image object
image_pil = Image.open('./images/flower.jpg')

# summarize some details about the image
print('Format:\t{0}'.format(image_pil.format))
print('Image Size:\t{0}'.format(image_pil.size))
print('Mode:\t{0}'.format(image_pil.mode))
print(image_pil.info)

# COMMAND ----------

image_np = np.array(image_pil)

# reset figure settings from prior cells
plt.clf()

plt.imshow(image_np)
display()

# COMMAND ----------

# MAGIC %md Notice that the JPEG format uses a RGB mode instead of an RGBA.  This means the JPEG images do not support the concept of image transparency.  This can be seen in the 3-dimensional structure of the image array where each pixel only has 3 values:

# COMMAND ----------

image_np.shape

# COMMAND ----------

image_np[84,84,:] # Red, Green, Blue

# COMMAND ----------

# MAGIC %md Finally, we come to the Graphics Interchange Format (GIF). The GIF format is another format that was made popular in the earlier days of digital imagery.  It employs a lossless compression much like PNG but limits the size of images by reducing the range of available colors.  To do this, pixels are assigned a single value which are mapped back to a palete that instructs the computer as to how the range of colors in the image should be rendered or whether a pixel should be treated as transparent.  By reducing the bytes per pixel, the image size is greatly reduced at the cost of overall image color range:

# COMMAND ----------

# load the image as a PIL image object
image_pil = Image.open('./images/flower.gif')

# summarize some details about the image
print('Format:\t{0}'.format(image_pil.format))
print('Image Size:\t{0}'.format(image_pil.size))
print('Mode:\t{0}'.format(image_pil.mode))
print(image_pil.palette)
print(image_pil.info)

# COMMAND ----------

image_np = np.array(image_pil)
image_np.shape

# COMMAND ----------

image_np[84,84]

# COMMAND ----------

# MAGIC %md Because GIFs do not have a native three-channel range, you must force PIL to use the image's palette to generate the three-channel colors required to render the image properly in a notebook:

# COMMAND ----------

image_np = np.array(image_pil.convert('RGB'))

plt.imshow(image_np)
display()

# COMMAND ----------

# MAGIC %md Both JPEG and PNG (but not GIF), support an additional feature called Exchangeable Image File Format (EXIF).  EXIF allows metadata about the an image to be embedded in the headers of the image.  This does not affect the pixels of the image that we have been exploring but is part of the other data that makes up each image file.
# MAGIC 
# MAGIC To see how this information may be useful, consider this image taken with a cell phone camera:

# COMMAND ----------

# load the image as a PIL image object
image_pil = Image.open('./images/bird.jpg')

# summarize some details about the image
print('Format:\t{0}'.format(image_pil.format))
print('Image Size:\t{0}'.format(image_pil.size))
print('Mode:\t{0}'.format(image_pil.mode))
print(image_pil.info)

# COMMAND ----------

# load the image as a numpy array
image_np = np.asarray(image_pil)

# display the image
plt.imshow(image_np)
plt.show()

display()

# COMMAND ----------

# MAGIC %md The EXIF information is presented as binary information in the image's informational structures.  Using the \_getexif method on the image, this information can be extracted as a dictionary.  This dictionary presents each EXIF element as a key-value pair where the key is an EXIF standard integer value.  Using PIL's ExifTags enumeration, we can translate these into some friendly names. 
# MAGIC 
# MAGIC There's a lot to digest in this block but you'll likely just want to copy and paste it should you need it in the future so don't work too hard to understand it; just focus on the output:

# COMMAND ----------

from PIL import ExifTags

exif = {}

# for each key-value pair in the exif dictionary
for k, v in image_pil._getexif().items():
  
  # if the key is a recognized EXIF key
  if k in ExifTags.TAGS:
    # translate the key to a friendly name
    key = ExifTags.TAGS[k]
    
    # if that friendly name is GPSInfo, it's value is a nested dictionary of other EXIF tags
    if key=='GPSInfo':
      gps = {}
      # for each GPSInfo key, value
      for kg, vg in v.items():
        # if the GPSInfo key is recognized
        if kg in ExifTags.GPSTAGS:
          # translate it to a friendly name
          gps[ExifTags.GPSTAGS[kg]] = vg
      # return the translated GPSInfo value (nested dicitionary)    
      v = gps
      
    exif[key] = v

# display the exif data
exif

# COMMAND ----------

# MAGIC %md There's quite a bit of useful info in the EXIF metadata.  Can you see why the image is rotated the way it is?  (The Orientation value of 6 tells you I was holding my phone upright.)  Can you tell the date when I took the photo?  (Look for the DateTimeOriginal value.)  Can you tell where I was when I took this photo?  This last one is a bit tricky as the info is embedded in the GPSInfo tag which contains a nested dictionary of values.  The latitude and longitude information recorded there can vary a bit by device manufacturer but for this version of the iPhone, this information can be converted to decimal degrees as follows:

# COMMAND ----------

latitude = (exif['GPSInfo']['GPSLatitude'], exif['GPSInfo']['GPSLatitudeRef'])
longitude = (exif['GPSInfo']['GPSLongitude'], exif['GPSInfo']['GPSLongitudeRef'])

def calculate_decimal_degrees(dms):
  if dms[1] in ['S','W']: 
    factor = -1 
  else: 
    factor = 1
  
  degs = dms[0][0]
  mins = dms[0][1]
  secs = dms[0][2]
    
  return factor * (degs.numerator/degs.denominator + mins.numerator/mins.denominator/60 + secs.numerator/secs.denominator/3600)
  
print( '({0}, {1})'.format(calculate_decimal_degrees(latitude), calculate_decimal_degrees(longitude)))