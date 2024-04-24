# Frequent Itemset Analysis on Amazon Metadata Using Apriori And PCY With Data Base Integration
This repository contains files for finding frequent itemsets utilizing Apache Kafka on Amazon Metadata in JSON (JavaScript Object Notation) format. 

# Collaborators :
The group members for this assignment are :
- Ahmad Akhtar 21I-1655
- Inam ul Haq 22I-1906
- Abdurrehman 22I-1963

# Data Set 
You can download data set (meta data 12gb) from this link:

"https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_v2/"

Extract the zip to get the dataset of about 105gb.

# Dependencies: 
Important Libraries required for the preprocessing of this data and to create a sample data are listed below :

-import json
-import random
-import time
-from tqdm import tqdm
-import pandas as pd
-import numpy as np
-import re
-import nltk
-from nltk.corpus import stopwords
-from nltk.tokenize import word_tokenize
-from sklearn.preprocessing import MultiLabelBinarizer
-import streamlit as st
-from bs4 import BeautifulSoup
-import gc
-import swifter


# Create Sample Dataset
First of all, we have to sample out dataset to atleast 15gb. We make a sample file named : All_Amazon_Meta_Sampled.json

# Pre Processing Data 
First , read the sample file into the dataframe using pandas. Then, drop the unnecessary columns and only select the relevant columns. Moreover, check for null values and remove if any. After removing the null values, clean the columns that have texts after observing a sample of 5 to 10 records. Remove the used columns and then convert the processed dataframe into a json file for further calculations.




