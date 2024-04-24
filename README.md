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

- import json
- import random
- import time
- from tqdm import tqdm
- import pandas as pd
- import numpy as np
- import re
- import nltk
- from nltk.corpus import stopwords
- from nltk.tokenize import word_tokenize
- from sklearn.preprocessing import MultiLabelBinarizer
- import streamlit as st
- from bs4 import BeautifulSoup
- import gc
- import swifter


# Pre Processing Data 

- First of all, we have to sample out dataset to atleast 20gb. We make a sample file named : All_Amazon_Meta_Sampled.json
- Secondly, read the sample file into the dataframe using pandas. 
- Then, drop the unnecessary columns and only select the relevant columns.
- Moreover, check for null values and remove if any.
- After removing the null values, clean the columns that have texts after observing a sample of 5 to 10 records.
- Remove the used columns and then convert the processed dataframe into a json file for further calculations.


# Frequent Itemset Mining

Create a producer.py file that initializes a Kafka producer to send JSON data to a Kafka topic. It reads data from a JSON file line by line, serializes it, and sends it to the specified Kafka topic.

Frequent Itemsets are found using following 2 approaches :

- Consumer 1 : Apriori Algorithm

  Make first consumer application , which analyzes streaming data from Kafka to find frequent itemsets and generate association rules using the Apriori algorithm. It initializes connections to Kafka and MongoDB, updates frequent itemsets based on incoming transactions, and generates association rules. Finally, it continuously processes incoming data to provide insights into item relationships.

- Consumer 2 : PCY Algorithm

  The second consumer application mines association rules from streaming data using the PCY algorithm. It updates pair counts, determines frequent pairs with PCY, and generates association rules based on the frequent itemsets. The main loop processes streaming data from Kafka, updating counts and generating rules within a sliding window. Finally, it stores the results in MongoDB.
    
- Consumer 3 : Anomoly Detection

  Make a third consumer application which analyzes streaming data from Kafka to detect anomalies in temporal patterns of item transactions. It updates frequent itemsets for pairs of items and maintains historical counts for each item. Anomalies are detected using Z-scores calculated from historical counts. If an anomaly is detected, it is recorded in MongoDB. The main loop processes incoming data, updating counts and detecting anomalies within a sliding window of transactions.



# Database Integration
Each consumer stores the data in MongoDB. The name of MongoDB is 'Frequent_Itemset'. 

- Apriori stores frequent itemsets in collection named 'apriori' which includes frequent itemsets their support, count and transactions and stores its association rules in collection 'apriori_associationrules' storing antecednt, consequent, support , confidence and transaction.
- PCY stores its frequent itemsets in collection named 'pcy_frequent_itemsets' which includes frequent itemsets their support, count and transactions and stores its association rules in a separate collection 'pcy_association_rules' storing antecednt, consequent, support , confidence and transaction.
- Anomoly detection saves its itemsets in collection named 'anomalies' which has items , z-score and transaction count.



# Running on Kafka :
- Ensure that kafka is present in the home directory.
- Put all the .py files i.e. producer and consumers  along with the bash script (Bash_Rc.sh) are present in Documents directory.
- Open terminal and write the following command :
  ./Bash_Rc.sh
