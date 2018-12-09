#!/usr/bin/env python
# coding: utf-8

# In[4]:


import toLog
log = toLog.log('Feature extraction starting')


# In[1]:


from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName('Python spark')
         .config('spark.some.config.option','some-value')
         .getOrCreate()
         )


# In[2]:


# read file from hdfs and infer schema
df_cleaned = spark.read.csv("hdfs://masterbig-1.itc.unipi.it:54310/user/student18/df_cleaned.csv", header = True, inferSchema = True)
df_cleaned.printSchema()


# In[3]:


df_cleaned.rdd.filter(lambda x: x['Review'] is None).count()


# In[4]:


# register table (if not exists)
try:
    df_cleaned.createTempView('hotels')
except:
    pass


# In[5]:


# selecting only the reviews using spark.sql
df_hotels = spark.sql("SELECT Hotel_Name, Review, lat, lng FROM hotels")
df_hotels.show()


# In[6]:


print(df_hotels.count())


# In[7]:


rddHotels = df_hotels.select('Hotel_Name', 'Review', 'lat', 'lng').rdd


# # Features extraction

# In[8]:


from getTriples import getTriples


# In[9]:


import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
vader = SentimentIntensityAnalyzer()


# In[10]:


def splitTriple(record):
    res = []
    for tripla in record[1]:
        res.append((record[0],tripla))
        
    return res


# In[15]:


(rddHotels
 .map(lambda x: (x['Hotel_Name'], x['Review'] ))
 .map(lambda x: (x[0], getTriples(x[1])))
 .flatMap(splitTriple)).take(40)


# In[19]:


log.toLog('starting triples extraction')


# In[16]:


SAMPLE_PERC = 0.001
df_features = (rddHotels
#.sample(False, SAMPLE_PERC) 
 .filter(lambda x: x['Review'] is not None)
 .map(lambda x: (x['Hotel_Name'], getTriples(x['Review'])))
 .flatMap(splitTriple)
 .map(lambda x:( x[0], x[1][0], vader.polarity_scores(" ".join(x[1]))['compound'] )  )
).toDF(["hotel", "feature", "scores"])


# In[17]:


# register table (if not exists)
try:
    df_features.createTempView('features')
except:
    spark.catalog.dropTempView('features')
    df_features.createTempView('features')


# In[18]:


spark.sql("SELECT hotel, feature, scores from features").show()


# In[19]:


spark.sql("SELECT hotel, feature, AVG(scores) as avg_scores, COUNT(scores) as n_scores FROM features GROUP BY hotel, feature ORDER BY hotel, avg_scores  ").show(40)


# In[20]:


df_features.count()


# In[21]:


import json
# reading the defined categories
with open('final_categories.json') as f:
     categories = json.load(f)


# In[22]:


categories


# In[23]:


def assign_categories(feat):
    for cat,dict_feat in categories.items():
        if feat == cat or feat in dict_feat  :
            return cat
    
    return 'other'
    


# In[24]:


df_categories = (df_features.rdd
                 .map(lambda x: (x['hotel'],x['feature'],assign_categories(x['feature']), x['scores'] ))
                 .toDF(['hotel', 'feature', 'categories', 'score'])
                )


# In[25]:


df_categories.head(20)


# In[ ]:


df_categories.write.csv("hdfs://masterbig-1.itc.unipi.it:54310/user/student18/df_features.csv", header = True)


# In[26]:


# register table (if not exists)
try:
    df_categories.createTempView('categories')
except:
    spark.catalog.dropTempView('categories')
    df_categories.createTempView('categories')


# In[27]:


spark.sql("SELECT hotel, categories, feature, score from categories").show()


# In[35]:


spark.sql("SELECT hotel, categories, AVG(score) as avg_scores, COUNT(score) as n_scores FROM categories GROUP BY hotel, categories ORDER BY hotel, avg_scores  ")


# # Export final dataframe

# In[ ]:


log.toLog('start group by hotel/categories')


# In[28]:


import pyspark.sql.functions as func


# In[29]:


df_final = df_categories.groupBy("hotel").pivot('categories').agg(func.avg('score'))


# In[ ]:


df_final.write.csv("hdfs://masterbig-1.itc.unipi.it:54310/user/student18/df_categories.csv", header = True)


# In[ ]:


log.toLog('end feature extraction')
log.close()

