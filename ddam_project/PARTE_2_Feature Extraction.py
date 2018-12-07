#!/usr/bin/env python
# coding: utf-8

# In[36]:


import toLog
log = toLog.log('Feature extraction starting')


# In[37]:


from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName('Python spark')
         .config('spark.some.config.option','some-value')
         .getOrCreate()
         )


# In[38]:


# read file from hdfs and infer schema
df_cleaned = spark.read.csv("hdfs://masterbig-1.itc.unipi.it:54310/user/student18/df_cleaned.csv", header = True, inferSchema = True)
df_cleaned.printSchema()


# In[43]:


df_cleaned.rdd.filter(lambda x: x['Review'] is None).count()


# In[39]:


# register table (if not exists)
try:
    df_cleaned.createTempView('hotels')
except:
    pass


# In[40]:


# selecting only the reviews using spark.sql
df_hotels = spark.sql("SELECT Hotel_Name, Review, lat, lng FROM hotels")
df_hotels.show()


# In[41]:


print(df_hotels.count())


# In[6]:


rddHotels = df_hotels.select('Hotel_Name', 'Review', 'lat', 'lng').rdd


# # Features extraction

# In[7]:


from getTriples import getTriples


# In[8]:


import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
vader = SentimentIntensityAnalyzer()


# In[9]:


def splitTriple(record):
    res = []
    for tripla in record[1]:
        res.append((record[0],tripla))
        
    return res


# In[ ]:


(rddHotels
 .filter(lambda x: x['Review'] is not None)
 .map(lambda x: (x['Hotel_Name'], getTriples(x['Review'])))
  .flatMap(splitTriple)
).take(40)


# In[ ]:


log.toLog('starting triples extraction')


# In[10]:


SAMPLE_PERC = 0.001
df_features = (rddHotels
 #.sample(False, SAMPLE_PERC) 
 .filter(lambda x: x['Review'] is not None)
 .map(lambda x: (x['Hotel_Name'], getTriples(x['Review'])))
 .flatMap(splitTriple)
 .map(lambda x:( x[0], x[1][0], vader.polarity_scores(" ".join(x[1]))['compound'] )  )
).toDF(["hotel", "feature", "scores"])


# In[11]:


# register table (if not exists)
try:
    df_features.createTempView('features')
except:
    spark.catalog.dropTempView('features')
    df_features.createTempView('features')


# In[12]:


spark.sql("SELECT hotel, feature, scores from features").show()


# In[13]:


spark.sql("SELECT hotel, feature, AVG(scores) as avg_scores, COUNT(scores) as n_scores FROM features GROUP BY hotel, feature ORDER BY hotel, avg_scores  ").show(40)


# In[14]:


df_features.count()


# # Defining categories using Word2Vec

# In[ ]:


log.toLog('starting Word2Vec')


# In[15]:


from pyspark.mllib.feature import Word2Vec


# In[19]:


rdd_tokens = (df_cleaned.select("Review").rdd
              #.sample(False,0.1)
              .filter(lambda x: x['Review'] is not None)
              .map(lambda x: x['Review'].split(" "))
             )


# In[20]:


word2Vec = Word2Vec().setMinCount(50).setVectorSize(200).setWindowSize(4)


# In[21]:


model = word2Vec.fit(rdd_tokens)


# In[22]:


categories = ['breakfast', 'staff', 'room', 'internet', 'location', 'bath', 'food']
#food(breakfast), #staff(service), #room, #internet(wi-fi), #location, #bath


# In[23]:


all_categories = {cat:dict(model.findSynonyms(cat, num = 20)) for cat in categories}


# In[24]:


from pprint import pprint


# In[25]:


pprint(all_categories)


# In[27]:


import json

with open('categories.json', 'w') as outfile:
    json.dump(all_categories, outfile)


# In[ ]:





# In[28]:


for category, dict_feat in all_categories.items():
    copyOfDict = dict(dict_feat)
    for word, similarity in copyOfDict.items():
        if similarity < 0.6:
            del dict_feat[word] 
    
pprint(all_categories)  


# In[ ]:


all_categories


# In[ ]:


#eliminare duplicati tra categorie differenti


# In[29]:


def assign_categories(feat):
    for cat,dict_feat in all_categories.items():
        if feat == cat or feat in dict_feat  :
            return cat
    
    return 'other'
    


# In[31]:


df_categories = (df_features.rdd
                 .map(lambda x: (x['hotel'],x['feature'],assign_categories(x['feature']), x['scores'] ))
                 .toDF(['hotel', 'feature', 'categories', 'score'])
                )


# In[32]:


df_categories.head(10)


# In[ ]:


df_categories.write.csv("hdfs://masterbig-1.itc.unipi.it:54310/user/student18/df_features.csv", header = True)


# In[33]:


# register table (if not exists)
try:
    df_categories.createTempView('categories')
except:
    spark.catalog.dropTempView('categories')
    df_categories.createTempView('categories')


# In[34]:


spark.sql("SELECT hotel, categories, feature, score from categories").show()


# In[35]:


spark.sql("SELECT hotel, categories, AVG(score) as avg_scores, COUNT(score) as n_scores FROM categories GROUP BY hotel, categories ORDER BY hotel, avg_scores  ")


# # Export final dataframe

# In[ ]:


log.toLog('start group by hotel/categories')


# In[ ]:


import pyspark.sql.functions as func


# In[ ]:


df_final = df_categories.groupBy("hotel").pivot('categories').agg(func.avg('score'))


# In[ ]:


df_final.write.csv("hdfs://masterbig-1.itc.unipi.it:54310/user/student18/df_categories.csv", header = True)


# In[ ]:


log.toLog('end feature extraction')
log.close()

