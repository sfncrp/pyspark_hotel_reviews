#!/usr/bin/env python
# coding: utf-8

# In[1]:


import toLog
log = toLog.log('Feature extraction starting')


# In[2]:


from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName('Python spark')
         .config('spark.some.config.option','some-value')
         .getOrCreate()
         )


# In[3]:


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


# In[15]:


(rddHotels
 .map(lambda x: (x['Hotel_Name'], x['Review'] ))
 .map(lambda x: (x[0], getTriples(x[1])))
 .flatMap(splitTriple)).take(40)


# In[19]:


log.toLog('starting triples extraction')


# In[38]:


SAMPLE_PERC = 0.001
df_features = (rddHotels
 #.sample(False, SAMPLE_PERC)
 .filter(lambda x: x['Review'] is not None)
 .map(lambda x: (x['Hotel_Name'], getTriples(x['Review'])))
 .flatMap(splitTriple)
 .map(lambda x:( x[0], x[1][0], (vader.polarity_scores(" ".join(x[1]))['compound']+1)*5) )
              ).toDF(["hotel", "feature", "scores"])


# In[39]:


# register table (if not exists)
try:
    df_features.createTempView('features')
except:
    spark.catalog.dropTempView('features')
    df_features.createTempView('features')


# In[40]:


spark.sql("SELECT hotel, feature, scores from features order by scores ").show()


# In[41]:


spark.sql("SELECT hotel, feature, AVG(scores) as avg_scores, COUNT(scores) as n_scores FROM features GROUP BY hotel, feature ORDER BY hotel, avg_scores  ").show(40)


# In[42]:


df_features.count()


# In[43]:


import json
# reading the defined categories
with open('final_categories.json') as f:
     categories = json.load(f)


# In[44]:


categories


# In[45]:


def assign_categories(feat):
    'assign category to each feature'
    for cat,dict_feat in categories.items():
        if feat == cat or feat in dict_feat  :
            return cat
    
    return 'other'
    


# In[46]:


df_features = (df_features.rdd
                 .map(lambda x: (x['hotel'],x['feature'],assign_categories(x['feature']), x['scores'] ))
                 .toDF(['hotel', 'feature', 'categories', 'score'])
                )


# In[47]:


df_features.head(20)


# In[ ]:


df_features.write.csv("hdfs://masterbig-1.itc.unipi.it:54310/user/student18/df_features.csv", header = True)


# # Features cleaning

# In[3]:


#df_features = spark.read.csv("hdfs://masterbig-1.itc.unipi.it:54310/user/student18/df_features.csv", header = True, inferSchema = True)


# In[48]:


df_features.printSchema()


# In[50]:


try:
    df_features.createTempView('df_features')
except:
    spark.catalog.dropTempView('df_features')
    df_features.createTempView('df_features')


# In[51]:


#remove all features with neutral score
df_features_temp = spark.sql("SELECT * FROM df_features WHERE score != 5.0 and categories != 'other'")


# In[52]:


try:
    df_features_temp.createTempView('temp')
except:
    spark.catalog.dropTempView('temp')
    df_features_temp.createTempView('temp')


# In[ ]:





# In[53]:


spark.sql("SELECT hotel, count(categories) as conto FROM temp WHERE categories != 'other' GROUP BY hotel having conto > 600  order by conto DESC ").show()


# In[54]:


df_features_cleaned = spark.sql("SELECT hotel, categories, feature, score, count(categories) over(partition by(hotel) order by score) as conto, hotel FROM temp WHERE categories != 'other' ORDER BY conto DESC").filter('conto > 70')


# In[55]:


df_features_cleaned.count()


# In[ ]:





# In[ ]:





# In[ ]:





# In[56]:


# register table 
try:
    df_features_cleaned.createTempView('categories')
except:
    spark.catalog.dropTempView('categories')
    df_features_cleaned.createTempView('categories')


# In[57]:


spark.sql("SELECT hotel, categories, feature, score from categories").show()


# In[58]:


spark.sql("SELECT hotel, categories, AVG(score) as avg_scores, COUNT(score) as n_scores FROM categories GROUP BY hotel, categories ORDER BY hotel, avg_scores  ").show()


# # Export final dataframe

# In[ ]:


log.toLog('start group by hotel/categories')


# In[43]:


import pyspark.sql.functions as func


# In[44]:


df_final = df_features_cleaned.groupBy("hotel").pivot('categories').agg(func.avg('score'))


# In[45]:


df_final.write.csv("hdfs://masterbig-1.itc.unipi.it:54310/user/student18/df_categories.csv", header = True)


# In[ ]:


log.toLog('end feature extraction')
log.close()

