#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
spark = (SparkSession.builder
         .appName('ddam_project')
         .config('spark.some.config.option','some-value')
         .getOrCreate()
         )


# In[2]:


# read file from hdfs and infer schema
df_raw = spark.read.csv("hdfs://masterbig-1.itc.unipi.it:54310/user/student18/Hotel_Reviews.csv", header = True, inferSchema = True)
df_raw.printSchema()


# In[3]:


from pyspark.sql.functions import monotonically_increasing_id


# In[4]:


df_raw_id = df_raw.withColumn('id', monotonically_increasing_id())
df_raw_id.printSchema()
df_raw_id.take(1)


# ## Reviews transformation

# In[5]:


def catReviews(row):
    if row["Negative_Review"] == "No Negative" and row["Positive_Review"] == "No Positive":
        return "EMPTY"
    else:
        if row["Negative_Review"] == "No Negative":
            return (row["Positive_Review"].lower())
        elif row["Positive_Review"] == "No Positive":
            return (row["Negative_Review"].lower() )
        else:
            return(row["Negative_Review"].lower()  + ". " + row["Positive_Review"].lower())
            #users_ratings.append(row[12])       
        


# In[6]:


def correction(row):
    return (row.replace(" don t ", " don't ")
            .replace(" didn t ", " didn't ")
            .replace(" haven t ", " haven't ")
            .replace(" hadn t ", " hadn't ")
            .replace(" isn t ", " isn't ")
            .replace(" weren t ", " weren't ")
            .replace(" wasn t ", " wasn't ")
            .replace(" dont ", " don't ")
            .replace(" didnt ", " didn't ")
            .replace(" i ", " I ")
           )


# In[53]:


#counting rows with empty reviews
#df_raw_id.rdd.map(lambda x: (x['id'], catReviews(x))
#                  .filter(lambda x: x[1] == "EMPTY").count().take(1) )


# In[7]:


rdd_reviews = (df_raw_id.rdd.map(lambda x: (x['id'], catReviews(x)))
               .filter(lambda x: x[1] != "EMPTY")
               .map(lambda x: (x[0], correction(x[1]))) 
              )


# In[ ]:


#rdd_reviews.take(3)


# In[33]:





# In[ ]:





# In[ ]:





# # Keep only English reviews

# In[8]:


import langdetect as ld


# In[9]:


#detect english reviews


#reviews_rdd.map(ld.detect).take(10)
def detect_Eng(review):
    if (len(review) < 100 ):
        return True
    try:
        if ld.detect(review)== 'en':
            return True
        else:
            return False
    except:
        return True


# In[32]:


#stampa le reviews in lingua differente dall'inglese
#rdd_reviews.filter(lambda x: not detect_Eng(x[1])).take(30)


# In[10]:


#remove EMPTY reviews, keep only english reviews
#creiamo un nuovo data frame(df_revs) con colonne id, Review

df_revs = rdd_reviews.filter(lambda x: detect_Eng(x[1])).toDF(['id','Review'])
df_revs.printSchema()


# In[22]:


#df_revs.take(3)


# In[11]:


#creiamo un nuovo dataframe con le review modificate, eliminando quelle "vecchie" e senza contare 
df_cleaned = df_raw_id.join(df_revs, ['id']).drop("Positive_Review", "Negative_Review")


# In[13]:


df_cleaned.take(3)


# In[ ]:


df_cleaned.write.csv("hdfs://masterbig-1.itc.unipi.it:54310/user/student18/df_cleaned.csv", header = True)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




