#!/usr/bin/env python
# coding: utf-8

# # Data transforming and cleaning

# In[1]:


import toLog
log = toLog.log('ETL process start')


# In[2]:


from pyspark.sql import SparkSession
spark = (SparkSession.builder
         .appName('ddam_project')
         .config('spark.some.config.option','some-value')
         .getOrCreate()
         )


# In[3]:


# read file from hdfs and infer schema
df_raw = spark.read.csv("hdfs://masterbig-1.itc.unipi.it:54310/user/student18/Hotel_Reviews.csv", header = True, inferSchema = True)
df_raw.printSchema()


# In[4]:


N_raw = df_raw.count()
print(N_raw)


# In[5]:


from pyspark.sql.functions import monotonically_increasing_id


# In[6]:


# adding id
df_raw_id = df_raw.withColumn('id', monotonically_increasing_id())
df_raw_id.printSchema()
df_raw_id.take(1)


# ## Reviews transformation

# In[7]:


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
        


# In[8]:


def catReviewsLen(row, L = 11):
    out = ""
    if len(row["Negative_Review"]) > L:
        out += row["Negative_Review"]
    if len(row["Positive_Review"]) > L:
        out += '.' + row["Positive_Review"]
        
    return out.lower()
     


# In[10]:


def correction(row):
    ''' Corrections and (some) stop words removal '''
    return (row.replace(" don t ", " don't ")
            .replace(" didn t ", " didn't ")
            .replace(" haven t ", " haven't ")
            .replace(" hadn t ", " hadn't ")
            .replace(" isn t ", " isn't ")
            .replace(" weren t ", " weren't ")
            .replace(" wasn t ", " wasn't ")
            .replace(" dont ", " don't ")
            .replace(" didnt ", " didn't ")
            .replace(" wont ", " won't ")
            .replace(" won t ", " won't ")
            .replace(" wouldn t ", " wouldn't ")
            .replace(" wouldnt ", " wouldn't ")
            .replace(" i ", " I ")
            .replace(" a "," ") # removing stop words 
            .replace(" self "," ")
            .replace(" it "," ")
            .replace(" bit "," ")
            .replace(" lot "," ")
            .replace(" also "," ")
            .replace(" wi-fi ","wifi")
            .replace(" s ","")
            .replace(" the "," ")
           )


# In[ ]:


rdd_reviews = (df_raw_id.rdd
               #.sample(False, 0.10)
               .map(lambda x: (x['id'], catReviewsLen(x)))
               .filter(lambda x: len(x[1]) > 1) # remove empty reviews
               .map(lambda x: (x[0], correction(x[1]))) 
              )


# In[ ]:


N_cat = rdd_reviews.count()
print(N_cat)


# In[ ]:


# removed reviews:
N_raw - N_cat


# In[ ]:


log.toLog( 'counting removed reviews: '+str(N_raw - N_cat))


# In[ ]:


#rdd_reviews.filter(lambda x: len(x[1])<1 ).count()


# # Keep only English reviews

# In[ ]:


import langdetect as ld


# In[ ]:


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


# In[ ]:


#stampa le reviews in lingua differente dall'inglese
#rdd_reviews.filter(lambda x: not detect_Eng(x[1])).take(30)


# In[ ]:


#rdd_reviews.filter(lambda x: x[1] is None).count()


# In[ ]:


log.toLog('starting detect_Eng')


# In[ ]:


#remove EMPTY reviews, keep only english reviews
#creiamo un nuovo data frame(df_revs) con colonne id, Review

df_lang = (rdd_reviews.filter(lambda x: detect_Eng(x[1]))
           #.filter(lambda x: x[1] is not None)
           .toDF(['id','Review'])
          )
df_lang.printSchema()


# In[ ]:


df_lang.take(3)


# In[ ]:


N_cat_lang = df_lang.count()
N_cat_lang


# In[ ]:


# Not english reviews removed:
N_cat - N_cat_lang


# In[ ]:


#creiamo un nuovo dataframe con le review modificate, eliminando quelle "vecchie" e senza contare 
df_cleaned_lang = df_raw_id.join(df_lang, 'id', 'inner').drop("Positive_Review", "Negative_Review")
df_cleaned_lang.printSchema()


# In[ ]:


N_cleaned_lang = df_cleaned_lang.count()
log.toLog('counted not english rev, removed:' + str(N_raw-N_cleaned_lang))
N_cleaned_lang


# # Hotel nationality

# In[12]:


import reverse_geocode
log.toLog('starting country extraction')


# In[13]:


# example
coord =[ (43.6753176,10.5408628) ]
reverse_geocode.search(coord)


# In[14]:


# check na value for lat/lng
#(df_cleaned_lang
# .sample(False, 0.01)
# .select('lat','lng').rdd.filter(lambda x: x['lng']== 'NA').count()
#)


# In[ ]:





# In[15]:


df_coord = ( df_raw_id.select('id', 'lat', 'lng').rdd
            #.sample(False, 0.01)
            .filter(lambda x: x['lat']!='NA' and x['lng']!='NA')
            .map(lambda x: (x['id'], [ [float(x['lat']), float(x['lng'])] ] )) 
            .map(lambda x: (x[0], reverse_geocode.search(x[1])))
            .map(lambda x: (x[0] , x[1][0]['city'], x[1][0]['country'], x[1][0]['country_code']))
           ).toDF(['id','h_city','h_country','h_country_code'])


# In[17]:


df_coord.show(10)


# In[19]:


df_coord.createTempView('coord')


# In[21]:


spark.sql("SELECT h_city, COUNT(h_city) AS n_city           FROM coord           GROUP BY h_city           ORDER BY n_city DESC           ").show()            
          


# In[22]:


hotel_countries = df_coord.select('h_country').distinct().rdd.map(lambda x: x['h_country']).collect()
hotel_countries


# In[25]:


#hotel_cities = df_coord.select('h_city').distinct().rdd.map(lambda x: x['h_city']).collect()
#hotel_cities


# In[ ]:


# estrarre la nazionalità dall'indirizzo
def extractCountry(row):
    for country in hotel_countries:
        if country.lower() in row['Hotel_Address'].lower():
            return (row['id'], country )
    return 'EMPTY'
 


# In[ ]:


rdd_hotel_countries = (df_raw_id
                       #.sample(False, 0.005)
                       .select('id', 'Hotel_Address').rdd.map(extractCountry)
                      )


# In[ ]:


# check if all reviews have an associated hotel_country 
# rdd_hotel_countries.filter(lambda x: x[1] == 'EMPTY').count()


# In[ ]:


# rdd_hotel_countries.take(1)


# In[ ]:


df_hotel_countries = rdd_hotel_countries.toDF(['id', 'Hotel_Country'])


# In[ ]:


print (df_hotel_countries.show())


# ## Join and save cleaned df to hdfs

# In[ ]:


log.toLog( 'started final join and write to hdfs')


# In[ ]:


df_cleaned = df_cleaned_lang.join(df_hotel_countries, 'id', 'inner')


# In[ ]:


df_cleaned.write.csv("hdfs://masterbig-1.itc.unipi.it:54310/user/student18/df_cleaned.csv", header = True)


# In[26]:


log.toLog( 'etl notebook finished')
log.close(' ')


# In[ ]:




