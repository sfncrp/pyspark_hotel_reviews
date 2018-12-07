
# coding: utf-8

# # Data transforming and cleaning

# In[53]:

import toLog
log = toLog.log('ETL process start')


# In[94]:

from pyspark.sql import SparkSession
spark = (SparkSession.builder
         .appName('ddam_project')
         .config('spark.some.config.option','some-value')
         .getOrCreate()
         )


# In[95]:

# read file from hdfs and infer schema
df_raw = spark.read.csv("hdfs://masterbig-1.itc.unipi.it:54310/user/student18/Hotel_Reviews.csv", header = True, inferSchema = True)
df_raw.printSchema()


# In[96]:

N_raw = df_raw.count()
print(N_raw)


# In[97]:

from pyspark.sql.functions import monotonically_increasing_id


# In[98]:

# adding id
df_raw_id = df_raw.withColumn('id', monotonically_increasing_id())
df_raw_id.printSchema()
df_raw_id.take(1)


# ## Reviews transformation

# In[99]:

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
        


# In[100]:

def catReviewsLen(row, L = 11):
    out = ""
    if len(row["Negative_Review"]) > L:
        out += row["Negative_Review"]
    if len(row["Positive_Review"]) > L:
        out += '.' + row["Positive_Review"]
        
    return out.lower()
     


# In[101]:

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
            .replace(" wont ", " won't ")
            .replace(" won t ", " won't ")
            .replace(" wouldn t ", " wouldn't ")
            .replace(" wouldnt ", " wouldn't ")
            .replace(" i ", " I ")
           )


# In[102]:

#counting rows with empty reviews
#df_raw_id.rdd.map(lambda x: (x['id'], catReviews(x))
#                  .filter(lambda x: x[1] == "EMPTY").count().take(1) )


# In[103]:

rdd_reviews = (df_raw_id.rdd
               #.sample(False, 0.10)
               .map(lambda x: (x['id'], catReviewsLen(x)))
               .filter(lambda x: len(x[1]) > 1) # remove empty reviews
               .map(lambda x: (x[0], correction(x[1]))) 
              )


# In[104]:

N_cat = rdd_reviews.count()
print(N_cat)


# In[105]:

# removed reviews:
N_raw - N_cat


# In[12]:

log.toLog( 'counting removed reviews: '+str(N_raw - N_cat))


# In[34]:

#rdd_reviews.filter(lambda x: len(x[1])<1 ).count()


# # Keep only English reviews

# In[107]:

import langdetect as ld


# In[108]:

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


# In[109]:

#stampa le reviews in lingua differente dall'inglese
#rdd_reviews.filter(lambda x: not detect_Eng(x[1])).take(30)


# In[110]:

#rdd_reviews.filter(lambda x: x[1] is None).count()


# In[ ]:

log.toLog('starting detect_Eng')


# In[42]:

#remove EMPTY reviews, keep only english reviews
#creiamo un nuovo data frame(df_revs) con colonne id, Review

df_lang = (rdd_reviews.filter(lambda x: detect_Eng(x[1]))
           #.filter(lambda x: x[1] is not None)
           .toDF(['id','Review'])
          )
df_lang.printSchema()


# In[17]:

df_lang.take(3)


# In[43]:

N_cat_lang = df_lang.count()
N_cat_lang


# In[44]:

# Not english reviews removed:
N_cat - N_cat_lang


# In[50]:

#creiamo un nuovo dataframe con le review modificate, eliminando quelle "vecchie" e senza contare 
df_cleaned_lang = df_raw_id.join(df_lang, 'id', 'inner').drop("Positive_Review", "Negative_Review")
df_cleaned_lang.printSchema()


# In[51]:

N_cleaned_lang = df_cleaned_lang.count()
log.toLog('counted not english rev, removed:' + str(N_raw-N_cleaned_lang))
N_cleaned_lang


# # Hotel nationality

# In[52]:

import reverse_geocode
log.toLog('starting country extraction')


# In[57]:

# example
coord =[ (43.6753176,10.5408628) ]
reverse_geocode.search(coord)


# In[ ]:

# check na value for lat/lng
#(df_cleaned_lang
# .sample(False, 0.01)
# .select('lat','lng').rdd.filter(lambda x: x['lng']== 'NA').count()
#)


# In[ ]:




# In[58]:

df_coord = ( df_raw_id.select('id', 'lat', 'lng').rdd
            #.sample(False, 0.01)
            .filter(lambda x: x['lat']!='NA' and x['lng']!='NA')
            .map(lambda x: (x['id'], [ [float(x['lat']), float(x['lng'])] ] )) 
            .map(lambda x: (x[0], reverse_geocode.search(x[1])))
            .map(lambda x: (x[0] , x[1][0]['city'], x[1][0]['country'], x[1][0]['country_code']))
           ).toDF(['id','h_city','h_country','h_country_code'])


# In[60]:

print(df_coord.head(5))


# In[63]:

hotel_countries = df_coord.select('h_country').distinct().rdd.map(lambda x: x['h_country']).collect()
hotel_countries


# In[64]:

# estrarre la nazionalità dall'indirizzo
def extractCountry(row):
    for country in hotel_countries:
        if country.lower() in row['Hotel_Address'].lower():
            return (row['id'], country )
    return 'EMPTY'
 


# In[74]:

rdd_hotel_countries = (df_raw_id
                       #.sample(False, 0.005)
                       .select('id', 'Hotel_Address').rdd.map(extractCountry)
                      )


# In[70]:

# check if all reviews have an associated hotel_country 
# rdd_hotel_countries.filter(lambda x: x[1] == 'EMPTY').count()


# In[71]:

# rdd_hotel_countries.take(1)


# In[72]:

df_hotel_countries = rdd_hotel_countries.toDF(['id', 'Hotel_Country'])


# In[73]:

print (df_hotel_countries.show())


# ## Join and save cleaned df to hdfs

# In[ ]:

log.toLog( 'started final join and write to hdfs')


# In[ ]:

df_cleaned = df_cleaned_lang.join(df_hotel_countries, 'id', 'inner')


# In[ ]:

df_cleaned.write.csv("hdfs://masterbig-1.itc.unipi.it:54310/user/student18/df_cleaned.csv", header = True)


# In[ ]:

log.toLog( 'etl notebook finished')
log.close()


# In[ ]:



