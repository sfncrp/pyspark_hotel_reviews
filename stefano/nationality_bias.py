# coding: utf-8
from pprint import pprint

from pyspark.sql import SparkSession
spark = (SparkSession.builder
         .appName('ddam_project')
         .config('spark.some.config.option','some-value')
         .getOrCreate()
         )
###########################################################

df_cleaned = spark.read.csv("hdfs://masterbig-1.itc.unipi.it:54310/user/student18/df_cleaned.csv", header = True, inferSchema = True)
df_cleaned.printSchema()

df_cleaned.take(3)



# ## Hotel City-Nationality Extraction

import reverse_geocode

# check na value for lat/lng
df_cleaned.select('lat','lng').rdd.filter(lambda x: x['lng']== 'NA').count()

coord =[ (43.6753176,10.5408628) ]
reverse_geocode.search(coord)

df_coord = ( df_cleaned.select('id', 'lat', 'lng').rdd
         .filter(lambda x: x['lat']!='NA' and x['lng']!='NA')
          .map(lambda x: (x['id'], [ [float(x['lat']), float(x['lng'])] ] )) 
          .map(lambda x: (x[0], reverse_geocode.search(x[1])))
          .map(lambda x: (x[0] , x[1][0]['city'], x[1][0]['country'], x[1][0]['country_code']))
         ).toDF(['id','city','country','country_code'])

pprint(df_coord.take(5))

df_coord.groupby('country').count().orderBy('count', ascending = False).show()


df_coord.groupby('country_code', 'country', 'city').count().orderBy('country','count',ascending = False).show(100)


hotel_countries = df_coord.select('country').distinct().rdd.map(lambda x: x['country']).collect()

hotel_countries

# estrarre la nazionalità dall'indirizzo
def extractCountry(row):
    for country in hotel_countries:
        if country.lower() in row['Hotel_Address'].lower():
            return (row['id'], country )
    return 'EMPTY'
        
            
rdd_hotel_countries = df_cleaned.select('id', 'Hotel_Address').rdd.map(extractCountry)


# check if all reviews have an associated hotel_country 
rdd_hotel_countries.filter(lambda x: x[1] == 'EMPTY').count()


rdd_hotel_countries.take(1)


df_hotel_countries = rdd_hotel_countries.toDF(['id', 'Hotel_Country'])


df_hotel_countries.head(10)
df_cleaned.select('Reviewer_Nationality').distinct().count()
df_cleaned.select('Reviewer_Nationality').distinct()

###########################################################

df_cleaned.createTempView('Reviewer_Nationality')

spark.sql("SELECT Reviewer_Nationality, COUNT(*) AS N                                                 FROM Reviewer_Nationality                                                 GROUP BY Reviewer_Nationality                                                 ORDER BY N DESC                                                ").show(100)

###########################################################

df_nationality_tmp = df_hotel_countries.join(df_cleaned.select('id','Reviewer_Nationality', 'Reviewer_Score'), ['id'] )  

df_nationality_tmp.explain()

# adding column with Abroad feature: 1 if reviewer was abroad, 0 otherwise

df_abroad_temp = df_nationality_tmp.rdd.map(lambda row: (row['id'], 0 if row['Reviewer_Nationality'] == row['Hotel_Country'] else 1 )).toDF(['id','Abroad'])

df_nationality = df_nationality_tmp.join(df_abroad_temp, ['id'])

pprint(df_nationality.take(3))

df_nationality.rdd.filter(lambda x: x['Abroad']== 0).count()
df_nationality.printSchema()

df_nationality.createTempView('nationality')


spark.sql("SELECT Reviewer_Nationality,  COUNT(*) AS N, \
AVG(Reviewer_Score) AS AVG_SCORE, \
STDDEV(Reviewer_Score) AS S \
FROM nationality \
GROUP BY Reviewer_Nationality  \
HAVING N> 500 \
ORDER BY AVG_SCORE DESC \
").show(200)


spark.sql("SELECT COUNT(*) AS N, AVG(Reviewer_Score) AS AVG_SCORE \
FROM nationality \
GROUP BY NULL \
ORDER BY AVG_SCORE DESC \
").show()


spark.sql("SELECT Hotel_Country, COUNT(*) AS N, AVG(Reviewer_Score) AS AVG_SCORE, \
STDDEV(Reviewer_Score) AS S \
FROM nationality \
GROUP BY Hotel_Country \
ORDER BY AVG_SCORE DESC \
").show()



spark.sql("SELECT Abroad, COUNT(*) AS N, AVG(Reviewer_Score) AS AVG_SCORE \
FROM nationality \
GROUP BY Abroad \
ORDER BY AVG_SCORE DESC \
").show()


###########################################################

# 
df_mean_score_by_Hotel_nationality =  spark.sql("SELECT Reviewer_Nationality, AVG(Reviewer_Score) AS AVG_SCORE                                                 FROM nationality                                                 GROUP BY Reviewer_Nationality                                                 ORDER BY AVG_SCORE DESC                                                ")
df_mean_score_by_Hotel_nationality.show()


df_mean_score_by_Hotel_Country =  spark.sql("SELECT Hotel_Country, AVG(Reviewer_Score) AS AVG_SCORE                                                 FROM nationality                                                 GROUP BY Hotel_Country                                                 ORDER BY AVG_SCORE DESC                                                ")
df_mean_score_by_Hotel_Country.show()




# #### Quant'è la differenza tra media aritmetica e media geografica sferica/ellissoide?
