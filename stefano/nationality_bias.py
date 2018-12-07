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

'''
Si osserva che i reviewers di differenti nazionalità danno voti 
differenti, ad esempio i cittadini statunitensi danno mediamente 
voti piu alti, mentre quelli dei paesi medioorientali quali Emirati Arabi, Oman, Pakistan Arabia saudita tendono a dare voti piu bassi. 
I voti degli alberghi potrebbero essere corretti tenendo in considerazione questi bias. 

Qual'è il valor medio da considerare? Gli USA pesano molto, poiché 
hanno molti turisti, quindi la media è spostata verso l'alto, 
ma un effetto simile si puo avere anche sulla mediana. 

Come voto medio, su cui poi calcolare lo scarto possiamo prendere 
il paese mediano, ovvero calcolare la mediana delle medie o mediane. 

Come si calcola la mediana in modo distribuito? 
Provare a fare una funzione con map/reduce/pyspark! 

'''

spark.sql("SELECT Reviewer_Nationality,  COUNT(*) AS N, \
AVG(Reviewer_Score) AS AVG_SCORE, \
STDDEV(Reviewer_Score) AS S \
FROM nationality \
GROUP BY Reviewer_Nationality  \
HAVING N> 500 \
ORDER BY AVG_SCORE DESC \
").show(200)


df_countries_avgs = spark.sql("SELECT Reviewer_Nationality,  COUNT(*) AS N, \
AVG(Reviewer_Score) AS AVG_SCORE, \
STDDEV(Reviewer_Score) AS S \
FROM nationality \
GROUP BY Reviewer_Nationality  \
HAVING N> 500 \
ORDER BY AVG_SCORE DESC \
")

df_countries_avgs.show()

pprint(rdd_countries_avgs.take(10))


''' come calcolare i quantili in pyspark ? ''' 


df_nationality.approxQuantile('Reviewer_Score', [0.25,0.5,0.75], 0.001)


df_nationality.groupby('Reviewer_Nationality').agg('Reviewer_Score', [0.25,0.5,0.75], 0.001)


import pyspark.sql.functions as func

def median(values_list):
    med = np.median(values_list)
    return float(med)
udf_median = func.udf(median)
udf_median = func.udf(median, spark.sql.types.FloatType())

group_df = df.groupby(['a', 'd'])
df_grouped = group_df.agg(udf_median(func.collect_list(col('c'))).alias('median'))
df_grouped.show()


from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank,col,count

w =  Window.partitionBy('Reviewer_Nationality').orderBy(df_nationality.Reviewer_Score )


df_median_countries = df_nationality.select('Reviewer_Nationality',
                      'Reviewer_Score',
                      percent_rank().over(w).alias("percentile"),
                      count(col('Reviewer_Nationality')).over(w).alias('N'))\
.orderBy('N', ascending = False)\
.where('percentile>0.48 and percentile<0.52')


spark.catalog.dropTempView("medians")

df_median_countries.createTempView('medians')

spark.sql("SELECT * FROM medians").show()

spark.sql("SELECT Reviewer_Nationality, \ 
AVG(percentile) AS PERC \
FROM medians\
GROUP BY Reviewer_Nationality\
").show()



###########################################################

# 
df_mean_score_by_Hotel_nationality =  spark.sql("SELECT Reviewer_Nationality, AVG(Reviewer_Score) AS AVG_SCORE                                                 FROM nationality                                                 GROUP BY Reviewer_Nationality                                                 ORDER BY AVG_SCORE DESC                                                ")
df_mean_score_by_Hotel_nationality.show()


df_mean_score_by_Hotel_Country =  spark.sql("SELECT Hotel_Country, AVG(Reviewer_Score) AS AVG_SCORE                                                 FROM nationality                                                 GROUP BY Hotel_Country                                                 ORDER BY AVG_SCORE DESC                                                ")
df_mean_score_by_Hotel_Country.show()




# #### Quant'è la differenza tra media aritmetica e media geografica sferica/ellissoide?
