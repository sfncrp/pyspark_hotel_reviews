from pprint import pprint
import os

###########################################################
from pyspark import SparkConf, SparkContext

APP_NAME = 'test'

conf = SparkConf().setAppName(APP_NAME).setMaster('local[*]')
sc = SparkContext(conf=conf)

###########################################################

raw = sc.textFile("hdfs://masterbig-1.itc.unipi.it:54310/user/student18/Hotel_Reviews.csv")

attributes = raw.take(1)
attributes_rdd = sc.parallelize(attributes)

raw_data = raw.subtract(attributes_rdd)

attributes = attributes[0].split(',')
pprint(attributes)

# dictionary to easily select attributes by name
attributes_dict = {att: i for i,att in enumerate(attributes)}

selection = ['Hotel_Name', 'Negative_Review', 'Positive_Review']

reviews = (
    raw_data.map(lambda x: x.split(','))
)

pprint(reviews.take(1)[0])
len(attributes)

import csv

def split(line):
    reader = csv.reader(StringIO(line))
    return reader.next()
    


###########################################################
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName('Python spark')
         .config('spark.some.config.option','some-value')
         .getOrCreate()
         )

###########################################################

df = spark.read.csv("hdfs://masterbig-1.itc.unipi.it:54310/user/student18/Hotel_Reviews.csv", header = True)

df.printSchema()
df.createGlobalTempView('raw')

df_reviews = spark.sql("SELECT Hotel_Address, Hotel_Name, Negative_Review, Positive_Review  FROM global_temp.raw")

df_reviews.show()

# counting Hotels
spark.sql('SELECT  COUNT( DISTINCT Hotel_Name) FROM global_temp.raw').show()

n_review_byhotel = spark.sql('SELECT Hotel_Name, COUNT(*) AS N FROM global_temp.raw GROUP BY Hotel_Name ORDER BY N ASC')


n_review_byhotel_rdd = n_review_byhotel.rdd


pprint(n_review_byhotel_rdd.takeOrdered(500, key = lambda x: x[1], ))

import numpy as np


n_review_byhotel_rdd.map()
n_review_byhotel_rdd.top(10)

review_distrib_byhotel = n_review_byhotel_rdd.collect()

import matplotlib.pyplot as plt


review_distrib = [h[1] for h in review_distrib_byhotel]


import matplotlib as mpl
mpl.use('Agg')

plt.close()
plt.bar(range(len(review_distrib)), review_distrib)
plt.axhline(np.mean(review_distrib), color = 'red')
plt.axhline(np.median(review_distrib))

plt.savefig('prova.png')

plt.close()
plt.hist(review_distrib, bins = 100)
plt.axvline(50, color = 'black')
plt.axvline(np.mean(review_distrib), color = 'red')
plt.axvline(np.median(review_distrib))

plt.savefig('hist.png')

list(review_distrib)

np.median(review_distrib)

# 
np.quantile(review_distrib, (0.10, 0.5,  0.90))
np.quantile(review_distrib, (0.25, 0.5,  0.75))

np.quantile(review_distrib, (0.05, 0.10,  0.25))

plt.close()
bp =  plt.boxplot(review_distribf)
plt.axhline(np.mean(review_distrib), color = 'red')
plt.axhline(50)

bp

plt.savefig('boxplot.png')



df.col('Hotel_Name')


###########################################################
# counting 


from pyspark.sql.functions import concat, lit

# unisco le review 
df_reviews = df.select(df.Hotel_Name, concat(df.Negative_Review, df.Positive_Review).alias('Review'))

N = df_reviews.count()


df_count = (df_reviews.select('Review').rdd
            .flatMap(lambda x: x[0].lower().split(' ') ) 
            .filter(lambda x: len(x)>2)
            .map(lambda word: (word, 1))
            .reduceByKey(lambda count1, count2: count1+count2)
            .map(lambda x: [ 100*(x[1]/N ) ,x[0]])
            )
W = df_count.count()

pprint(df_count.top(50))

''' 
Approccio alternativo alla feature extraction:

prima guardare le parole piu frequenti o la distribuzione tf-idf,
da qui individuare parole interessanti che possano costituire una 
categoria, raggruppandole e CONTANDOLE

dopodiche cercare le parole nel testo e valutare la vicinanza con aggettivi/avverbi che ne danno un sentiment

'''

counts = sorted(df_count.collect(), reverse =True, key = lambda x: x[0])


with open('hotel_word_counts.csv', 'w') as f:
    writer = csv.writer(f)
    for count in counts:
        writer.writerow(count)
    



rooms
staff
food
noise/noisy
bathroom
quiet
restaurant
breakfast
coffee
metro, trasporti?
pool
parking
walking
outside
centre



lines = (df_count.sortByKey(ascending=False)
         .map(lambda line: '{:.2f} , {}'.format(line[0],line[1]))
)

lines.saveAsTextFile('hdfs://masterbig-1.itc.unipi.it:54310/user/student18/hotel_word_counts.csv')
