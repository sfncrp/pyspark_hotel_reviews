{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "spark = (SparkSession.builder\n",
    "         .appName('ddam_project')\n",
    "         .config('spark.some.config.option','some-value')\n",
    "         .getOrCreate()\n",
    "         )"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- Hotel_Address: string (nullable = true)\n",
      " |-- Additional_Number_of_Scoring: integer (nullable = true)\n",
      " |-- Review_Date: string (nullable = true)\n",
      " |-- Average_Score: double (nullable = true)\n",
      " |-- Hotel_Name: string (nullable = true)\n",
      " |-- Reviewer_Nationality: string (nullable = true)\n",
      " |-- Negative_Review: string (nullable = true)\n",
      " |-- Review_Total_Negative_Word_Counts: integer (nullable = true)\n",
      " |-- Total_Number_of_Reviews: integer (nullable = true)\n",
      " |-- Positive_Review: string (nullable = true)\n",
      " |-- Review_Total_Positive_Word_Counts: integer (nullable = true)\n",
      " |-- Total_Number_of_Reviews_Reviewer_Has_Given: integer (nullable = true)\n",
      " |-- Reviewer_Score: double (nullable = true)\n",
      " |-- Tags: string (nullable = true)\n",
      " |-- days_since_review: string (nullable = true)\n",
      " |-- lat: string (nullable = true)\n",
      " |-- lng: string (nullable = true)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "# read file from hdfs and infer schema\n",
    "df_raw = spark.read.csv(\"hdfs://masterbig-1.itc.unipi.it:54310/user/student18/Hotel_Reviews.csv\", header = True, inferSchema = True)\n",
    "df_raw.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": [
    "from pyspark.sql.functions import monotonically_increasing_id"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- Hotel_Address: string (nullable = true)\n",
      " |-- Additional_Number_of_Scoring: integer (nullable = true)\n",
      " |-- Review_Date: string (nullable = true)\n",
      " |-- Average_Score: double (nullable = true)\n",
      " |-- Hotel_Name: string (nullable = true)\n",
      " |-- Reviewer_Nationality: string (nullable = true)\n",
      " |-- Negative_Review: string (nullable = true)\n",
      " |-- Review_Total_Negative_Word_Counts: integer (nullable = true)\n",
      " |-- Total_Number_of_Reviews: integer (nullable = true)\n",
      " |-- Positive_Review: string (nullable = true)\n",
      " |-- Review_Total_Positive_Word_Counts: integer (nullable = true)\n",
      " |-- Total_Number_of_Reviews_Reviewer_Has_Given: integer (nullable = true)\n",
      " |-- Reviewer_Score: double (nullable = true)\n",
      " |-- Tags: string (nullable = true)\n",
      " |-- days_since_review: string (nullable = true)\n",
      " |-- lat: string (nullable = true)\n",
      " |-- lng: string (nullable = true)\n",
      " |-- id: long (nullable = false)\n",
      "\n"
     ]
    },
    {
     "data": {
      "text/plain": [
       "[Row(Hotel_Address=' s Gravesandestraat 55 Oost 1092 AA Amsterdam Netherlands', Additional_Number_of_Scoring=194, Review_Date='8/3/2017', Average_Score=7.7, Hotel_Name='Hotel Arena', Reviewer_Nationality=' Russia ', Negative_Review=' I am so angry that i made this post available via all possible sites i use when planing my trips so no one will make the mistake of booking this place I made my booking via booking com We stayed for 6 nights in this hotel from 11 to 17 July Upon arrival we were placed in a small room on the 2nd floor of the hotel It turned out that this was not the room we booked I had specially reserved the 2 level duplex room so that we would have a big windows and high ceilings The room itself was ok if you don t mind the broken window that can not be closed hello rain and a mini fridge that contained some sort of a bio weapon at least i guessed so by the smell of it I intimately asked to change the room and after explaining 2 times that i booked a duplex btw it costs the same as a simple double but got way more volume due to the high ceiling was offered a room but only the next day SO i had to check out the next day before 11 o clock in order to get the room i waned to Not the best way to begin your holiday So we had to wait till 13 00 in order to check in my new room what a wonderful waist of my time The room 023 i got was just as i wanted to peaceful internal garden view big window We were tired from waiting the room so we placed our belongings and rushed to the city In the evening it turned out that there was a constant noise in the room i guess it was made by vibrating vent tubes or something it was constant and annoying as hell AND it did not stop even at 2 am making it hard to fall asleep for me and my wife I have an audio recording that i can not attach here but if you want i can send it via e mail The next day the technician came but was not able to determine the cause of the disturbing sound so i was offered to change the room once again the hotel was fully booked and they had only 1 room left the one that was smaller but seems newer ', Review_Total_Negative_Word_Counts=397, Total_Number_of_Reviews=1403, Positive_Review=' Only the park outside of the hotel was beautiful ', Review_Total_Positive_Word_Counts=11, Total_Number_of_Reviews_Reviewer_Has_Given=7, Reviewer_Score=2.9, Tags=\"[' Leisure trip ', ' Couple ', ' Duplex Double Room ', ' Stayed 6 nights ']\", days_since_review='0 days', lat='52.3605759', lng='4.9159683', id=0)]"
      ]
     },
     "execution_count": 4,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "df_raw_id = df_raw.withColumn('id', monotonically_increasing_id())\n",
    "df_raw_id.printSchema()\n",
    "df_raw_id.take(1)"
   ]
  },
  {
   "cell_type": "raw",
   "metadata": {},
   "source": []
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Reviews transformation"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": [
    "def catReviews(row):\n",
    "    if row[\"Negative_Review\"] == \"No Negative\" and row[\"Positive_Review\"] == \"No Positive\":\n",
    "        return \"EMPTY\"\n",
    "    else:\n",
    "        if row[\"Negative_Review\"] == \"No Negative\":\n",
    "            return (row[\"Positive_Review\"].lower())\n",
    "        elif row[\"Positive_Review\"] == \"No Positive\":\n",
    "            return (row[\"Negative_Review\"].lower() )\n",
    "        else:\n",
    "            return(row[\"Negative_Review\"].lower()  + \". \" + row[\"Positive_Review\"].lower())\n",
    "            #users_ratings.append(row[12])       \n",
    "        "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": [
    "def correction(row):\n",
    "    return (row.replace(\" don t \", \" don't \")\n",
    "            .replace(\" didn t \", \" didn't \")\n",
    "            .replace(\" haven t \", \" haven't \")\n",
    "            .replace(\" hadn t \", \" hadn't \")\n",
    "            .replace(\" isn t \", \" isn't \")\n",
    "            .replace(\" weren t \", \" weren't \")\n",
    "            .replace(\" wasn t \", \" wasn't \")\n",
    "            .replace(\" dont \", \" don't \")\n",
    "            .replace(\" didnt \", \" didn't \")\n",
    "            .replace(\" i \", \" I \")\n",
    "           )"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 53,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "PythonRDD[72] at RDD at PythonRDD.scala:48"
      ]
     },
     "execution_count": 53,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#counting rows with empty reviews\n",
    "#df_raw_id.rdd.map(lambda x: (x['id'], catReviews(x))\n",
    "#                  .filter(lambda x: x[1] == \"EMPTY\").count().take(1) )"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "metadata": {},
   "outputs": [],
   "source": [
    "rdd_reviews = (df_raw_id.rdd.map(lambda x: (x['id'], catReviews(x)))\n",
    "               .filter(lambda x: x[1] != \"EMPTY\")\n",
    "               .map(lambda x: (x[0], correction(x[1]))) \n",
    "              )"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": [
    "#rdd_reviews.take(3)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 33,
   "metadata": {
    "scrolled": true
   },
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Keep only English reviews"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": [
    "import langdetect as ld"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "CPU times: user 3 µs, sys: 1 µs, total: 4 µs\n",
      "Wall time: 9.3 µs\n"
     ]
    }
   ],
   "source": [
    "#detect english reviews\n",
    "\n",
    "\n",
    "#reviews_rdd.map(ld.detect).take(10)\n",
    "def detect_Eng(review):\n",
    "    if (len(review) < 100 ):\n",
    "        return True\n",
    "    try:\n",
    "        if ld.detect(review)== 'en':\n",
    "            return True\n",
    "        else:\n",
    "            return False\n",
    "    except:\n",
    "        return True"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 32,
   "metadata": {},
   "outputs": [],
   "source": [
    "#stampa le reviews in lingua differente dall'inglese\n",
    "#rdd_reviews.filter(lambda x: not detect_Eng(x[1])).take(30)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- id: long (nullable = true)\n",
      " |-- Review: string (nullable = true)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "#remove EMPTY reviews, keep only english reviews\n",
    "#creiamo un nuovo data frame(df_revs) con colonne id, Review\n",
    "\n",
    "df_revs = rdd_reviews.filter(lambda x: detect_Eng(x[1])).toDF(['id','Review'])\n",
    "df_revs.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 22,
   "metadata": {},
   "outputs": [],
   "source": [
    "#df_revs.take(3)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "metadata": {},
   "outputs": [],
   "source": [
    "#creiamo un nuovo dataframe con le review modificate, eliminando quelle \"vecchie\" e senza contare \n",
    "df_cleaned = df_raw_id.join(df_revs, ['id']).drop(\"Positive_Review\", \"Negative_Review\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[Row(id=26, Hotel_Address=' s Gravesandestraat 55 Oost 1092 AA Amsterdam Netherlands', Additional_Number_of_Scoring=194, Review_Date='5/25/2017', Average_Score=7.7, Hotel_Name='Hotel Arena', Reviewer_Nationality=' United Kingdom ', Review_Total_Negative_Word_Counts=51, Total_Number_of_Reviews=1403, Review_Total_Positive_Word_Counts=134, Total_Number_of_Reviews_Reviewer_Has_Given=2, Reviewer_Score=9.6, Tags=\"[' Leisure trip ', ' Group ', ' Duplex Double Room ', ' Stayed 2 nights ']\", days_since_review='70 days', lat='52.3605759', lng='4.9159683', Review=' nothing at all to do with the hotel of course but people tend to slam their bedroom doors as they leave so if you re thinking of having a little lay in just be prepared for a slam to awake you other than that nothing to fault at all .  the hotel itself is in a lovely location a 5min if that tram ride into the center train right outside the hotel easy access to everywhere the staff are super friendly and always on hand to help and advice wonderful bright comfortable and clean rooms beds are amazing felt as if we were sleeping on clouds bathroom was clean spacious and airy we had shampoo body wash there alongside hair dryer towels safe wardrobe hangers clothes holders desk chair room service hair dryer fridge kettle and cups teas and coffees it literally has everything you need in the room perfect hotel couldn t fault it at all the lovely smell from the restaurant downstairs is great tennis court outside the hotel little pond as well and a huge park to walk around '),\n",
       " Row(id=29, Hotel_Address=' s Gravesandestraat 55 Oost 1092 AA Amsterdam Netherlands', Additional_Number_of_Scoring=194, Review_Date='5/16/2017', Average_Score=7.7, Hotel_Name='Hotel Arena', Reviewer_Nationality=' Hungary ', Review_Total_Negative_Word_Counts=24, Total_Number_of_Reviews=1403, Review_Total_Positive_Word_Counts=49, Total_Number_of_Reviews_Reviewer_Has_Given=2, Reviewer_Score=9.2, Tags=\"[' Leisure trip ', ' Couple ', ' Duplex Double Room ', ' Stayed 3 nights ']\", days_since_review='79 days', lat='52.3605759', lng='4.9159683', Review=' there is an ongoing construction enlarging the hotel the hotel s restaurant is nice but overpriced the bathroom is not very practical .  the hotel is located in a beautiful old monastery building which has a special mood combined with all modern features there is a big park just opposite to the hotel public transportation and a nice bakery for breakfast are also close good wifi all around the hotel '),\n",
       " Row(id=474, Hotel_Address='1 15 Templeton Place Earl s Court Kensington and Chelsea London SW5 9NB United Kingdom', Additional_Number_of_Scoring=244, Review_Date='12/2/2016', Average_Score=8.5, Hotel_Name='K K Hotel George', Reviewer_Nationality=' United States of America ', Review_Total_Negative_Word_Counts=49, Total_Number_of_Reviews=1831, Review_Total_Positive_Word_Counts=5, Total_Number_of_Reviews_Reviewer_Has_Given=3, Reviewer_Score=6.3, Tags=\"[' Business trip ', ' Solo traveler ', ' Classic Double Room ', ' Stayed 3 nights ', ' Submitted from a mobile device ']\", days_since_review='244 day', lat='51.4918878', lng='-0.1949706', Review=' front desk night manager charged my bill in dollars without permission he tried to net an extra 40 on my bill with the horrible exchange rate when I caught it he told me I must have authorized it at check in which I had not completely unacceptable .  good english breakfast ')]"
      ]
     },
     "execution_count": 13,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "df_cleaned.take(3)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": [
    " df_cleaned.write.csv(\"hdfs://masterbig-1.itc.unipi.it:54310/user/student18/df_cleaned.csv\", header = True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.4.3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
