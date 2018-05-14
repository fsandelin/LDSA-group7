from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
import pyspark.sql.functions as f

import os

from collections import Counter

os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

spark = SparkSession.builder \
 .appName("Python Spark SQL basic example") \
 .config("spark.some.config.option", "some-value") \
 .getOrCreate()

sc = spark.sparkContext
textFile = sc.textFile("/home/goldenshiber/LDSA/Spark/sv-en/europarl-v7.sv-en.en")

textFile2 = sc.textFile("/home/goldenshiber/LDSA/Spark/sv-en/europarl-v7.sv-en.sv")

wordsData = textFile.map(lambda x: x.split(' '))

#print(wordsData.count())

#print(wordsData.take(4))

wordsData2 = textFile2.map(lambda x: x.split(' '))
#print(wordsData.take(4))

#print(wordsData2.count())
#print(wordsData2.take(4))

def Func(lines):
    lines = [[word.lower() for word in lines] ]
    return lines

rdd1 = wordsData.map(Func)
rdd2 = wordsData2.map(Func)

#print(rdd1.take(4))
#print(rdd2.take(4))

#remove the < cases

filteredRDD = rdd1.filter(lambda x: not x[0].startswith('<'))
filteredRDD2 = rdd2.filter(lambda x: not x[0].startswith('<'))

#print(filteredRDD.count())
#print(filteredRDD2.count())


#Record the line numbers associated with each line (hindft: ZipWithIndex())
rdd1=rdd1.zipWithIndex()
rdd2=rdd2.zipWithIndex()



#Swap the key and value - so that the line number is the key
rdd1inverted = rdd1.map(lambda x:(x[1],x[0]))
rdd2inverted = rdd2.map(lambda x:(x[1],x[0]))

#Join the RDDs
rdd_join=rdd1inverted.join(rdd2inverted)
print(rdd_join.take(1))
#print(rdd_join.first())

#Unequal sentences
rddFilter_join=rdd_join.filter(lambda x: len(x[1][0][0]) == len(x[1][1][0]))
print(rddFilter_join.take(1))

#Filter all the long words
filter = rddFilter_join.filter(lambda x: sum(sum(len(k) for k in i.split()) for i in x[1][0][0]) < 20)

#Remove the indexes
filter_flat = filter.flatMap(lambda x: x[1:])

#Filter numbers
numbers = ["0","1","2","3","4","5","6","7","8","9"]

def contains_word(s, w):
    lenW = len(w)
    lenS = len(s)
    i = 0
    statement = 0
    while i < lenW and statement == 0:
        j=0
        while j < lenS and statement == 0 :
            if w[i] == s[j]:
                statement = 1
            else:
                j = j+1
        i = i + 1
    return statement

filter_number = filter_flat.filter(lambda x: contains_word(x[1][0][0],numbers) == 0 )

#Combine the words into occurence and value
#filter_map = filter_number.map(lambda x: for k in.i.split() for i in  )

'''
Function to create a list from a string, with word tuples in mind.
'''
def mapByPair(s, w):
    lenW = len(w)
    i = 0
    l = []
    while i < lenW:
        l.append(tuple([s[i],w[i]]))
        i = i+1
    return l



filter_number_pairs = filter_number.map(lambda x: x.mapByPair(x[1][0][1],x[1][1][0]))
