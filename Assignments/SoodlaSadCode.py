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

wordsData2 = textFile2.map(lambda x: x.split(' '))


def Func(lines):
    lines = [[word.lower() for word in lines] ]
    return lines

rdd1 = wordsData.map(Func)
rdd2 = wordsData2.map(Func)

#remove the < cases

filteredRDD = rdd1.filter(lambda x: not x[0][0].startswith('<'))
filteredRDD2 = rdd2.filter(lambda x: not x[0][0].startswith('<'))


#Record the line numbers associated with each line (hindft: ZipWithIndex())
rdd1=rdd1.zipWithIndex()
rdd2=rdd2.zipWithIndex()



#Swap the key and value - so that the line number is the key
rdd1inverted = rdd1.map(lambda x:(x[1],x[0]))
rdd2inverted = rdd2.map(lambda x:(x[1],x[0]))

#Join the RDDs
rdd_join=rdd1inverted.join(rdd2inverted)
#print(rdd_join.take(1))


#Unequal sentences
rddFilter_join=rdd_join.filter(lambda x: len(x[1][0][0]) == len(x[1][1][0]))
#print(rddFilter_join.take(1))

#Filter all the long words
filter = rddFilter_join.filter(lambda x: sum(sum(len(k) for k in i.split()) for i in x[1][0][0]) < 20)

#Remove the indexes
filter_flat = filter.flatMap(lambda x: x[1:])

#Filter numbers
numbers = ["0","1","2","3","4","5","6","7","8","9","!",",","?","$","/","%","-","_",";","#","-",'\\']
numbers2 = ["0","1","2","3","4","5","6","7","8","9","!",",","?","$","/","%","-","_",";","#",".",""]

'''
A function to check if words contains stopwords or numbers, or are in fact numbers or stop words.
'''

def contains_word(s, w, w2):
    lenW = len(w)
    lenW2 =len(w2)
    lenS = len(s)
    i = 0
    statement = 0
    while i < lenS and statement == 0:
        j = 0
        k = 0
        while j < lenW and statement == 0 :
            if s[i].find(w[j])!= -1: # found
                statement = 1
            else:j = j+1
        while k < lenW2:
            if s[i] == w2[k] and statement == 0:
                    statement = 1
            else: k = k+1
        i = i + 1
    return statement

filter_number = filter_flat.filter(lambda x: contains_word(x[0][0],numbers, numbers2) == 0 )
filter_number = filter_number.filter(lambda x: contains_word(x[1][0],numbers, numbers2) == 0 )

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
        l.append(((s[i], w[i]),1))
        i = i+1
    return l

filter_number_pairs = filter_number.flatMap(lambda x: mapByPair(x[0][0],x[1][0]))

RDDFinal = filter_number_pairs.reduceByKey(lambda x,y: x + y)
RDDFinal2=RDDFinal.sortBy(lambda x: x[1],ascending=False)
Translation =RDDFinal2.collect()
print(RDDFinal2.take(20))



