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
print(wordsData.take(4))

#print(wordsData2.count())
#print(wordsData2.take(4))

def Func(lines):
    #lines = lines.lower()
    #lines = [[word.lower() for word in text.split()] for text in lines]
    lines = [[word.lower() for word in lines] ]
    #lines = lines.split()
    return lines

rdd1 = wordsData.map(Func)
rdd2 = wordsData2.map(Func)

print(rdd1.take(4))
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



