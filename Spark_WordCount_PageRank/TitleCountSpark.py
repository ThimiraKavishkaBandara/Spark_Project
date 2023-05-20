#!/usr/bin/env python3

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext
import re


stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

with open(stopWordsPath) as f:
    	#TODO
    stopWords = f.read()
    stopWords = stopWords.split()


with open(delimitersPath) as f:
        #TODO
    delimiters = f.read()
    delimiters = delimiters.split()
    regex = ' |\t|\,|\;|\.|\?|\!|\-|\:|\@|\[|\]|\(|\)|\{|\}|\_|\*|\/|\s|\|'



conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)
# flat_text = lines.flatMap(lambda x: x.split(i for i in delimiters)).map(lambda x: (x,1)).reduceByKey(lambda x,y : x+y)

flat_text = lines.flatMap(lambda x: re.split(regex, x.lower())).filter(lambda x : x.strip()).filter(lambda x: x not in stopWords).map(lambda x: (x,1)).reduceByKey(lambda x,y : x+y).sortBy(lambda x: x[1], False)
top10 = flat_text.take(10)
sorted_top10 = sorted(top10, key = lambda x : [x])
outputFile = open(sys.argv[4],"w")
#TODO

for line in sorted_top10:
    word, count = line
    print(word, count)
    outputFile.write(word + '\t' + str(count) + '\n')


#write results to output file. Foramt for each line: (line +"\n")

sc.stop()


#    # print(type(line))
#     line = str(line)
#     # line = line.strip()
#     word, count = line.split(",",1)
#     print(word, count)
#     outputFile.write(word + count +"\n")