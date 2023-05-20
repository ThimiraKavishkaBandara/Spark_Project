#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 100)
flat_map = lines.map(lambda x: (x.split(":")[0], x.split(":")[1]))
values = flat_map.values().flatMap(lambda x: x.split(' ')).filter(lambda x : x.strip()).map(lambda x: (x,1)).reduceByKey(lambda x,y : x+y).sortBy(lambda x: x[1], False)
top10 = values.take(10)
sorted_top10 = sorted(top10)

output = open(sys.argv[2], "w")
for line in sorted_top10:
    word, count = line
    output.write(word + '\t' + str(count) + '\n')

#write results to output file. Foramt for each line: (key + \t + value +"\n")

sc.stop()

