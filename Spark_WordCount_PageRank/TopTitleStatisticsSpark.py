#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)
print(lines.collect())
lst = []
res = {}

flat_map = lines.flatMap(lambda line: line.split(" ")).map(lambda x: (x.split("\t")[0], int(x.split("\t")[1]))).values()

mean = int(flat_map.mean())
sum = int(flat_map.sum())
min = int(flat_map.min())
max = int(flat_map.max())
var = int(flat_map.variance())


#TODO
outputFile = open(sys.argv[2], "w")
outputFile.write('Mean\t%s\n' % mean)
outputFile.write('Sum\t%s\n' % sum)
outputFile.write('Min\t%s\n' % min)
outputFile.write('Max\t%s\n' % max)
outputFile.write('Var\t%s\n' % var)
'''
TODO write your output here
write results to output file. Format
outputFile.write('Mean\t%s\n' % ans1)
outputFile.write('Sum\t%s\n' % ans2)
outputFile.write('Min\t%s\n' % ans3)
outputFile.write('Max\t%s\n' % ans4)
outputFile.write('Var\t%s\n' % ans5)
'''

sc.stop()

