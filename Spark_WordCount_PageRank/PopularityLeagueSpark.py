#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)
rank = None

lst = []
lst2 = []
lst3 = []
lst4 = []
Ldict = {}
lines = sc.textFile(sys.argv[1], 100)
flat_map = lines.map(lambda x: (x.split(":")[0], x.split(":")[1]))
valCount = flat_map.values().flatMap(lambda x: x.split(' ')).filter(lambda x : x.strip()).map(lambda x: (x,1)).reduceByKey(lambda x,y : x+y).sortBy(lambda x: x[1], False)

leagueIds = sc.textFile(sys.argv[2], 100)
league = leagueIds.flatMap(lambda x: x.split(' ')).collect()
for l in league:
    rank = valCount.lookup(l)
    lst.append((rank))
  

for index, lt in enumerate(lst):
    for l in lt:
        lst2.append((index,l))

sorted_lst = sorted(lst2, key = lambda x: x[1])
#print(sorted_lst)       

for index, l in enumerate(league):
    Ldict[index]= l
#print(Ldict)

for k,v in sorted_lst:
    key = Ldict[k]
    lst3.append(key)
#print(lst3)

for index, l in enumerate(lst3):
    lst4.append((l, index))
#print(lst4)

sorted_lst2 = sorted(lst4, key = lambda x:x[0])
#print(sorted_lst2)



output = open(sys.argv[3], "w")
for k,v in sorted_lst2:
    output.write(k + "\t" + str(v) +"\n")

#TODO
#write results to output file. Foramt for each line: (key + \t + value +"\n")

sc.stop()

