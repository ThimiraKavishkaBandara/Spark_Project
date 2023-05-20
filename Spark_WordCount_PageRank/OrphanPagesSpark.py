#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)
kset = set()
vset = set()
lst = []
olst = []
lines = sc.textFile(sys.argv[1], 100) 
flat_map = lines.map(lambda x: (x.split(":")[0], x.split(":")[1]))
keys = flat_map.keys()
values = flat_map.values().flatMap(lambda x: x.split(' ')).filter(lambda x : x.strip())

for k in keys.collect():
    #k = int(k)
    kset.add(k)

for v in values.collect():
    #v = int(v)
    vset.add(v)


intersect = kset.intersection(vset)
diff = kset - intersect
orphan = sorted(list(diff))

# for k in kset:
#     if k not in vset or k in vset:
#         k = int(k)
#         lst.append(k)
# sorted_lst = sorted(lst)
# print(sorted_lst)
# #TODO

output = open(sys.argv[2], "w")
for page in orphan:
    page = str(page)
    output.write(page + '\n')
#TODO
#write results to output file. Foramt for each line: (line + "\n")

sc.stop()

