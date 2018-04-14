from pyspark import SparkContext, SparkConf
from operator import add

conf = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf=conf)

input = sc.textFile("ch2/README.md")
count = input.flatMap(lambda line: line.split(' '))\
    .map(lambda word: (word, 1)).reduceByKey(add)

output = count.collect()
output.sort(key=lambda tup: tup[1], reverse=True)
for (word, count) in output:
    print(str(word)+" "+str(count))