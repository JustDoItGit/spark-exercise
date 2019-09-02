from pyspark import SparkContext, SparkConf
from operator import add


conf = SparkConf().setAppName("exercise").setMaster("local")
sc = SparkContext(conf=conf)

log = sc.textFile("demo.txt").flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(add)
print(log.collect())

# 更复杂的例子，排序输出 出现频率最高的 10 个单词
sortWords = log.sortBy(lambda pair: pair[1], False).take(10)
print(sortWords)
