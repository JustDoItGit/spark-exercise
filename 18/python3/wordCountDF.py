from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
from operator import add


spark = SparkSession.builder.appName("exercise").getOrCreate()

lines = spark.read.text("demo.txt")
words = lines.select(explode(split(lines.value, " ")).alias("word"))

print(words.groupBy("word").count().take(10))

# 进一步对 word 排序并输出
print(words.groupBy("word").count().orderBy("count", ascending=False).take(10))
