import findspark
findspark.init()

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("WORDCOUNT").setMaster("local[*]")
sc = SparkContext(conf=conf)


words = sc.textFile("C:\\Users\\Dell\\Documents\\file.txt", 4).flatMap(lambda line: line.split(" "))

a = words.map(lambda x: (x, 1))

b = a.reduceByKey(lambda a ,b: a + b)

print("count:", b.collect())
