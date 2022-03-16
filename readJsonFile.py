

import findspark
findspark.init()


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("readJsonFile").master("local").getOrCreate()

df = spark.read.json("C:\\Users\\Dell\\Downloads\\jsonfile\\sample2.json").cache()
df.printSchema()
df.show(5, truncate=False)