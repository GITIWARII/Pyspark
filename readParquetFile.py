import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("readJsonFile").getOrCreate()

df = spark.read.parquet("C:\\Users\\Dell\\Downloads\\parquet\\userdata1.parquet")
df.printSchema()
df.show(5)
