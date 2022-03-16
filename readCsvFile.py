import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("readCsvFile").getOrCreate()
df = spark.read.options(header='True').csv("C:\\Users\\Dell\\Downloads\\business-financial-data-december-2021-quarter-csv")
df.printSchema()
df.show(5)