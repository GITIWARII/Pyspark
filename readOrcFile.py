import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("readOrcFile").getOrCreate()

df = spark.read.orc("C:\\Users\\Dell\\Downloads\\files\\userdata1.orc")
df.printSchema()
df.show()