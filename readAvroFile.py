
import findspark
findspark.init()

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("readAvroFile").config('spark.jars', "C:\\Users\\Dell\\Downloads\\files\\spark-avro_2.12-3.0.3.jar").getOrCreate()
df = spark.read.format("avro").load("C:/Users/Dell/Downloads/files/userdata1.avro")
df.printSchema()
df.show(5)
