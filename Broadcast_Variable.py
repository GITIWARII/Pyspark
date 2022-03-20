import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
spark = SparkSession.builder.master("local").appName("broadcast").getOrCreate()
sc = spark.sparkContext
def getCountry(sc):
    country = dict(
        {'AK': 'Alaska', 'IND': 'India', 'UK': 'United Kingdom', 'NY': 'New York', 'TX': 'Texas',
         'WA': 'Washington', 'FL': 'Florida'})

    broadCastVar = sc.broadcast(country)

    def getValue(cid):
        if cid in country:
            return broadCastVar.value[cid]

    testudf = udf(getValue)
    read_data = spark.read.format("csv").option('header','True').load("C:\\Users\\Dell\\Desktop\\countryCodes.csv")
    read_data.printSchema()
    final_df = read_data.withColumn('broad_country', testudf("country"))
    final_df.show()