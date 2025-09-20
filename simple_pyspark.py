from pyspark.sql import SparkSession
import time

spark = SparkSession \
    .builder \
    .appName('simple_pyspark') \
    .getOrCreate()

schema = 'NAME STRING, AGE INT, MARRIAGE BOOLEAN'
df = spark.createDataFrame(data=[('KIM', 25), ('MIN', 22)], schema=schema)
df.show()

# sleep 5 minutes
time.sleep(300)