import time
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName('standalone') \
    .getOrCreate()

schema = 'ID INT, COUNTRY STRING, HIT LONG'
df = spark.createDataFrame(data=[(1, 'Korea', 120), (2, 'USA', 80), (3, 'Japan', 40)], schema=schema)
df.show()
df.count()

# sleep 5 minutes
time.sleep(300)