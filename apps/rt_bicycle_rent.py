from common.base_stream_app import BaseStreamApp
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

class RtBycicleRent(BaseStreamApp):
    def __init__(self, app_name):
        super().__init__(app_name)