from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from common.logger import Logger
import argparse

class BaseStreamApp():
    def __init__(self, app_name):
        self.app_name = app_name
        self.kafka_offset_dir = f'/home/spark/kafka_offsets/{app_name}'  # Kafka Offset Checkpoint 경로 지정
        self.dataframe_chkpnt_dir = f'/home/spark/dataframe_checkpoints/{self.app_name}'  # DataFrame Checkpoint 경로 지정

        # Logger 생성
        self.logger = Logger(app_name)

        # Spark Parameter 설정
        self.SPARK_EXECUTOR_INSTANCES = '3'
        self.SPARK_EXECUTOR_CORES = '2'
        self.SPARK_EXECUTOR_MEMORY = '2g'
        self.SPARK_SQL_SHUFFLE_PARTITIONS = '6'

        self.log_mode = ''
        self.set_argparse()

    def set_argparse(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-l', '--log_mode', default='info', help = 'info or debug')
        args = parser.parse_args()
        self.log_mode = args.log_mode

    def get_session_builder(self):
        return SparkSession \
            .builder \
            .appName(self.app_name) \
            .config('spark.executor.instances', self.SPARK_EXECUTOR_INSTANCES) \
            .config('spark.executor.cores', self.SPARK_EXECUTOR_CORES) \
            .config('spark.executor.memory', self.SPARK_EXECUTOR_MEMORY) \
            .config('spark.sql.shuffle.partitions', self.SPARK_SQL_SHUFFLE_PARTITIONS)

    def for_each_batch(self, df: DataFrame, epoch_id, spark: SparkSession):
        '''
        _for_each_batch 함수 실행 전, 후 공통 로직 삽입 용도
        '''
        self.logger.write_log('info', f'============================= epoch_id: {epoch_id} start =============================', epoch_id)
        self._for_each_batch(df, epoch_id, spark)
        self.logger.write_log('info', f'============================= epoch_id: {epoch_id} end =============================', epoch_id)

    def _for_each_batch(self, df: DataFrame, epoch_id, spark: SparkSession):
        '''
        Spark Streaming Application 의 for each batch
        '''
        pass