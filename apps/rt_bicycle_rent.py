from common.base_stream_app import BaseStreamApp
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

class RtBicycleRent(BaseStreamApp):
    def __init__(self, app_name):
        super().__init__(app_name)
        self.last_dttm= ''

    def init_call(self, spark_session: SparkSession):
        '''
        Spark 프로그램 기동할 때마다 1회만 수행되는 함수
        '''
        self.last_stt_info_df = spark_session.createDataFrame([],
                                                      'stt_id           STRING',
                                                      'lst_prk_cnt      STRING',
                                                      )

        # 결과 저장용 테이블
        spark_session.sql(f'''
            CREATE TABLE IF NOT EXISTS bicycle_rent-info(
                stt_id           STRING,
                stt_nm           STRING,
                rent_cnt         INT,
                return_cnt       INT,
                lst_prk_cnt      INT,
                stt_lttd         STRING,
                crt_dttm         TIMESTAMP
            )
            LOCATION 's3a://datalake-spark-sink-hrkim/bicycle_rent-info'
            PARTITIONED BY (ymd INT, hh INT)
            STORED AS PARQUET
            '''
        )
        self.logger.write_log('info', 'Completed: CREATE TABLE IF NOT EXISTS bicycle_rent-info')

    def main(self):
        spark = self.get_session_builder().getOrCreate()

        # 체크포인트 경로 설정
        spark.sparkContext.setCheckpointDir(self.dataframe_chkpnt_dir)

        streaming_query = spark.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', 'kafka01:9092, kafka02:9092, kafka03:9092') \
                .option('subscribe', 'apis.seouldata.rt-bicycle') \
                .option('startingOffsets', 'latest') \
                .option('failOnDataLoss', 'false') \
                .load() \
                .selectExpr(
                    'CAST(key AS STRING) AS KEY',
                    'CAST(value AS STRING) AS VALUE'
                ) \
                .writeStream \
                .format('console') \
                .foreachBatch(lambda df, epoch_id: self.for_each_batch(df, epoch_id, spark)) \
                .option("checkpointLocation", self.kafka_offset_dir) \
                .start()

    def _for_each_batch(self, df: DataFrame, epoch_id, spark: SparkSession):
        '''
        오버라이딩
        '''
        df.persist()
        cnt = df.count()
        self.logger.write_log('info', f'streaming 인입 건수 : {cnt}')
        self.logger.write_log('info', f'df.show()')
        df.show(truncate=False)
        df.unpersist()


if __name__ == '__main__':
    rt_bicycle_rent = RtBicycleRent(app_name='rt_bicycle_rent')
    rt_bicycle_rent.main()