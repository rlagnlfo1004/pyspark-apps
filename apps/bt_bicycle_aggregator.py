from pyspark.sql.functions import col, sum, when, lit, udf, count, coalesce
from pyspark.sql.types import BooleanType, LongType
from common.base_stream_app import BaseSparkApp
from datetime import datetime
from holidayskr import is_holiday

def check_is_holiday_py(date_input_str: str):
    '''
    주어진 날짜(YYMMDD 형식)가 (주말 또는 공휴일)인지 확인
    UDF로 등록되어 Spark에서 사용될 파이썬 함수
    '''
    if date_input_str is None:
        return None
    try:
        date_obj = datetime.strptime(date_input_str, '%Y%m%d')
        date_str = date_obj.strftime('%Y-%m-%d')

        # 주말 확인 (월요일=0, 일요일=6)
        is_weekend_status = date_obj.weekday() >= 5
        # 공휴일 확인
        is_holiday_status = is_holiday(date_str)

        return is_weekend_status or is_holiday_status
    except Exception:
        return None

class BtBicycleAggregator(BaseSparkApp):
    def __init__(self, app_name):
        super().__init__(app_name)

    def main(self):
        '''
        ** 증분 집계 **
        S3(bicycle.bicycle_rent_info)의 최신 하루치 데이터만 읽어
        기존 통계 테이블(bicycle.station_hourly_stats)에 병합(Merge)하는 배치 job
        '''
        # sparkSession 객체 얻기
        spark = self.get_session_builder().getOrCreate()

        # 통계 결과 테이블 생성 및 덮어쓰기
        self.logger.write_log('info', 'Completed: CREATE TABLE IF NOT EXISTS bicycle.station_hourly_stats')
        spark.sql(f'''
                CREATE TABLE IF NOT EXISTS bicycle.station_hourly_stats (
                        stt_id                          STRING,
                        hh                              STRING,
                        day_type                        STRING,
                        sum_prk_cnt                     BIGINT,
                        total_data_cnt                  BIGINT
                )
                LOCATION 's3a://datalake-spark-sink-hrkim/bicycle/station_hourly_stats'
                STORED AS PARQUET
                ''')

        # 기존 누적 통계 로드
        last_stt_hourly_df = spark.read.table('bicycle.station_hourly_stats').persist()
        self.logger.write_log('info', 'Loaded old cumulative statistics.')

        # 처리 대상 날짜 탐색
        latest_ymd = spark.sql('SELECT MAX(ymd) FROM bicycle.bicycle_rent_info').first()[0]
        self.logger.write_log('info', f'Processing data for MAX(ymd) = {latest_ymd}')

        # latest_ymd 하루치 데이터 로드 및 처리
        is_holiday_udf = udf(check_is_holiday_py, BooleanType())
        daily_stt_info_df = spark.read.table('bicycle.bicycle_rent_info') \
                                    .where(col('ymd') == latest_ymd) \

        # 일일 통계 계산 (sum, count)
        daily_processed_df = daily_stt_info_df.withColumn(
                                'day_type',
                                when(is_holiday_udf(col('ymd')), lit('HOLIDAY_OR_WEEKEND'))
                                .otherwise(lit('WEEKDAY'))
                            ).groupBy('stt_id', 'hh', 'day_type') \
                             .agg(
                                    sum('lst_prk_cnt').alias('daily_sum'),
                                    count('lst_prk_cnt').alias('daily_cnt')
                             )

        self.logger.write_log('info', f'Calculated daily stats for {latest_ymd}.')


        # 기존 통계와 일일 통계 병합
        new_stt_hourly_df =  last_stt_hourly_df.alias('l').join(
            daily_processed_df.alias('d'),
            on=['stt_id', 'hh', 'day_type'],
            how='full'
        ).select(
            coalesce(col('l.stt_id'), col('d.stt_id')).alias('stt_id'),
            coalesce(col('l.hh'), col('d.hh')).alias('hh'),
            coalesce(col('l.day_type'), col('d.day_type')).alias('day_type'),
            (coalesce(col('l.sum_prk_cnt'), lit(0).cast(LongType())) +
                    coalesce(col('d.daily_sum'), lit(0).cast(LongType()))).alias('sum_prk_cnt'),
            (coalesce(col('l.total_data_cnt'), lit(0).cast(LongType())) +
                    coalesce(col('d.daily_cnt'), lit(0).cast(LongType()))).alias('total_data_cnt')
        )

        # 최종 통계를 테이블에 덮어쓰기
        new_stt_hourly_df.coalesce(3) \
            .write \
            .mode('overwrite') \
            .format('parquet') \
            .insertInto('bicycle.station_hourly_stats')

        self.logger.write_log('info', f'Completed: Incremental update (ymd={latest_ymd}). Job finished.')
        last_stt_hourly_df.unpersist()
        spark.stop()

if __name__ == '__main__':
    bt_bicycle_aggregator = BtBicycleAggregator(app_name='bt_bicycle_aggregator')
    bt_bicycle_aggregator.main()
