from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import time

spark = SparkSession \
    .builder \
    .appName('dataframe_cache') \
    .getOrCreate()

company_emp_path = 'hdfs:///home/spark/sample/linkedin_jobs/companies/employee_counts.csv'
company_emp_schema = 'company_id LONG,employee_count LONG,follower_count LONG,time_recorded TIMESTAMP'
company_ind_path = 'hdfs:///home/spark/sample/linkedin_jobs/companies/company_industries.csv'
company_ind_schema = 'company_id LONG, industry STRING'

# employee_counts Load
company_emp_df = spark.read \
    .option('header', 'true') \
    .option('multiline', 'true') \
    .schema(company_emp_schema) \
    .csv(company_emp_path)
company_emp_df.persist()
emp_cnt = company_emp_df.count()
print(f'company_emp_df count : {emp_cnt}')

# employee_counts 중복 제거
company_emp_drop_df = company_emp_df.dropDuplicates(['company_id'])
emp_drop_cnt = company_emp_drop_df.count()
print(f'company_emp_df dropDuplicates count : {emp_drop_cnt}')

# company_industries Load
company_ind_df = spark.read \
    .option('header', 'true') \
    .option('multiline', 'true') \
    .schema(company_ind_schema) \
    .csv(company_ind_path)
company_ind_df.persist()
ind_cnt = company_ind_df.count()
print(f'company_ind_df count : {ind_cnt}')

company_it_df = company_ind_df.filter(col('industry') == 'IT Services and IT Consulting')

company_emp_cnt_df = company_emp_drop_df.join(
    company_it_df,
    on='company_id',
    how='inner'
).select('company_id', 'employee_count') \
    .sort('employee_count', acsending=False)

company_emp_cnt_df.show()
time.sleep(300)


