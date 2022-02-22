from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import DateType, IntegerType

import os
import datetime

def main():
    BUCKET = os.environ['BUCKET']
    PATH_RAW = os.environ['PATH_RAW']
    PATH_CLEAN = os.environ['PATH_CLEAN']

    spark = SparkSession.builder \
        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:3.0.0-s_2.12,org.apache.hadoop:hadoop-aws:3.3.1") \
        .enableHiveSupport() \
        .getOrCreate()

    df_immigration = spark.read.format('com.github.saurfang.sas.spark').load(f's3a://{BUCKET}/{PATH_RAW}/i94_apr16_sub.sas7bdat')

    # Create UDF for converting SAS numeric date to datetime
    sas_start_date = datetime.datetime(1960, 1, 1)
    sas_to_dt = udf(lambda x: (sas_start_date + datetime.timedelta(days=x)) if x else None, DateType())

    # Convert SAS date numeric fields to datetime
    df_immigration = df_immigration.withColumn('arrdate', sas_to_dt('arrdate'))
    df_immigration = df_immigration.withColumn('depdate', sas_to_dt('depdate'))

    # Cast double type columns to integer type
    for column in ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94mode', 'i94bir', 'i94visa', 'count', 'biryear', 'admnum']:
        df_immigration = df_immigration.withColumn(column, col(column).cast(IntegerType()))

    # Save data to parquet files on S3
    df_immigration.write.mode('overwrite').parquet(f's3a://{BUCKET}/{PATH_CLEAN}/immigration.parquet')


if __name__ == '__main__':
    main()