import configparser
from datetime import datetime
from airflow import DAG

from operators import CleanDataOperator
from helpers import DataCleaner

config = configparser.ConfigParser()
config.read('airflow.cfg')

# AWS setup
aws_credentials = 'aws_default'
REGION = config.get('AWS','REGION')

# S3 setup
BUCKET = config.get('S3', 'BUCKET')
PATH_RAW = config.get('S3', 'PATH_RAW')
PATH_CLEAN = config.get('S3', 'PATH_CLEAN')

# Data files
file_airport = 'airport-codes_csv.csv'
file_temp = 'GlobalLandTemperaturesByCity.csv'
file_cities = 'us-cities-demographics.csv'

with DAG('cleaning_dag',
    start_date=datetime.utcnow(),
    description='Clean the CSV data on S3'
) as dag:

    clean_airport_data_task = CleanDataOperator(
        task_id='clean_airport_data',
        aws_credentials_id=aws_credentials,
        region=REGION,
        bucket=BUCKET,
        input_path=PATH_RAW,
        output_path=PATH_CLEAN,
        file=file_airport,
        cleaning_function=DataCleaner.clean_airport_data
    )

    clean_temperature_data_task = CleanDataOperator(
        task_id='clean_temperature_data',
        aws_credentials_id=aws_credentials,
        region=REGION,
        bucket=BUCKET,
        input_path=PATH_RAW,
        output_path=PATH_CLEAN,
        file=file_temp,
        cleaning_function=DataCleaner.clean_temperature_data
    )

    clean_cities_data_task = CleanDataOperator(
        task_id='clean_cities_data',
        aws_credentials_id=aws_credentials,
        region=REGION,
        bucket=BUCKET,
        input_path=PATH_RAW,
        output_path=PATH_CLEAN,
        file=file_cities,
        load_options={'delimiter': ';'}
    )
