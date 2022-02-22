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
file_cities = 'us-cities-demographics.csv'

txt_load_options = {
    'header': None, 
    'sep': '=', 
    'names': ['code', 'name'], 
    'quotechar': '\''
}


with DAG('clean_csv_data',
    start_date=datetime.utcnow(),
    description='Clean the CSV data on S3'
) as dag:

    clean_airport_data_task = CleanDataOperator(
        task_id='clean_airport_data',
        aws_credentials_id=aws_credentials,
        region=REGION,
        bucket=BUCKET,
        input_file=f'{PATH_RAW}/{file_airport}',
        output_file=f'{PATH_CLEAN}/{file_airport}',
        cleaning_function=DataCleaner.clean_airport_data,
        load_options={
            'keep_default_na': False,
            'na_values': ['', '-1.#IND', '1.#QNAN', '1.#IND', '-1.#QNAN', '#N/A','N/A', '#NA', 'NULL', 'NaN', '-NaN', 'nan', '-nan']
        }
    )

    clean_cities_data_task = CleanDataOperator(
        task_id='clean_cities_data',
        aws_credentials_id=aws_credentials,
        region=REGION,
        bucket=BUCKET,
        input_file=f'{PATH_RAW}/{file_cities}',
        output_file=f'{PATH_CLEAN}/{file_cities}',
        cleaning_function=DataCleaner.clean_cities_data,
        load_options={'delimiter': ';'}
    )

    clean_i94prtl_data_task = CleanDataOperator(
        task_id='clean_i94prtl_data',
        aws_credentials_id=aws_credentials,
        region=REGION,
        bucket=BUCKET,
        input_file=f'{PATH_RAW}/i94prtl.txt',
        output_file=f'{PATH_CLEAN}/i94prtl.csv',
        cleaning_function=DataCleaner.clean_i94prtl_data,
        load_options=txt_load_options
    )

    for txt_file in ['i94addrl', 'i94cntyl', 'i94model', 'i94visa']:
        clean_txt_data_task = CleanDataOperator(
            task_id=f'clean_{txt_file}_data',
            aws_credentials_id=aws_credentials,
            region=REGION,
            bucket=BUCKET,
            input_file=f'{PATH_RAW}/{txt_file}.txt',
            output_file=f'{PATH_CLEAN}/{txt_file}.csv',
            load_options=txt_load_options
        )
