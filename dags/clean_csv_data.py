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

def _get_txt_load_options(category: str):
    return {
        'header': None, 
        'sep': '=', 
        'names': [f'{category}_code', f'{category}_name'], 
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
        load_options=_get_txt_load_options('airport')
    )

    clean_i94addrl_data_task = CleanDataOperator(
        task_id=f'clean_i94addrl_data',
        aws_credentials_id=aws_credentials,
        region=REGION,
        bucket=BUCKET,
        input_file=f'{PATH_RAW}/i94addrl.txt',
        output_file=f'{PATH_CLEAN}/i94addrl.csv',
        load_options=_get_txt_load_options('state')
    )

    clean_i94cntyl_data_task = CleanDataOperator(
        task_id=f'clean_i94cntyl_data',
        aws_credentials_id=aws_credentials,
        region=REGION,
        bucket=BUCKET,
        input_file=f'{PATH_RAW}/i94cntyl.txt',
        output_file=f'{PATH_CLEAN}/i94cntyl.csv',
        load_options=_get_txt_load_options('country')
    )

    clean_i94model_data_task = CleanDataOperator(
        task_id=f'clean_i94model_data',
        aws_credentials_id=aws_credentials,
        region=REGION,
        bucket=BUCKET,
        input_file=f'{PATH_RAW}/i94model.txt',
        output_file=f'{PATH_CLEAN}/i94model.csv',
        load_options=_get_txt_load_options('mode')
    )

    clean_i94visa_data_task = CleanDataOperator(
        task_id=f'clean_i94visa_data',
        aws_credentials_id=aws_credentials,
        region=REGION,
        bucket=BUCKET,
        input_file=f'{PATH_RAW}/i94visa.txt',
        output_file=f'{PATH_CLEAN}/i94visa.csv',
        load_options=_get_txt_load_options('visa')
    )
