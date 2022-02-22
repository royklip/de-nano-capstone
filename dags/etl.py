import configparser
from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from operators import (
    CreateRedshiftClusterOperator,
    CreateRedshiftConnectionOperator,
    LoadDataOperator,
    DeleteRedshiftClusterOperator
)

config = configparser.ConfigParser()
config.read('airflow.cfg')

# AWS setup
aws_credentials = 'aws_default'
REGION = config.get('AWS','REGION')

# IAM setup
IAM_ROLE_NAME = config.get('IAM_ROLE', 'NAME')

# Cluster setup
CLUSTER_IDENTIFIER = config.get('CLUSTER_SETUP', 'CLUSTER_IDENTIFIER')
CLUSTER_TYPE = config.get('CLUSTER_SETUP', 'CLUSTER_TYPE')
NUM_NODES = int(config.get('CLUSTER_SETUP', 'NUM_NODES'))
NODE_TYPE = config.get('CLUSTER_SETUP', 'NODE_TYPE')

# DB setup
DB_NAME = config.get('CLUSTER', 'DB_NAME')
DB_USER = config.get('CLUSTER', 'DB_USER')
DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
DB_PORT = int(config.get('CLUSTER', 'DB_PORT'))

# S3 setup
BUCKET = config.get('S3', 'BUCKET')
PATH_RAW = config.get('S3', 'PATH_RAW')
PATH_CLEAN = config.get('S3', 'PATH_CLEAN')

# Data files
file_airport = 'airport-codes_csv.csv'
file_temp = 'GlobalLandTemperaturesByCity.csv'
file_cities = 'us-cities-demographics.csv'

with DAG('etl',
    start_date=datetime.utcnow(),
    description='Create redshift cluster, create redshift tables, move data from S3 to redshift staging tables, \
        perform data wrangling and store them in the star schema tables'
) as dag:

    create_redshift_cluster_task = CreateRedshiftClusterOperator(
        task_id='create_redshift_cluster',
        aws_credentials_id=aws_credentials,
        region=REGION,
        iam_role_name=IAM_ROLE_NAME,
        cluster_id=CLUSTER_IDENTIFIER,
        cluster_type=CLUSTER_TYPE,
        node_type=NODE_TYPE,
        num_nodes=NUM_NODES,
        db_name=DB_NAME,
        db_user=DB_USER,
        db_password=DB_PASSWORD,
        db_port=DB_PORT
    )

    create_redshift_connection_task = CreateRedshiftConnectionOperator(
        task_id='create_redshift_connection',
        conn_id='redshift',
        aws_credentials_id=aws_credentials,
        region=REGION,
        cluster_id=CLUSTER_IDENTIFIER,
        schema=DB_NAME,
        login=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

    create_tables_task = PostgresOperator(
        task_id='create_tables',
        sql='sql/create_tables.sql',
        postgres_conn_id='redshift'
    )

    stage_airport_to_redshift_task = S3ToRedshiftOperator(
        task_id='stage_airport_to_redshift',
        redshift_conn_id='redshift',
        s3_bucket=BUCKET,
        s3_key=f'{PATH_CLEAN}/airport-codes_csv.csv',
        schema='PUBLIC',
        table='staging_airport',
        copy_options=['csv', 'IGNOREHEADER 1']
    )

    stage_temperature_to_redshift_task = S3ToRedshiftOperator(
        task_id='stage_temperature_to_redshift',
        redshift_conn_id='redshift',
        s3_bucket=BUCKET,
        s3_key=f'{PATH_CLEAN}/GlobalLandTemperaturesByCity.csv',
        schema='PUBLIC',
        table='staging_temperature',
        copy_options=['csv', 'IGNOREHEADER 1']
    )

    stage_cities_to_redshift_task = S3ToRedshiftOperator(
        task_id='stage_cities_to_redshift',
        redshift_conn_id='redshift',
        s3_bucket=BUCKET,
        s3_key=f'{PATH_CLEAN}/us-cities-demographics.csv',
        schema='PUBLIC',
        table='staging_cities',
        copy_options=['csv', 'IGNOREHEADER 1']
    )

    stage_immigration_to_redshift_task = S3ToRedshiftOperator(
        task_id='stage_immigration_to_redshift',
        redshift_conn_id='redshift',
        s3_bucket=BUCKET,
        s3_key=f'{PATH_CLEAN}/immigration.parquet',
        schema='PUBLIC',
        table='staging_immigration',
        copy_options=['FORMAT AS PARQUET']
    )

    i94addrl_to_redshift_task = S3ToRedshiftOperator(
        task_id='i94addrl_to_redshift',
        redshift_conn_id='redshift',
        s3_bucket=BUCKET,
        s3_key=f'{PATH_CLEAN}/i94addrl.csv',
        schema='PUBLIC',
        table='state_codes',
        copy_options=['csv', 'IGNOREHEADER 1']
    )

    i94cntyl_to_redshift_task = S3ToRedshiftOperator(
        task_id='i94cntyl_to_redshift',
        redshift_conn_id='redshift',
        s3_bucket=BUCKET,
        s3_key=f'{PATH_CLEAN}/i94cntyl.csv',
        schema='PUBLIC',
        table='country_codes',
        copy_options=['csv', 'IGNOREHEADER 1']
    )

    i94model_to_redshift_task = S3ToRedshiftOperator(
        task_id='i94model_to_redshift',
        redshift_conn_id='redshift',
        s3_bucket=BUCKET,
        s3_key=f'{PATH_CLEAN}/i94model.csv',
        schema='PUBLIC',
        table='mode_codes',
        copy_options=['csv', 'IGNOREHEADER 1']
    )

    i94prtl_to_redshift_task = S3ToRedshiftOperator(
        task_id='i94prtl_to_redshift',
        redshift_conn_id='redshift',
        s3_bucket=BUCKET,
        s3_key=f'{PATH_CLEAN}/i94prtl.csv',
        schema='PUBLIC',
        table='airport_codes',
        copy_options=['csv', 'IGNOREHEADER 1']
    )

    i94visa_to_redshift_task = S3ToRedshiftOperator(
        task_id='i94visa_to_redshift',
        redshift_conn_id='redshift',
        s3_bucket=BUCKET,
        s3_key=f'{PATH_CLEAN}/i94visa.csv',
        schema='PUBLIC',
        table='visa_codes',
        copy_options=['csv', 'IGNOREHEADER 1']
    )

    load_data_task = PostgresOperator(
        task_id='load_data',
        sql='sql/load_data.sql',
        postgres_conn_id='redshift'
    )

    # load_user_dimension_table = LoadDataOperator(
    #     task_id='Load_user_dim_table',
    #     table='users',
    #     sql_select=SqlQueries.user_table_insert,
    #     redshift_conn_id='redshift'
    # )

    delete_redshift_cluster_task = DeleteRedshiftClusterOperator(
        task_id='delete_redshift_cluster',
        aws_credentials_id=aws_credentials,
        region=REGION,
        iam_role_name=IAM_ROLE_NAME,
        cluster_id=CLUSTER_IDENTIFIER,
        db_port=DB_PORT
    )

    create_redshift_cluster_task >> create_redshift_connection_task >> create_tables_task
    create_tables_task >> [
        stage_airport_to_redshift_task, 
        stage_temperature_to_redshift_task, 
        stage_cities_to_redshift_task, 
        stage_immigration_to_redshift_task,
        i94addrl_to_redshift_task,
        i94cntyl_to_redshift_task,
        i94model_to_redshift_task,
        i94prtl_to_redshift_task,
        i94visa_to_redshift_task
    ] >> load_data_task
    load_data_task >> delete_redshift_cluster_task