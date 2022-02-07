from datetime import datetime
import configparser

from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow import DAG
from operators import CreateEmrConnectionOperator


config = configparser.ConfigParser()
config.read('airflow.cfg')

# AWS setup
aws_credentials = 'aws_default'
REGION = config.get('AWS','REGION')
BUCKET = config.get('S3', 'BUCKET')
PATH_RAW = config.get('S3', 'PATH_RAW')
PATH_CLEAN = config.get('S3', 'PATH_CLEAN')
PATH_SCRIPTS = config.get('S3', 'PATH_SCRIPTS')

JOB_FLOW_OVERRIDES = {
    'Name': 'RoyUdacityNanodegree',
    'ReleaseLabel': 'emr-6.5.0',
    'Applications': [{'Name': 'Spark'}, {'Name': 'Hadoop'}],
    'Configurations': [
        {
            'Classification': 'spark-env',
            'Properties': {},
            'Configurations': [
                {
                    'Classification': 'export',
                    'Properties': {
                        'PYSPARK_PYTHON': '/usr/bin/python3',
                        'BUCKET': BUCKET,
                        'PATH_RAW': PATH_RAW,
                        'PATH_CLEAN': PATH_CLEAN
                    }
                }
            ]
        },
    ],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Primary node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm1.medium',
                'InstanceCount': 1,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}

SPARK_STEPS = [
    {
        'Name': 'CleanImmigrationData',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'client',
                '--packages',
                'saurfang:spark-sas7bdat:3.0.0-s_2.12,org.apache.hadoop:hadoop-aws:3.3.1',
                f's3://{BUCKET}/{PATH_SCRIPTS}/clean_immigration_data.py',
            ],
        },
    }
]


def _scripts_to_s3(filename, key, bucket_name=BUCKET):
    s3 = S3Hook()
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=f'{PATH_SCRIPTS}/{key}')


with DAG('clean_sas_data',
    start_date=datetime.utcnow(),
    description='Test'
) as dag:

    script_to_s3 = PythonOperator(
        task_id='script_to_s3',
        python_callable=_scripts_to_s3,
        op_kwargs={'filename': './dags/scripts/clean_immigration_data.py', 'key': 'clean_immigration_data.py',},
    )

    create_emr_connection = CreateEmrConnectionOperator(
        task_id='create_emr_connection',
        conn_id='emr_default'
    )

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES
    )

    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=SPARK_STEPS
    )

    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
    )

    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_cluster', 
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}"
    )

    script_to_s3 >> create_emr_connection >> create_emr_cluster >> step_adder >> step_checker >> cluster_remover