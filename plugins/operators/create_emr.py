from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator

from helpers import Emr


class CreateEmrClusterOperator(BaseOperator):

    def __init__(self, 
            aws_credentials_id: str, 
            region: str,
            job_flow: dict,
            *args, 
            **kwargs
        ):
        super().__init__(*args, **kwargs)

        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.job_flow = job_flow


    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type='iam')
        credentials = aws_hook.get_credentials()

        emr = Emr(credentials, self.region)

        self.log.info(f"Create cluster")
        emr.create_cluster(self.job_flow)
