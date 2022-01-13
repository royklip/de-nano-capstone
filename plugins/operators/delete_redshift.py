from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator

from helpers import Redshift


class DeleteRedshiftClusterOperator(BaseOperator):

    def __init__(self, 
            aws_credentials_id: str, 
            region: str,
            iam_role_name: str,
            cluster_id: str,
            db_port: int,
            *args, 
            **kwargs
        ):
        super().__init__(*args, **kwargs)

        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.iam_role_name = iam_role_name
        self.cluster_id = cluster_id
        self.db_port = db_port


    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type='iam')
        credentials = aws_hook.get_credentials()

        redshift = Redshift(credentials, self.region)

        self.log.info(f"Delete access port to {self.cluster_id}")
        redshift.delete_access_port(self.cluster_id, self.db_port)

        self.log.info(f"Delete cluster {self.cluster_id}")
        redshift.delete_cluster(self.cluster_id)

        self.log.info(f"Delete IAM role {self.iam_role_name}")
        redshift.delete_iam_role(self.iam_role_name)
