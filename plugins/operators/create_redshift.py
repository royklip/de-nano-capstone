from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator

from helpers import Redshift


class CreateRedshiftClusterOperator(BaseOperator):

    def __init__(self, 
            aws_credentials_id: str, 
            region: str,
            iam_role_name: str,
            cluster_id: str,
            cluster_type: str,
            node_type: str,
            num_nodes: int,
            db_name: str,
            db_user: str,
            db_password: str,
            db_port: int,
            *args, 
            **kwargs
        ):
        super().__init__(*args, **kwargs)

        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.iam_role_name = iam_role_name
        self.cluster_id = cluster_id
        self.cluster_type = cluster_type
        self.node_type = node_type
        self.num_nodes = num_nodes
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_port = db_port


    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type='iam')
        credentials = aws_hook.get_credentials()

        redshift = Redshift(credentials, self.region)

        self.log.info(f"Create IAM role {self.iam_role_name}")
        redshift.create_iam_role(self.iam_role_name)

        self.log.info(f"Create cluster {self.cluster_id}")
        redshift.create_cluster(
            self.cluster_id,
            self.cluster_type,
            self.node_type,
            self.num_nodes,
            self.db_name,
            self.db_user,
            self.db_password,
            self.iam_role_name
        )

        self.log.info("Create access port to the cluster")
        redshift.create_access_port(self.cluster_id, self.db_port)
