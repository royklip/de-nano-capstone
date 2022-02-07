from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator
from helpers import (
    Redshift,
    ConnectionCreator
)


class CreateRedshiftConnectionOperator(BaseOperator):

 
    def __init__(self,
            conn_id: str,
            aws_credentials_id: str,
            region: str,
            cluster_id: str,
            schema: str,
            login: str,
            password: str,
            port: int,
            *args, 
            **kwargs
        ):
        super().__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.cluster_id = cluster_id
        self.schema = schema
        self.login = login
        self.password = password
        self.port = port


    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type='iam')
        credentials = aws_hook.get_credentials()

        self.log.info("Get the Redshift DB host address")
        redshift = Redshift(credentials, self.region)
        cluster_props = redshift.get_cluster_props(self.cluster_id)
        db_host = cluster_props['Endpoint']['Address']
        self.log.info(f"Found the Redshift DB host address on {db_host}")

        self.log.info(f"Create connection {self.conn_id}")
        ConnectionCreator.create_connection(self.conn_id, kwargs={
            'host': db_host,
            'schema': self.schema,
            'login': self.login,
            'password': self.password,
            'port': self.port
        })