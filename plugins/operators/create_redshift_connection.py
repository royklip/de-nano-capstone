from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow import settings
from airflow.models import (
    BaseOperator,
    Connection
)
from helpers import Redshift


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

        session = settings.Session()
        conn_id = self.conn_id

        self.log.info(f"Check if connection {conn_id} already exists")
        conn_name = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        if str(conn_name) == str(conn_id):
            self.log.info(f"Connection {conn_id} already exists")
            # TODO edit connection
        else:
            self.log.info(f"Connection {conn_id} does not exists, create it")
            conn = Connection(
                conn_id=conn_id,
                conn_type='postgres',
                host=db_host,
                schema=self.schema,
                login=self.login,
                password=self.password,
                port=self.port
            )

            self.log.info(f"Add the connection object to the session")
            session.add(conn)
            session.commit()

        session.close()

        "capstonecluster.c51djjlqyw9s.eu-west-1.redshift.amazonaws.com:5439/capstone"