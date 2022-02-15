from airflow.models import BaseOperator
from airflow.models import Connection
from airflow.utils.db import merge_conn


class CreateEmrConnectionOperator(BaseOperator):

 
    def __init__(self,
            conn_id: str,
            *args, 
            **kwargs
        ):
        super().__init__(*args, **kwargs)

        self.conn_id = conn_id


    def execute(self, context):
        self.log.info(f"Create connection {self.conn_id}")
        merge_conn(Connection(
                conn_id=self.conn_id,
                conn_type='emr'
            ))
