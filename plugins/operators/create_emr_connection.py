from airflow.models import BaseOperator
from helpers import ConnectionCreator


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
        ConnectionCreator.create_connection(self.conn_id)
