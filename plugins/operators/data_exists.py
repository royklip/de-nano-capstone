from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator

from typing import List


class DataExistsOperator(BaseOperator):

    def __init__(self, 
            redshift_conn_id: str, 
            tables: List[str],
            *args,
            **kwargs
        ):
        super().__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.tables=tables


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")

            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")

            self.log.info(f"Data count on table {table} check passed with {records[0][0]} records")
