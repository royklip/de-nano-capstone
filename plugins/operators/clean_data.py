from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator

from helpers import S3

import pandas as pd
from typing import Callable, Dict


class CleanDataOperator(BaseOperator):

    def __init__(self, 
            aws_credentials_id: str, 
            region: str,
            bucket: str,
            input_file: str,
            output_file: str,
            cleaning_function: Callable[[pd.DataFrame], pd.DataFrame] = None,
            load_options: Dict[str, str] = {},
            *args,
            **kwargs
        ):
        super().__init__(*args, **kwargs)

        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.bucket = bucket
        self.input_file = input_file
        self.output_file = output_file
        self.cleaning_function = cleaning_function
        self.load_options = load_options


    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type='iam')
        credentials = aws_hook.get_credentials()

        s3 = S3(credentials, self.region, self.bucket)

        self.log.info(f"Load {self.input_file} from S3")
        df = pd.read_csv(s3.load_data(self.input_file), **self.load_options)

        self.log.info("Clean the data")
        # If there is no cleaning function, simply copy the dataframe
        if self.cleaning_function == None:
            df_clean = df.copy()
        else:
            df_clean = self.cleaning_function(df)

        self.log.info(f"Store {self.output_file} on S3")
        s3.store_data(self.output_file, df_clean)
