import pandas as pd
import io

from helpers.aws.aws_service import AwsService


class S3(AwsService):
    def __init__(self, credentials, region: str, bucket: str):
        """ Initialize the s3 class. """
        super(S3, self).__init__(credentials, region)

        # Create an S3 client
        self.s3 = self.session.client('s3')

        self.bucket = bucket


    def load_data(self, path: str, file_name: str):
        """ Load data from S3 """
        return self.s3.get_object(Bucket=self.bucket, Key=f'{path}/{file_name}')['Body']

    
    def store_data(self, path: str, file_name: str, df: pd.DataFrame) -> None:
        """ Store data on S3 """
        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)
            response = self.s3.put_object(Bucket=self.bucket, Key=f'{path}/{file_name}', Body=csv_buffer.getvalue())

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")
