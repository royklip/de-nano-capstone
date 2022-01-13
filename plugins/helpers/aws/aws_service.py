import boto3


class AwsService:
    def __init__(self, credentials, region):
        """ Initialize the AWS service class. """        
        # Get AWS credentials
        self.KEY = credentials.access_key
        self.SECRET = credentials.secret_key

        # Create boto session
        self.session = boto3.Session(
            aws_access_key_id=self.KEY,
            aws_secret_access_key=self.SECRET,
            region_name=region
        )