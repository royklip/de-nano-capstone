import boto3
import os
import configparser

config = configparser.ConfigParser()
config.read('capstone.cfg')
    
KEY = config.get('AWS','AWS_ACCESS_KEY_ID')
SECRET = config.get('AWS','AWS_SECRET_ACCESS_KEY')
REGION = config.get('AWS','REGION')
BUCKET = config.get('S3','BUCKET')
BUCKET_FOLDER = config.get('S3','PATH_RAW')

# Create an S3 client
s3 = boto3.client('s3',
    region_name=REGION,
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)

# Upload the data to S3
file_paths = [
    '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat',
    '../../data2/GlobalLandTemperaturesByCity.csv',
    'data/airport-codes_csv.csv',
    'data/immigration_data_sample.csv',
    'data/us-cities-demographics.csv'
]

for file_path in file_paths:
    with open(file_path, 'rb') as f:
        file_path_split = os.path.split(file_path)
        s3.upload_fileobj(f, BUCKET, BUCKET_FOLDER + '/' + file_path_split[1])