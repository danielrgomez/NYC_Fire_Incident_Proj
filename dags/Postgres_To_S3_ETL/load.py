import psycopg2
import csv
import boto3
import requests
from tenacity import retry, wait_exponential, stop_after_attempt


@retry(wait=wait_exponential(multiplier=2, min=2, max=16), stop=stop_after_attempt(5))
def upload_file_to_s3(data_name,s3_client):
    for name in data_name:
        s3_client.upload_file('./temp_csv_files/exported_' + f'{name}' + '.csv', 'nyc-fire-incidents-s3', 'exported_' + f'{name}' + '.csv')
        print('exported_' + f'{name}' + '.csv' + ' File uploaded successfully!')

def load_data_to_s3():
    ##This may be the preferred approach as opposed to the above
    sts_client = boto3.client('sts')

    # Assume the IAM role
    assumed_role = sts_client.assume_role(
        RoleArn="arn:aws:iam::564001313146:role/S3AccessRoleForNYCFireIncidentsProj",
        RoleSessionName="MyS3Session"
    )
    print("Assumed Role")

    # Extract temporary credentials
    credentials = assumed_role['Credentials']
    s3_client = boto3.client(
        's3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
    data_name = ['nyc_fire_incidents_data','nyc_traffic_data']
    try:
        upload_file_to_s3(data_name,s3_client)
        print("Successfully Uploaded Files to S3!")

    except requests.exceptions.RequestException as e:
        print(f"Failed to upload to S3")