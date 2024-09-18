import boto3
from dotenv import load_dotenv
from io import StringIO
import os
import pandas as pd
import requests

API_URL = "https://educationdata.urban.org/api/v1/schools/ccd/enrollment/{year}/{grade}/"
GRADE = 'grade-pk'

load_dotenv()
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
BUCKET_NAME = os.environ['BUCKET_NAME']

S3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

years = [
            2020,
            2021
        ]

def pull_and_upload_data(year):
    print(f'Starting to pull data for {year}.')
    fetch_url = API_URL.format(year=year, grade=GRADE)

    try:
        response = requests.get(fetch_url).json()
    except requests.exceptions.RequestException as err:
        raise SystemExit(err)
    else:
        data = response['results']
        record_count = response['count']

    while response['next']:
        try:
            response = requests.get(response['next']).json()
        except requests.exceptions.RequestException as err:
            raise SystemExit(err)
        else:
            data.extend(response['results'])
    
    data_df = pd.json_normalize(data)
    print(f'Successfully pulled data for {year}.')

    # Checking if every downloaded record has been processed
    assert record_count == len(data_df), f"""ERROR: Record Count Mismatch!\n
    Downloaded {record_count} records, but processed {len(data_df)}."""

    with StringIO() as csv_buffer:
        data_df.to_csv(csv_buffer, index=False)
        s3_response = S3.put_object(
            Bucket=BUCKET_NAME,
            Key=f'prek_enrollment_{year}.csv',
            Body=csv_buffer.getvalue()
        )
        http_response = s3_response.get('ResponseMetadata')

        # Verifying that processed data has been uploaded to AWS bucket
        if http_response.get('HTTPStatusCode') == 200:
            print(f'Data for {year} uploaded successfully')
        else:
            print(f'Data for {year} NOT uploaded successfully')

if __name__ == "__main__":
    for y in years:
        pull_and_upload_data(y)