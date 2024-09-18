import boto3
from dotenv import load_dotenv
import json
import os
import requests

API_URL = 'https://educationdata.urban.org/api/v1/schools/ccd/enrollment/{year}/{grade}/'
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

def pull_and_upload_data(year, grade):
    print(f'Starting to pull data for {year}: ', end='')
    fetch_url = API_URL.format(year=year, grade=grade)
    #
    try:
        response = requests.get(fetch_url).json()
    except requests.exceptions.RequestException as err:
        raise SystemExit(err)
    else:
        data = response['results']
        record_count = response['count']
    #
    while response['next']:
        try:
            response = requests.get(response['next']).json()
        except requests.exceptions.RequestException as err:
            raise SystemExit(err)
        else:
            data.extend(response['results'])
    print(f'SUCCESS')
    js = [json.dumps(i) for i in data]
    assert record_count == len(data), f"""ERROR: Record Count Mismatch!\n
    Downloaded {record_count} records, but processed {len(data)}."""
    # Start upload to S3
    print(f'Starting to upload data for {year}: ', end='')
    s3_response = S3.put_object(
        Bucket=BUCKET_NAME,
        Key=f'prek_enrollment_{year}.json',
        Body='[' + ', \n'.join(js) + ']'
    )
    http_response = s3_response.get('ResponseMetadata')
    # Verifying that processed data has been uploaded to AWS bucket
    if http_response.get('HTTPStatusCode') == 200:
        print(f'Data for {year} uploaded successfully\n')
    else:
        print(f'Data for {year} NOT uploaded successfully\n')

if __name__ == "__main__":
    for y in years:
        pull_and_upload_data(y, GRADE)