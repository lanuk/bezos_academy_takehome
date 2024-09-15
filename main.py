import boto3
from io import StringIO
import os
import pandas as pd
import requests

API_URL = "https://educationdata.urban.org/api/v1/schools/ccd/enrollment/{year}/{grade}/"
GRADE = 'grade-pk'

S3 = boto3.client('s3')
BUCKET_NAME = 'bezos-academy-prek-enrollment'

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

years = [
            2020,
            2021,
            2022
        ]

def pull_and_upload_data(year):
    fetch_url = API_URL.format(year=year, grade=GRADE)
    try:
        response = requests.get(fetch_url).json()
    except requests.exceptions.RequestException as err:
        raise SystemExit(err)
    
    data = response['results']
    while response['next']:
        try:
            response = requests.get(response['next']).json()
        except requests.exceptions.RequestException as err:
            raise SystemExit(err)
        
        data.extend(response['results'])
    
    data_df = pd.json_normalize(data)

    with StringIO() as csv_buffer:
        data_df.to_csv(csv_buffer, index=False)
        s3_response = s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f'prek_enrollment_{year}.csv',
            Body=csv_buffer.getvalue()
        )

# https://educationdata.urban.org/documentation/schools.html#ccd-enrollment-by-grade