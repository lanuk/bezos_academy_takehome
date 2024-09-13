import requests

from urllib.request import urlopen
from json import loads

API_URL = "https://educationdata.urban.org/api/v1/schools/ccd/enrollment/{year}/{grade}/"
GRADE = 'grade-pk'
years = [
            2020,
            2021,
            2022
        ]

def pull_data(year):
    fetch_url = API_URL.format(year=year, grade=GRADE)
    try:
        response = requests.get(fetch_url)
    except requests.exceptions.RequestException as err:
        raise SystemExit(err)

    return response.json()

# https://educationdata.urban.org/documentation/schools.html#ccd-enrollment-by-grade