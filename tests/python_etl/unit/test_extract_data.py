import os
import pytest
from moto import mock_s3
import boto3
import pandas as pd
from python_etl import extract_data

REGION = "us-west-2"
BUCKET_NAME = "TEST_BUCKET_NAME"
PROD_NYT_URL = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv"
PROD_JH_URL = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv"
PREV_DATA = "acg-covid-data.csv"
ENVIRONMENT = "production"

@pytest.fixture
def mock_environment_variables(monkeypatch):
    monkeypatch.setenv("PROD_NYT_URL", PROD_NYT_URL)
    monkeypatch.setenv("PROD_JH_URL", PROD_JH_URL)


@mock_s3
def test_extract_data_rows(mock_environment_variables):

    s3 = boto3.client('s3')
        
    s3.create_bucket(
        Bucket=BUCKET_NAME,
        CreateBucketConfiguration={
            'LocationConstraint': REGION,
        },
    )

    nyt_data, jh_data, prev_data = extract_data.extract_data(ENVIRONMENT, BUCKET_NAME, PREV_DATA, s3)

    # The assertion on the error message is used to make a failed test case more informative for troubleshooting
    err_msg = ''

    if len(nyt_data) == 0:
        err_msg += 'NY Times data has no rows\n'

    if len(jh_data) == 0:
        err_msg += 'Johns Hopkins data has no rows\n'

    if not isinstance(nyt_data, pd.DataFrame):
        err_msg += 'NY Times data is not a dataframe\n'

    if not isinstance(jh_data, pd.DataFrame):
        err_msg += 'Johns Hopkins data is not a dataframe\n'

    if prev_data is not None:
        err_msg += 'Previous data should be None Type\n'


    assert err_msg == ''


@mock_s3
def test_extract_data_with_prev_data(mock_environment_variables):

    s3 = boto3.client('s3')
        
    s3.create_bucket(
        Bucket=BUCKET_NAME,
        CreateBucketConfiguration={
            'LocationConstraint': REGION,
        },
    )

    with open(os.path.join(os.path.dirname(__file__), 'mock_prev_data.csv'), 'rb') as data:
        s3.upload_fileobj(data, BUCKET_NAME, ENVIRONMENT + '/' + PREV_DATA)

    prev_data = extract_data.extract_data(ENVIRONMENT, BUCKET_NAME, PREV_DATA, s3)[2]

    # The assertion on the error message is used to make a failed test case more informative for troubleshooting
    
    err_msg = ''

    if not isinstance(prev_data, pd.DataFrame):
        err_msg += "Previous data is not a dataframe\n"
    
    if len(prev_data) == 0:
        err_msg += "There is an error in the s3 GET logic, all data removed"

    assert err_msg == ''