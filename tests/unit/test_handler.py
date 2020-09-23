import os
import json
import pytest
from moto import mock_s3
import boto3

REGION = "us-west-2"
BUCKET_NAME = "TEST_BUCKET_NAME"
PREV_DATA = "acg-covid-data.csv"
CHANGE_LOG = "CHANGE_LOG.csv"
PROD_NYT_URL = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv"
PROD_JH_URL = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv"


@pytest.fixture
def mock_environment_variables(monkeypatch):
    monkeypatch.setenv("PROD_NYT_URL", PROD_NYT_URL)
    monkeypatch.setenv("PROD_JH_URL", PROD_JH_URL)
    monkeypatch.setenv("BUCKET_NAME", BUCKET_NAME)
    monkeypatch.setenv("PREV_DATA", PREV_DATA)
    monkeypatch.setenv("CHANGE_LOG", CHANGE_LOG)


@mock_s3
def test_lambda_handler(mock_environment_variables):
    s3 = boto3.client('s3')
        
    s3.create_bucket(
        Bucket=BUCKET_NAME,
        CreateBucketConfiguration={
            'LocationConstraint': REGION,
        },
    )

    from python_etl import app

    res = app.lambda_handler({"environment": "production"}, "")

    assert res["Status"] == "New Data Loaded"


@mock_s3
def test_lambda_handler_with_prev_data(mock_environment_variables):
    s3 = boto3.client('s3')
        
    s3.create_bucket(
        Bucket=BUCKET_NAME,
        CreateBucketConfiguration={
            'LocationConstraint': REGION,
        },
    )

    with open(os.path.join(os.path.dirname(__file__), 'mock_prev_data.csv'), 'rb') as data:
        s3.upload_fileobj(data, BUCKET_NAME, 'production/' + PREV_DATA)

    from python_etl import app

    res = app.lambda_handler({"environment": "production"}, "")

    assert res["Status"] == "Daily Data Updated"