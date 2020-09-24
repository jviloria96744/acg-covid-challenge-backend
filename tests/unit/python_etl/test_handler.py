import os
import json
import pytest
from moto import mock_s3
import boto3

REGION = "us-west-2"
BUCKET_NAME = "TEST_BUCKET_NAME"
PREV_DATA = "acg-covid-data.csv"
CHANGE_LOG = "CHANGE_LOG.csv"
TEST_NYT_URL = "https://gist.githubusercontent.com/jviloria96744/c5b713facc72861a6facdb437667e937/raw/5c8da6b2fec7f18fc22c0067dbf7479a6ae49fcc/test_nyt_data.csv"
TEST_JH_URL = "https://gist.githubusercontent.com/jviloria96744/c5b713facc72861a6facdb437667e937/raw/5c8da6b2fec7f18fc22c0067dbf7479a6ae49fcc/test_jh_data.csv"


@pytest.fixture
def mock_environment_variables(monkeypatch):
    monkeypatch.setenv("TEST_NYT_URL", TEST_NYT_URL)
    monkeypatch.setenv("TEST_JH_URL", TEST_JH_URL)
    monkeypatch.setenv("BUCKET_NAME", BUCKET_NAME)
    monkeypatch.setenv("PREV_DATA", PREV_DATA)
    monkeypatch.setenv("CHANGE_LOG", CHANGE_LOG)

@pytest.fixture
def event():
    return {
        "environment": "testing"
    }


@mock_s3
def test_lambda_handler(event, mock_environment_variables):
    s3 = boto3.client('s3')
        
    s3.create_bucket(
        Bucket=BUCKET_NAME,
        CreateBucketConfiguration={
            'LocationConstraint': REGION,
        },
    )

    from python_etl import app

    res = app.lambda_handler(event, "")

    assert res["Status"] == "New Data Loaded"


@mock_s3
def test_lambda_handler_with_prev_data(event, mock_environment_variables):
    s3 = boto3.client('s3')
        
    s3.create_bucket(
        Bucket=BUCKET_NAME,
        CreateBucketConfiguration={
            'LocationConstraint': REGION,
        },
    )

    with open(os.path.join(os.path.dirname(__file__), 'mock_prev_data.csv'), 'rb') as data:
        s3.upload_fileobj(data, BUCKET_NAME, event["environment"] + '/' + PREV_DATA)

    from python_etl import app

    res = app.lambda_handler(event, "")

    assert res["Status"] == "Daily Data Updated"