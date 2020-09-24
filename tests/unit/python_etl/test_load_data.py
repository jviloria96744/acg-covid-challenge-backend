import os
import pytest
from moto import mock_s3
import boto3
import pandas as pd
from python_etl import load_data

REGION = "us-west-2"
BUCKET_NAME = "TEST_BUCKET_NAME"
KEY = "acg-covid-data.csv"
CHANGE_LOG = "CHANGE_LOG.csv"
ENVIRONMENT = "production"


def test_create_change_log():

    new_records = ["2020-09-23"]
    updated_records = ["2020-09-21", "2020-09-22"]

    change_log = load_data.create_change_log(new_records, updated_records)

    # The assertion on the error message is used to make a failed test case more informative for troubleshooting
    err_msg = ''

    if len(change_log) != 3:
        err_msg += 'Change log has the incorrect number of rows\n'

    if not isinstance(change_log, pd.DataFrame):
        err_msg += 'Change log is not a dataframe'

    assert err_msg == ''


@mock_s3
def test_load_data():

    s3 = boto3.client('s3')
        
    s3.create_bucket(
        Bucket=BUCKET_NAME,
        CreateBucketConfiguration={
            'LocationConstraint': REGION,
        },
    )

    new_records = ["2020-09-23"]
    updated_records = ["2020-09-21", "2020-09-22"]
    new_data = pd.read_csv(os.path.join(os.path.dirname(__file__), 'mock_prev_data.csv'))

    load_data.load_data(ENVIRONMENT, BUCKET_NAME, KEY, CHANGE_LOG, new_data, new_records, updated_records, s3)

    items = s3.list_objects_v2(Bucket=BUCKET_NAME)

    keys = set([item['Key'] for item in items["Contents"]])

    assert set([ENVIRONMENT + '/' + KEY, ENVIRONMENT + '/' + CHANGE_LOG]) == keys
