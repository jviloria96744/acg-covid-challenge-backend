import os
import boto3
from extract_data import extract_data
from transform_data import transform_data
from load_data import load_data

s3 = boto3.client('s3')

BUCKET_NAME = os.environ["BUCKET_NAME"]
KEY = os.environ["PREV_DATA"]
CHANGE_LOG = os.environ["CHANGE_LOG"]


def lambda_handler(event, context):
    """
    Parameters
    ----------
    event: dict, required

        keys:
            environment: str in {'production', 'development'} determines which download URLs to use

    context: object, required
        Lambda Context runtime methods and attributes

    Returns
    ------
    Invocation is Asynchronous
    """

    env = event["environment"]

    ny_times_data, jh_data, prev_data = extract_data(
        env, BUCKET_NAME, KEY, s3)

    transformed_data, new_records, updated_records = transform_data(
        ny_times_data, jh_data, prev_data)

    load_data(env, BUCKET_NAME, KEY, CHANGE_LOG,
              transformed_data, new_records, updated_records, s3)

    if prev_data is None:
        return {
            "Status": "New Data Loaded",
            "New Records": str(len(new_records)),
            "Updated Records": "--"
        }

    return {
        "Status": "Daily Data Updated",
        "New Records": str(len(new_records)),
        "Updated Records": str(len(updated_records))
    }