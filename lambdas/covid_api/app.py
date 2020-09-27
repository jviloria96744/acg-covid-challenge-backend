import logging
import json
import os
import boto3
from json_logger import setup_logging

setup_logging(logging.INFO)
logger = logging.getLogger()

s3 = boto3.client('s3')
BUCKET_NAME = os.environ["BUCKET_NAME"]
KEY_NAME = os.environ["KEY_NAME"]


def get_covid_data():
    """
    Parameters
    ----------
    None
        We can include parameters in a future enhancement and include them in the WHERE clause of the SQL query
    Returns
    ------
    records: list, list of record (row) objects
    """

    res = s3.select_object_content(
        Bucket=BUCKET_NAME,
        Key=KEY_NAME,
        ExpressionType="SQL",
        Expression=f"""
            select * from s3object s
        """,
        InputSerialization={"CSV": {"FileHeaderInfo": "Use"}},
        OutputSerialization={"JSON": {}},
    )

    for event in res["Payload"]:
        if "Records" in event:
            records = event["Records"]["Payload"].decode("utf-8")

    records = [json.loads(record) for record in records.split("\n") if len(record) > 0]
    
    return records


def get_last_modified_date():
    """
    Parameters
    ----------
    None
    Returns
    ------
    last modified date: str, last modified date of covid data csv converted to string and formatted
    """

    res = s3.head_object(Bucket=BUCKET_NAME, Key=KEY_NAME)

    return res['LastModified'].strftime("%m/%d/%Y, %H:%M:%S")

def lambda_handler(event, context):
    """
    Parameters
    ----------
    event: dict, required 

    context: object, required

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict
        Contains 'body' dict with 'count' key for new visit count
        In event of error, 'body' dict has 'error' key with error message
    """

    try:
        
        data = get_covid_data()
        last_modified_date = get_last_modified_date()
        return {
            "statusCode": 200,
            "headers": {"Access-Control-Allow-Origin":"*"},
            "body": json.dumps({
                "data": data,
                "last_modified_date": last_modified_date
            }),
        }
    except Exception as e:
        return {
            "statusCode": 400,
            "headers": {"Access-Control-Allow-Origin":"*"},
            "body": json.dumps({
                "error": str(e)
            }),
        }
