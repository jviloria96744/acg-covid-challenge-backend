import logging
import os
import pandas as pd
from json_logger import setup_logging

setup_logging(logging.INFO)
logger = logging.getLogger()


def extract_data(environment, bucket, key, s3):
    ny_times_url, jh_data_url = set_data_sources(environment)

    try:
        ny_times_data = pd.read_csv(ny_times_url)
    except:
        logger.error("Error downloading NY Times dataset")
        raise

    try:
        jh_data = pd.read_csv(jh_data_url)
    except:
        logger.error("Error downloading Johns Hopking dataset")
        raise

    if len(ny_times_data) == 0 or len(jh_data) == 0:
        logger.error(
            "Error with downloaded data sets, one or more data sets are empty")
        raise
    
    try:
        prev_data = extract_previous_data(bucket, environment + '/' + key, s3)
    except:
        logger.error(
            f"Error retrieving previous data from s3 Bucket: {bucket}")
        raise

    logger.info("Data extracted successfully")
    return ny_times_data, jh_data, prev_data


def set_data_sources(environment):
    if environment == "production":
        ny_times_url = os.environ["PROD_NYT_URL"]
        jh_data_url = os.environ["PROD_JH_URL"]
    else:
        ny_times_url = os.environ["TEST_NYT_URL"]
        jh_data_url = os.environ["TEST_JH_URL"]

    return ny_times_url, jh_data_url


def extract_previous_data(bucket, key, s3):
    try:
        res = s3.get_object(Bucket=bucket, Key=key)
        prev_data = pd.read_csv(res["Body"])
    except:
        return None

    return prev_data
    