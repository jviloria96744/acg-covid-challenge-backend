import logging
import os
import pandas as pd
from json_logger import setup_logging

setup_logging(logging.INFO)
logger = logging.getLogger()


def extract_data(environment, bucket, key, s3):
    """
    Parameters
    ----------
    environment: str in {'production', 'testing'} determines which download URLs to use and S3 key name

    bucket: str, S3 Bucket Name used as "database" to store csv files in load step

    key: str, Key that will be used to store csv file in S3 bucket, in this case, it is used to retrieve previous day's/run's data

    s3: s3 Client 

    Returns
    ------
    ny_times_data: DataFrame, downloaded NY Times Data

    jh_data: DataFrame, downloaded Johns Hopkins Data

    prev_data: DataFrame or None, previous day's/run's data retrieved from s3 Bucket.  On initial load of data, this will be None
    """

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
    """
    Parameters
    ----------
    environment: str in {'production', 'testing'} determines which download URLs to use and S3 key name

    Returns
    ------
    ny_times_url: str, NY Times Data download source

    jh_data: str, Johns Hopkins Data download source

    During testing, dummy data is used from a GitHub Gist
    """
    if environment == "production":
        ny_times_url = os.environ["PROD_NYT_URL"]
        jh_data_url = os.environ["PROD_JH_URL"]
    else:
        ny_times_url = os.environ["TEST_NYT_URL"]
        jh_data_url = os.environ["TEST_JH_URL"]

    return ny_times_url, jh_data_url


def extract_previous_data(bucket, key, s3):
    """
    Parameters
    ----------
    bucket: str, S3 Bucket Name used as "database" to store csv files in load step

    key: str, Key that will be used to store csv file in S3 bucket, in this case, it is used to retrieve previous day's/run's data

    s3: s3 Client 

    Returns
    ------
    prev_data: DataFrame or None, previous day's/run's data retrieved from s3 Bucket.  On initial load of data, this will be None
    """

    try:
        res = s3.get_object(Bucket=bucket, Key=key)
        prev_data = pd.read_csv(res["Body"])
    except:
        return None

    return prev_data
    