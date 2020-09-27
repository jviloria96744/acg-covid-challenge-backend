from io import StringIO
import logging
import pandas as pd
from json_logger import setup_logging

setup_logging(logging.INFO)
logger = logging.getLogger()


def load_data(env, bucket, key, change_log_key, new_data, new_records, updated_records, s3):
    """
    Parameters
    ----------
    env: str in {'production', 'testing'} determines S3 key name (prefix)

    bucket: str, S3 Bucket Name used as "database" to store csv files in load step

    key: str, Key that will be used with env to store csv file in S3 bucket

    change_log_key: str, Key that will be used with env to store log of new/updated records

    new_data: DataFrame, output of transform step that will be uploaded to s3

    new_records: List, list of dates that are newly added to the daily data set

    updated_records: List, list of dates that were previously in the data set that have updated fields

    s3: s3 Client 

    Returns
    ------
    No returns, this function uploads two csv files to s3: the resulting data set from the Transform step and a change log of which dates are new/updated
    """

    try:
        upload_data_to_s3(new_data, env, bucket, key, s3)
    except:
        logger.error("Error uploading new data to S3")
        raise

    try:
        change_log_data = create_change_log(new_records, updated_records)
    except:
        logger.error("Error creating change log")
        raise

    try:
        upload_data_to_s3(change_log_data, env, bucket, change_log_key, s3)
    except:
        logger.error("Error uploading change log to S3")
        raise

    logger.info("Data Loading Complete")


def create_change_log(new_records, updated_records):
    """
    Parameters
    ----------
    new_records: List, list of dates that are newly added to the daily data set

    updated_records: List, list of dates that were previously in the data set that have updated fields 

    Returns
    ------
    change_log, DataFrame, dataframe with two fields, 'date' and 'status update' with all dates that are new/updated
    """

    change_status_col = ["NEW RECORD"] * \
        len(new_records) + ["UPDATED_RECORD"] * len(updated_records)
    change_record_col = new_records + updated_records

    change_log = pd.DataFrame(
        [change_record_col, change_status_col]).transpose()
    change_log.columns = ["date", "status_update"]
    return change_log


def upload_data_to_s3(data, env, bucket, key, s3):
    """
    Parameters
    ----------
    data: DataFrame, data to be written to s3

    env: str in {'production', 'testing'} determines S3 key name (prefix)

    bucket: str, S3 Bucket Name used as "database" to store csv files in load step

    key: str, Key that will be used with env to store csv file in S3 bucket

    s3: s3 Client 

    Returns
    ------
    No returns, this function writes a DataFrame to s3 as a csv file
    """

    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Body=csv_buffer.getvalue(),
                  Key=env + '/' + key)

    csv_buffer.close()
