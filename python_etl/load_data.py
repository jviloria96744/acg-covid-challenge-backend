from io import StringIO
import logging
import pandas as pd
from json_logger import setup_logging

setup_logging(logging.INFO)
logger = logging.getLogger()


def load_data(env, bucket, key, change_log_key, new_data, new_records, updated_records, s3):
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
    change_status_col = ["NEW RECORD"] * \
        len(new_records) + ["UPDATED_RECORD"] * len(updated_records)
    change_record_col = new_records + updated_records

    change_log = pd.DataFrame(
        [change_record_col, change_status_col]).transpose()
    change_log.columns = ["date", "status_update"]
    return change_log


def upload_data_to_s3(data, env, bucket, key, s3):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Body=csv_buffer.getvalue(),
                  Key=env + '/' + key)

    csv_buffer.close()
