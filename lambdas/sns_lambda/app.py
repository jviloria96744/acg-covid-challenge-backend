import logging
import json
import os
import boto3
from json_logger import setup_logging

setup_logging(logging.INFO)
logger = logging.getLogger()

sns = boto3.client('sns')
SNS_TOPIC_ARN = os.environ["TOPIC_ARN"]


def create_message(payload, event_status, environment):
    """
    Parameters
    ----------
    payload: dict
        Status -- str in {New Data Loaded, Daily Data Updated}
        New Records -- int, number of new records added since previous run
        Updated Records -- int, number of updated records from this new run
    
    event_status: str in {Success, Failure} based on source lambda invocation

    environment: str in {production, testing}

    Returns
    ------
    message: str, message that is published to SNS topic

    subject: str, Subject line for SNS topic email subscribers
    """
    
    if event_status == 'Success':
        message = f"Environment: {environment}\n{payload['Status']}\nNumber of New Records: {payload['New Records']}\nNumber of Updated Records: {payload['Updated Records']}"
        subject = "COVID-19 ETL Process Successful, Data Updated"
    else:
        message = "An error occured in the COVID-19 ETL Process"
        subject = "COVID-19 ETL Process Unsuccessful"

    return message, subject


def lambda_handler(event, context):
    """
    Parameters
    ----------
    event: dict, required

        event from python_etl lambda function, this is set as a destination of that lambda
    context: object, required
        Lambda Context runtime methods and attributes

    Returns
    ------
    Invocation is Asynchronous
    """

    logger.info(event)

    try:

        event_payload = event["responsePayload"] 

        message, subject = create_message(event_payload, event["requestContext"]["condition"], event["requestPayload"]["environment"])
    except:
        logger.exception("Error in creating SNS Message")
        raise

    try:

        msg_attributes = {
            "environment": {
                "DataType": "String",
                "StringValue": event["requestPayload"]["environment"]
            }
        }
        sns.publish(TopicArn=SNS_TOPIC_ARN, Message=message, Subject=subject, MessageAttributes=msg_attributes)    
    except:
        logger.exception("Error in SNS publishing")
        raise    

    return {
        "Message": "Lambda Invoked"
    }
