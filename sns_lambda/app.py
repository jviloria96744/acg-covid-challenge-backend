import os
import boto3

sns = boto3.client('sns')
SNS_TOPIC_ARN = os.environ["TOPIC_ARN"]


def create_message(payload, event_status):
    print(type(payload))
    if event_status == 'Success':
        message = payload["Status"]
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

    event_payload = event["responsePayload"] 

    message, subject = create_message(event_payload, event["requestContext"]["condition"])

    sns.publish(TopicArn=SNS_TOPIC_ARN, Message=message, Subject=subject)        

    return {
        "Message": "Lambda Invoked"
    }
