import os
import boto3

sns = boto3.client('sns')
SNS_TOPIC_ARN = os.environ["TOPIC_ARN"]


def create_message(payload, event_status, environment):
    
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

    event_payload = event["responsePayload"] 

    message, subject = create_message(event_payload, event["requestContext"]["condition"], event["requestPayload"]["environment"])

    msg_attributes = {
        "environment": {
            "DataType": "String",
            "StringValue": event["requestPayload"]["environment"]
        }
    }
    sns.publish(TopicArn=SNS_TOPIC_ARN, Message=message, Subject=subject, MessageAttributes=msg_attributes)        

    return {
        "Message": "Lambda Invoked"
    }
