import os
import boto3

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

    print(event)

    return {
        "Hello"
    }
