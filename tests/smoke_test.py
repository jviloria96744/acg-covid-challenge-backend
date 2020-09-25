import json
import time
import boto3

cfn = boto3.client("cloudformation")
sqs = boto3.client("sqs")
s3 = boto3.client("s3")
lambda_client = boto3.client("lambda")


def run_smoke_test():
    BUCKET_NAME, QUEUE_URL, LAMBDA_NAME = get_cloudformation_outputs("python-etl-challenge")

    message, message_receipt_handle, environment = get_sqs_message(QUEUE_URL)

    test_message = "Environment: testing\nNew Data Loaded\nNumber of New Records: 3\nNumber of Updated Records: --"

    if message == test_message:
        testing_objects = [
            {
                'Key': f'{environment}/acg-covid-data.csv'
            },
            {
                'Key': f'{environment}/CHANGE_LOG.csv'
            }
        ]
        res = s3.delete_objects(Bucket=BUCKET_NAME, Delete={'Objects': testing_objects})
        if len(res["Deleted"]) != len(testing_objects):
            raise Exception

        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=message_receipt_handle)
    else:
        raise Exception
    

def get_cloudformation_outputs(stackname):

    response = cfn.describe_stacks(StackName=stackname)
    outputs = response["Stacks"][0]["Outputs"]

    for output in outputs:
        if output["OutputKey"] == "EtlDatabaseBucket":
            BUCKET_NAME = output["OutputValue"]
        elif output["OutputKey"] == "SmokeTestQueue":
            QUEUE_URL = output["OutputValue"]
        elif output["OutputKey"] == "PythonEtlFunction":
            LAMBDA_NAME = output["OutputValue"]

    return BUCKET_NAME, QUEUE_URL, LAMBDA_NAME


def invoke_test_lambda(lambda_name):
    lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType='Event',
        Payload=b"""{
            "environment": "testing"
        }
            """
    )

    
def get_sqs_message(queue_url):
    wait_index = 0

    while True:
        try:
            res = sqs.receive_message(QueueUrl=queue_url)
            message = json.loads(res["Messages"][0]["Body"])
            message_receipt_handle = res["Messages"][0]["ReceiptHandle"]

            environment = message["MessageAttributes"]["environment"]["Value"]

            message = message["Message"]

            break
        except KeyError:
            time.sleep((2**wait_index))
            wait_index += 1

    return message, message_receipt_handle, environment


def verify_message(message):
    test_message = """
    Something
    """

    return message.strip() == test_message.strip()


run_smoke_test()