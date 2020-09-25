import time
import boto3

cfn = boto3.client("cloudformation")
sqs = boto3.client("sqs")
s3 = boto3.client("s3")


def run_smoke_test():
    BUCKET_NAME, QUEUE_URL = get_cloudformation_outputs("python-etl-challenge")

    message, message_receipt_handle = get_sqs_message(QUEUE_URL)

    test_message = """
    Something
    """

    if message.strip() == test_message.strip():
        testing_objects = [
            {
                'Key': 'testing/acg-covid-data.csv'
            },
            {
                'Key': 'testing/CHANGE_LOG.csv'
            }
        ]
        res = s3.delete_objects(Bucket=BUCKET_NAME, Delete={'Objects': testing_objects})
        if len(res["Deleted"]) != len(testing_objects):
            raise Exception

        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message_receipt_handle)
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

    return BUCKET_NAME, QUEUE_URL


def get_sqs_message(queue_url):
    wait_index = 0

    while True:
        try:
            res = sqs.receive_message(QueueUrl=queue_url)
            message = json.loads(res["Messages"][0]["Body"])
            message_receipt_handle = res["Messages"][0]["ReceiptHandle"]

            message = message["Message"]

            break
        except KeyError:
            time.sleep((2**wait_index))
            wait_index += 1

    return message, message_receipt_handle


def verify_message(message):
    test_message = """
    Something
    """

    return message.strip() == test_message.strip()