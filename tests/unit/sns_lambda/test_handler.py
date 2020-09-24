import pytest
import boto3
from moto import mock_sns


@pytest.fixture()
def aws_credentials(monkeypatch):
    """Mocked AWS Credentials for moto."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")


@pytest.fixture
def mock_aws_environment(monkeypatch, aws_credentials):
    
    with mock_sns():
        boto3.setup_default_session()
        sns = boto3.client('sns')
        mock_topic = sns.create_topic(Name="some-topic")
        monkeypatch.setenv("TOPIC_ARN", mock_topic.get('TopicArn'))

        from sns_lambda import app

    return app


@pytest.fixture
def params():
    return {
        "Status": "New Data Updated",
        "New Records": 5,
        "Updated Records": "--"
    }


def test_create_message(params, mock_aws_environment):
    
    message, subject = mock_aws_environment.create_message(params, "Success", "production")

    err_msg = ""

    if "Environment: production" not in message:
        err_msg += "Environment is not listed properly"
    
    if subject != "COVID-19 ETL Process Successful, Data Updated":
        err_msg += "Subject is not listed properly"

    assert err_msg == ""


def test_create_message_failure(params, mock_aws_environment):

    message, subject = mock_aws_environment.create_message(params, "Failure", "production")

    err_msg = ""

    if message != "An error occured in the COVID-19 ETL Process":
        err_msg += "Message is not correct"
    
    if subject != "COVID-19 ETL Process Unsuccessful":
        err_msg += "Subject is not correct"

    assert err_msg == ""


@pytest.fixture
def sns_lambda_event():
    return {
        "responsePayload": {
            "Status": "New Data Updated",
            "New Records": 5,
            "Updated Records": "--"
        },
        "requestContext": {
            "condition": "Success"
        },
        "requestPayload": {
            "environment": "production"
        }
    }



@mock_sns
def test_handler(sns_lambda_event, aws_credentials, monkeypatch):
    sns = boto3.client('sns')
    mock_topic = sns.create_topic(Name="some-topic")
    monkeypatch.setenv("TOPIC_ARN", mock_topic.get('TopicArn'))

    from sns_lambda import app

    res = app.lambda_handler(sns_lambda_event, "")

    assert res["Message"] == "Lambda Invoked"