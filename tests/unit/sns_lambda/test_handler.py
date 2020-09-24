import pytest


@pytest.fixture
def mock_environment_variables(monkeypatch):
    monkeypatch.setenv("TOPIC_ARN", "DUMMY_VALUE")


@pytest.fixture
def params():
    return {
        "Status": "New Data Updated",
        "New Records": 5,
        "Updated Records": "--"
    }


def test_create_message(params, mock_environment_variables):
    
    from sns_lambda import app
    
    message, subject = app.create_message(params, "Success", "production")

    err_msg = ""

    if "Environment: production" not in message:
        err_msg += "Environment is not listed properly"
    
    if subject != "COVID-19 ETL Process Successful, Data Updated":
        err_msg += "Subject is not listed properly"

    assert err_msg == ""


def test_create_message_failure(params):

    from sns_lambda import app

    message, subject = app.create_message(params, "Failure", "production")

    err_msg = ""

    if message != "An error occured in the COVID-19 ETL Process":
        err_msg += "Message is not correct"
    
    if subject != "COVID-19 ETL Process Unsuccessful":
        err_msg += "Subject is not correct"

    assert err_msg == ""
