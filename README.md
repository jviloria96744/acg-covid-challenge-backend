# ACloudGuru September 2020 Challenge - Python COVID-19 ETL Process

This project contains source code and supporting files for a serverless application that provides the back-end for an event driven Python ETL Process for two COVID-19 Data Sources.

## Project Description

Basically, the project is to create an ETL process that runs daily based on a CloudWatch Event.

This process takes two COVID-19 data sources, merges them and performs a few transformations before loading them into a DB. Notifications should be sent after any successful updates or upon failure. A public dashboard is required for visualization of the data.

Finally, a blog post is required detailing our experience.

The official project description/announcement can be found [here](https://acloudguru.com/blog/engineering/cloudguruchallenge-python-aws-etl).

# Project Structure

In this section, I describe the directory structure of this repository.

- `.github/` - This is the CI/CD workflow used for deployment by GitHub Actions

- `events/` - This directory contains one json file, `event.json` used for local testing

- `lambdas/` - This directory contains the source code for the project lambda services. The individual services are described in the resources subsection

- `tests/` - This directory contains unit tests for two of the lambda services, each in their own directory. The root directory contains two files
  - `test_setup.py` - This file sets the path to be used by the test runner, adding the individual lambda src code directories to the testing path.
  - `smoke_test.py` - This file is used during the GitHub Actions workflow to run a smoke test and add test records to the DB and verify a correct SNS message is sent as a response
  - `conftest.py` - This file is actually located in the root of the project and is a config file that ignores `smoke_test.py` when running the `pytest` command.

The root of the project contains a `template.yaml` file which is the SAM Template that is responsible for deploying all of our AWS Resources described below.

## aWS Resources

The following is a list of AWS resources deployed for this project.

- S3 Bucket - A csv file stored in S3 acts as our "Database". The file also includes a prefix denoting the development environment, e.g. testing or production

- SNS Topic - This topic is used for notifying after successful/failed DB updates

- SQS Queue - This queue is created as a subscriber to the SNS Topic. The queue is only meant to be a subscriber for the smoke test during the CI/CD workflow.

- SNS Topic Subscriptions - Two subscribers are created for the SNS Topic described above,

  - Email - An email subscriber gets all notifications
  - SQS Queue - A SQS queue gets notifications created by the smoke test during the CI/CD workflow

- Queue Policy - This allows the SNS Topic to send messages to the SQS Queue.

- Python ETL Lambda Function - This function contains the ETL Logic. It is triggered by a daily CloudWatch Event Rule and has a Lambda Function destination for both Success and Failure states.

- SNS Lambda Function - This is the destination of the Python ETL Function and contains the logic for generating and publishing the SNS message in the case of job Success/Failure.

- COVID API Function/API Gateway - This is the API/Lambda that exposes the output of the ETL process publically.
