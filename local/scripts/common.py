"""Shared names and clients for the local scripts (all talk to Moto on :4566)."""
import boto3

ENDPOINT = "http://localhost:4566"
REGION = "us-east-1"
UPLOAD_BUCKET = "wordfreq-uploading"
PROCESS_BUCKET = "wordfreq-processing"
JOB_QUEUE = "wordfreq-jobs"
RESULT_QUEUE = "wordfreq-results"
DLQ = "wordfreq-jobs-dlq"
TABLE = "wordfreq"


def client(service: str):
    return boto3.client(service, endpoint_url=ENDPOINT, region_name=REGION,
                        aws_access_key_id="local", aws_secret_access_key="local")


def queue_url(name: str) -> str:
    return client("sqs").get_queue_url(QueueName=name)["QueueUrl"]
