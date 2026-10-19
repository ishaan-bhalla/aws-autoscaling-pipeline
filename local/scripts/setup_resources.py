"""Create the same resources as the AWS run: two buckets, two queues, one
DynamoDB table, and an S3 event notification from the processing bucket to
the job queue (the trigger that starts every job)."""
from common import (JOB_QUEUE, PROCESS_BUCKET, RESULT_QUEUE, TABLE, UPLOAD_BUCKET,
                    client)

s3, sqs, ddb = client("s3"), client("sqs"), client("dynamodb")

for bucket in (UPLOAD_BUCKET, PROCESS_BUCKET):
    s3.create_bucket(Bucket=bucket)

job_url = sqs.create_queue(QueueName=JOB_QUEUE, Attributes={"VisibilityTimeout": "200"})["QueueUrl"]
sqs.create_queue(QueueName=RESULT_QUEUE)
job_arn = sqs.get_queue_attributes(QueueUrl=job_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

s3.put_bucket_notification_configuration(
    Bucket=PROCESS_BUCKET,
    NotificationConfiguration={"QueueConfigurations": [
        {"QueueArn": job_arn, "Events": ["s3:ObjectCreated:*"]}]})

if TABLE not in ddb.list_tables()["TableNames"]:
    ddb.create_table(TableName=TABLE,
                     AttributeDefinitions=[{"AttributeName": "Filename", "AttributeType": "S"}],
                     KeySchema=[{"AttributeName": "Filename", "KeyType": "HASH"}],
                     BillingMode="PAY_PER_REQUEST")

# S3 sends a test event when a notification is configured; the worker would
# discard it, but clear it so the load test starts from an empty queue.
sqs.purge_queue(QueueUrl=job_url)
print(f"Ready: buckets {UPLOAD_BUCKET}, {PROCESS_BUCKET}; queues {JOB_QUEUE}, {RESULT_QUEUE}; table {TABLE}")
