"""Create the same resources as the AWS run: two buckets, two queues, one
DynamoDB table, and an S3 event notification from the processing bucket to
the job queue (the trigger that starts every job).

    python setup_resources.py                       # as on AWS: 200 s visibility, no DLQ
    python setup_resources.py --visibility 60 --dlq 3   # dead-letter queue after 3 receives
"""
import argparse
import json

from common import (DLQ, JOB_QUEUE, PROCESS_BUCKET, RESULT_QUEUE, TABLE, UPLOAD_BUCKET,
                    client)

ap = argparse.ArgumentParser()
ap.add_argument("--visibility", type=int, default=200, help="job queue visibility timeout (s)")
ap.add_argument("--dlq", type=int, default=0, metavar="MAX_RECEIVES",
                help="add a dead-letter queue; move a message there after this many receives")
args = ap.parse_args()

s3, sqs, ddb = client("s3"), client("sqs"), client("dynamodb")

for bucket in (UPLOAD_BUCKET, PROCESS_BUCKET):
    s3.create_bucket(Bucket=bucket)

attrs = {"VisibilityTimeout": str(args.visibility)}
if args.dlq:
    dlq_url = sqs.create_queue(QueueName=DLQ, Attributes={"MessageRetentionPeriod": "1209600"})["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    attrs["RedrivePolicy"] = json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": str(args.dlq)})
job_url = sqs.create_queue(QueueName=JOB_QUEUE, Attributes=attrs)["QueueUrl"]
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
dlq_note = f", dead-letter queue {DLQ} after {args.dlq} receives" if args.dlq else ""
print(f"Ready: buckets {UPLOAD_BUCKET}, {PROCESS_BUCKET}; queues {JOB_QUEUE} "
      f"(visibility {args.visibility} s{dlq_note}), {RESULT_QUEUE}; table {TABLE}")
