"""The load test from the AWS run: upload every file to the uploading bucket,
then copy them all to the processing bucket at once. Each copy fires an S3
event, so 120 job messages land on the queue within seconds.

    python load_test.py                          # the 120 books
    python load_test.py --poison 3 --malformed 1 # plus failure cases

--poison N adds N corrupt files: 200 KB with no whitespace, which the
worker's word scanner rejects ("token too long") on every attempt.
--malformed N also sends N job messages that are not S3 events at all."""
import argparse
import os
import random
from pathlib import Path

from common import JOB_QUEUE, PROCESS_BUCKET, UPLOAD_BUCKET, client, queue_url

ap = argparse.ArgumentParser()
ap.add_argument("--poison", type=int, default=0)
ap.add_argument("--malformed", type=int, default=0)
args = ap.parse_args()

DATA = Path(__file__).resolve().parents[1] / "data"
POISON = Path(__file__).resolve().parents[1] / "poison"
s3 = client("s3")

files = sorted(DATA.glob("*.txt"))
if args.poison:
    POISON.mkdir(exist_ok=True)
    rng = random.Random(42)
    for i in range(args.poison):
        p = POISON / f"zz-corrupt-{i + 1:02d}.txt"
        p.write_bytes(bytes(rng.randrange(0x21, 0x7f) for _ in range(200_000)))
        files.append(p)
for f in files:
    s3.upload_file(str(f), UPLOAD_BUCKET, f.name)
print(f"Uploaded {len(files)} files to {UPLOAD_BUCKET}")

# Equivalent of: aws s3 cp s3://uploading s3://processing --recursive
for f in files:
    s3.copy_object(Bucket=PROCESS_BUCKET, Key=f.name, CopySource={"Bucket": UPLOAD_BUCKET, "Key": f.name})
print(f"Copied {len(files)} files to {PROCESS_BUCKET}; jobs are on the queue")

if args.malformed:
    sqs, url = client("sqs"), queue_url(JOB_QUEUE)
    for i in range(args.malformed):
        sqs.send_message(QueueUrl=url, MessageBody=f"not an S3 event #{i + 1}: " + os.urandom(8).hex())
    print(f"Sent {args.malformed} malformed job messages")
