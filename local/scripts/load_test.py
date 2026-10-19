"""The load test from the AWS run: upload every file to the uploading bucket,
then copy them all to the processing bucket at once. Each copy fires an S3
event, so 120 job messages land on the queue within seconds."""
from pathlib import Path

from common import PROCESS_BUCKET, UPLOAD_BUCKET, client

DATA = Path(__file__).resolve().parents[1] / "data"
s3 = client("s3")

files = sorted(DATA.glob("*.txt"))
for f in files:
    s3.upload_file(str(f), UPLOAD_BUCKET, f.name)
print(f"Uploaded {len(files)} files to {UPLOAD_BUCKET}")

# Equivalent of: aws s3 cp s3://uploading s3://processing --recursive
for f in files:
    s3.copy_object(Bucket=PROCESS_BUCKET, Key=f.name, CopySource={"Bucket": UPLOAD_BUCKET, "Key": f.name})
print(f"Copied {len(files)} files to {PROCESS_BUCKET}; jobs are on the queue")
