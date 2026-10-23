#!/usr/bin/env bash
# Stop a worker in the middle of a job, as a scale-in would, and check what
# happens to that job. Runs once with the original worker and once with
# WORKER_HARDENED=1. About 2 minutes.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PYTHON:-python3}
DC="docker compose -f local/docker-compose.yml"
for mode in 0 1; do
  export WORKER_HARDENED=$mode WORKER_MESSAGE_VISIBILITY=200
  $DC down -v --remove-orphans >/dev/null 2>&1 || true
  $DC build -q worker 2>/dev/null
  $DC up -d moto >/dev/null 2>&1; sleep 3
  (cd local/scripts && $PY setup_resources.py >/dev/null && $PY - <<'PY'
from common import PROCESS_BUCKET, client
client("s3").put_object(Bucket=PROCESS_BUCKET, Key="alice-01.txt", Body=open("../data/alice-01.txt", "rb").read())
PY
)
  $DC up -d worker >/dev/null 2>&1
  cid=$($DC ps -q worker)
  until docker logs "$cid" 2>&1 | grep -q "received job"; do sleep 1; done
  sleep 3   # the job is now mid-way through its 10-20 s of processing
  t0=$(date +%s); docker stop -t 90 "$cid" >/dev/null; t1=$(date +%s)
  (cd local/scripts && $PY - "$mode" "$((t1 - t0))" <<'PY'
import sys
from common import JOB_QUEUE, TABLE, client, queue_url
a = client("sqs").get_queue_attributes(QueueUrl=queue_url(JOB_QUEUE), AttributeNames=["All"])["Attributes"]
n = client("dynamodb").scan(TableName=TABLE, Select="COUNT")["Count"]
mode = "hardened" if sys.argv[1] == "1" else "original"
print(f"{mode:<9} worker stopped mid-job: stop took {sys.argv[2]} s; result in DynamoDB: {n}; "
      f"message still on queue (hidden until visibility timeout): {a['ApproximateNumberOfMessagesNotVisible']}")
PY
)
  docker logs "$cid" 2>&1 | grep -E "Received terminated|Deleted message|exited cleanly" | sed 's/^/    log: /' || true
done
$DC down -v --remove-orphans >/dev/null 2>&1
