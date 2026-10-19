"""A local re-creation of the Auto Scaling policy used on AWS.

AWS setup (see README): CloudWatch alarms on the SQS metric
ApproximateNumberOfMessagesVisible drive an EC2 Auto Scaling group.
  - scale out: visible messages >= 10  -> add 1 instance
  - scale in:  visible messages == 0   -> remove 1 instance
  - min 1, max 5 instances, 120 s cooldown after every scaling action
CloudWatch publishes SQS metrics once a minute, so the alarms are evaluated
every 60 s here too. Each worker container stands in for one EC2 instance.

Writes every sample to results/local_run.csv. Run after load_test.py:
    python local/scripts/autoscale.py
"""
import csv
import subprocess
import time
from pathlib import Path

from common import JOB_QUEUE, TABLE, client, queue_url

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-f", str(ROOT / "local" / "docker-compose.yml")]
OUT = ROOT / "results" / "local_run.csv"

MIN_SIZE, MAX_SIZE = 1, 5
SCALE_OUT_AT, SCALE_IN_AT = 10, 0
COOLDOWN_S = 120
EVAL_EVERY_S = 60
SAMPLE_EVERY_S = 10
TOTAL_JOBS = 120
TIMEOUT_S = 45 * 60


def set_workers(n: int) -> None:
    subprocess.run(COMPOSE + ["up", "-d", "--no-recreate", "--scale", f"worker={n}", "worker"],
                   check=True, capture_output=True)


def main() -> None:
    sqs, ddb = client("sqs"), client("dynamodb")
    url = queue_url(JOB_QUEUE)
    workers, last_action = MIN_SIZE, float("-inf")
    set_workers(workers)
    start = next_eval = time.time()
    OUT.parent.mkdir(exist_ok=True)
    with OUT.open("w", newline="") as fh:
        log = csv.writer(fh)
        log.writerow(["seconds", "visible", "in_flight", "workers", "done", "action"])
        while True:
            now = time.time()
            attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=[
                "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
            visible = int(attrs["ApproximateNumberOfMessages"])
            in_flight = int(attrs["ApproximateNumberOfMessagesNotVisible"])
            done = ddb.scan(TableName=TABLE, Select="COUNT")["Count"]

            action = ""
            if now >= next_eval:
                next_eval += EVAL_EVERY_S
                cooled = now - last_action >= COOLDOWN_S
                if cooled and visible >= SCALE_OUT_AT and workers < MAX_SIZE:
                    workers += 1
                    action = f"scale out to {workers}"
                elif cooled and visible <= SCALE_IN_AT and workers > MIN_SIZE:
                    workers -= 1
                    action = f"scale in to {workers}"
                if action:
                    set_workers(workers)
                    last_action = now

            t = round(now - start)
            log.writerow([t, visible, in_flight, workers, done, action])
            fh.flush()
            print(f"{t:>5}s  visible {visible:>3}  in flight {in_flight:>3}  workers {workers}  done {done:>3}  {action}")

            if done >= TOTAL_JOBS and visible == 0 and in_flight == 0 and workers == MIN_SIZE:
                print("All jobs recorded and the group is back to its minimum size.")
                break
            if now - start > TIMEOUT_S:
                print("Timed out.")
                break
            time.sleep(SAMPLE_EVERY_S)


if __name__ == "__main__":
    main()
