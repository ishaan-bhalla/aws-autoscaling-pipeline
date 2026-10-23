"""A local re-creation of the Auto Scaling policies, old and new.

--policy simple (the policy used on AWS, see README):
  CloudWatch alarms on the SQS metric ApproximateNumberOfMessagesVisible
  drive an EC2 Auto Scaling group.
  - scale out: visible messages >= 10  -> add 1 instance
  - scale in:  visible messages == 0   -> remove 1 instance
  - 120 s cooldown after every scaling action

--policy target (the fix proposed in README section 4):
  Target tracking on backlog per instance, where backlog counts the work
  waiting *and* the work in progress:
      backlog per instance = (visible + in flight) / instances in service
  The policy keeps this near --target (10 messages per instance):
      desired = ceil((visible + in flight) / target), clamped to [min, max]
  Like the alarms AWS manages for target tracking, it scales out after
  --out-evals consecutive minutes above target (default 3) and scales in only
  after --in-evals consecutive minutes below target (default 15), so it
  adds capacity quickly and removes it slowly. Several instances can be
  added or removed in one step.

Both: min 1, max 5 instances; CloudWatch publishes SQS metrics once a minute,
so the policy is evaluated every 60 s. Each worker container stands in for
one EC2 instance. A removed worker gets SIGTERM and up to 90 s to stop (the
systemd default on the instances). Its log is saved just before and just
after the stop, so the jobs it held when it was removed can be counted.

Writes every 10 s sample to --out (CSV) and the worker logs next to it.
Run after load_test.py:
    python local/scripts/autoscale.py --policy target --out results/run.csv
"""
import argparse
import csv
import json
import math
import subprocess
import threading
import time
from pathlib import Path

from common import DLQ, JOB_QUEUE, TABLE, client, queue_url

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-f", str(ROOT / "local" / "docker-compose.yml")]

MIN_SIZE, MAX_SIZE = 1, 5
SCALE_OUT_AT, SCALE_IN_AT = 10, 0   # simple policy thresholds
COOLDOWN_S = 120                    # simple policy cooldown
EVAL_EVERY_S = 60
SAMPLE_EVERY_S = 10
STOP_TIMEOUT_S = 90
TIMEOUT_S = 45 * 60


def docker_out(*cmd: str) -> str:
    return subprocess.run(list(cmd), capture_output=True, text=True).stdout.strip()


def running_workers() -> list[str]:
    out = subprocess.run(COMPOSE + ["ps", "-q", "worker"], check=True, capture_output=True, text=True)
    return [c for c in out.stdout.split() if c]


def container_log(cid: str) -> str:
    out = subprocess.run(["docker", "logs", cid], capture_output=True, text=True)
    return out.stdout + out.stderr


def held_messages(log: str) -> list[str]:
    """Message IDs a worker received and had not finished, given up on, or
    crashed on, according to its own log (last event per message wins)."""
    last = {}
    for line in log.splitlines():
        parts = line.replace(",", " ").split()
        if line.startswith("Processing message "):
            last[parts[2]] = "held"
        elif line.startswith("Deleted message"):
            last[parts[2]] = "done"
        elif "Failed to process job" in line:
            last[parts[parts.index("job") + 1]] = "done"   # left to time out and retry
        elif line.startswith("Left unparseable message"):
            last[parts[-1]] = "done"
        elif line.startswith("CRASH INJECTED"):
            last[parts[3]] = "crashed"
    return [m for m, state in last.items() if state == "held"]


class Group:
    """The worker containers, scaled like an Auto Scaling group."""

    def __init__(self, log_dir: Path):
        self.log_dir = log_dir
        self.removals = []
        self.threads = []

    def scale_out(self, n: int) -> None:
        subprocess.run(COMPOSE + ["up", "-d", "--no-recreate", "--scale", f"worker={n}", "worker"],
                       check=True, capture_output=True)

    def remove(self, count: int, t: float, queue_in_flight: int) -> None:
        # Like compose's own scale-down, remove the newest containers first.
        victims = sorted(running_workers(), key=lambda c: docker_out("docker", "inspect", "-f", "{{.Created}}", c))
        for cid in victims[-count:]:
            name = docker_out("docker", "inspect", "-f", "{{.Name}}", cid).lstrip("/")
            # Rename first, so compose's next scale-out does not count it.
            subprocess.run(["docker", "rename", cid, f"{name}-removed"], capture_output=True)
            th = threading.Thread(target=self._stop, args=(cid, name, t, queue_in_flight))
            th.start()
            self.threads.append(th)

    def _stop(self, cid: str, name: str, t: float, queue_in_flight: int) -> None:
        held_at_stop = held_messages(container_log(cid))
        t0 = time.time()
        subprocess.run(["docker", "stop", "-t", str(STOP_TIMEOUT_S), cid], capture_output=True)
        stop_s = time.time() - t0
        log = container_log(cid)
        (self.log_dir / f"{name}.log").write_text(log)
        subprocess.run(["docker", "rm", cid], capture_output=True)
        abandoned = held_messages(log)
        self.removals.append({
            "seconds": round(t), "container": name, "queue_in_flight_at_decision": queue_in_flight,
            "held_when_stopped": len(held_at_stop), "abandoned": len(abandoned),
            "abandoned_ids": abandoned, "stop_took_s": round(stop_s, 1),
            "exited_cleanly": "Worker service exited cleanly" in log})

    def finish(self) -> None:
        for th in self.threads:
            th.join()
        for cid in running_workers():
            name = docker_out("docker", "inspect", "-f", "{{.Name}}", cid).lstrip("/")
            (self.log_dir / f"{name}.log").write_text(container_log(cid))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--policy", choices=["simple", "target"], default="simple")
    ap.add_argument("--target", type=float, default=10, help="target backlog per instance")
    ap.add_argument("--out-evals", type=int, default=3)
    ap.add_argument("--in-evals", type=int, default=15)
    ap.add_argument("--expect", type=int, default=120, help="documents that should reach DynamoDB")
    ap.add_argument("--out", default=str(ROOT / "results" / "local_run.csv"))
    args = ap.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    log_dir = out.parent / (out.stem + "_worker_logs")
    log_dir.mkdir(exist_ok=True)

    sqs, ddb = client("sqs"), client("dynamodb")
    url = queue_url(JOB_QUEUE)
    try:
        dlq_url = queue_url(DLQ)
    except sqs.exceptions.QueueDoesNotExist:
        dlq_url = None

    group = Group(log_dir)
    workers, last_action = MIN_SIZE, float("-inf")
    high = low = 0
    group.scale_out(workers)
    start = next_eval = time.time()
    with out.open("w", newline="") as fh:
        log = csv.writer(fh)
        log.writerow(["seconds", "visible", "in_flight", "workers", "done", "dlq", "action"])
        while True:
            now = time.time()
            attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=[
                "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
            visible = int(attrs["ApproximateNumberOfMessages"])
            in_flight = int(attrs["ApproximateNumberOfMessagesNotVisible"])
            dlq = int(sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=[
                "ApproximateNumberOfMessages"])["Attributes"]["ApproximateNumberOfMessages"]) if dlq_url else 0
            done = ddb.scan(TableName=TABLE, Select="COUNT")["Count"]

            action, new = "", workers
            if now >= next_eval:
                next_eval += EVAL_EVERY_S
                if args.policy == "simple":
                    cooled = now - last_action >= COOLDOWN_S
                    if cooled and visible >= SCALE_OUT_AT and workers < MAX_SIZE:
                        new = workers + 1
                    elif cooled and visible <= SCALE_IN_AT and workers > MIN_SIZE:
                        new = workers - 1
                else:
                    total = visible + in_flight
                    per_instance = total / workers
                    desired = max(MIN_SIZE, min(MAX_SIZE, math.ceil(total / args.target)))
                    high = high + 1 if per_instance > args.target else 0
                    low = low + 1 if per_instance < args.target else 0
                    if high >= args.out_evals and desired > workers:
                        new, high = desired, 0
                    elif low >= args.in_evals and desired < workers:
                        new, low = desired, 0
                if new > workers:
                    action = f"scale out to {new}"
                    group.scale_out(new)
                elif new < workers:
                    action = f"scale in to {new}"
                    group.remove(workers - new, now - start, in_flight)
                if action:
                    workers, last_action = new, now

            t = round(now - start)
            log.writerow([t, visible, in_flight, workers, done, dlq, action])
            fh.flush()
            print(f"{t:>5}s  visible {visible:>3}  in flight {in_flight:>3}  workers {workers}  "
                  f"done {done:>3}  dlq {dlq}  {action}", flush=True)

            if done >= args.expect and visible == 0 and in_flight == 0 and workers == MIN_SIZE:
                print("All jobs recorded and the group is back to its minimum size.")
                break
            if now - start > TIMEOUT_S:
                print("Timed out.")
                break
            time.sleep(SAMPLE_EVERY_S)

    group.finish()
    meta = {"policy": args.policy, "target": args.target, "out_evals": args.out_evals,
            "in_evals": args.in_evals, "start_epoch_ms": int(start * 1000),
            "end_epoch_ms": int(time.time() * 1000), "removals": group.removals}
    out.with_suffix(".removals.json").write_text(json.dumps(meta, indent=2))
    for r in group.removals:
        print(f"removed {r['container']} at {r['seconds']}s: held {r['held_when_stopped']}, "
              f"abandoned {r['abandoned']}, stop took {r['stop_took_s']} s")


if __name__ == "__main__":
    main()
