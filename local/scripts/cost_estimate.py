"""Estimate what each measured local run would cost on real AWS, per 1,000
documents, from its measured instance-seconds and AWS list prices.

This is an ESTIMATE from a local simulation, not an AWS bill: the instance
time is what the local Auto Scaling re-creation used, and the request counts
are derived from the pipeline design (see REQUESTS below).

    python cost_estimate.py results/2026-10-23
"""
import json
import sys
from pathlib import Path

run_dir = Path(sys.argv[1])
prices = json.loads((run_dir / "prices_us-east-1.json").read_text())["prices"]
P = {k: v["usd"] for k, v in prices.items()}

HOURS_PER_MONTH = 730          # AWS's convention for monthly-to-hourly
EBS_GB = 8                     # default root volume of an Ubuntu AMI
EBS_PER_HOUR = EBS_GB * P["ebs_gp3_per_gb_month"] / HOURS_PER_MONTH
LONG_POLL_S = 5                # the worker's ReceiveMessage wait time

# Requests per document attempt, from the design:
#   S3:  1 PUT (upload) + 1 COPY (into the processing bucket); 1 GET per attempt
#   SQS: 1 SendMessage (S3 event) + 1 ReceiveMessage + 1 DeleteMessage
#        + 1 SendMessage (result queue)
#   DynamoDB: 1 write request unit (each item is well under 1 KB)
# Idle workers also long-poll SQS; upper bound: one ReceiveMessage per 5 s
# per instance for the whole run.


def estimate(s: dict, instance: str) -> dict:
    docs = s["documents_recorded"]
    hours = s["instance_seconds_total"] / 3600
    attempts = (docs + s.get("failed_attempts_logged", 0) + s.get("injected_crashes", 0)
                + s.get("jobs_abandoned_by_removed_workers", 0))
    ec2 = hours * P[f"ec2_{instance}_linux_per_hour"]
    ipv4 = hours * P["public_ipv4_in_use_per_hour"]
    ebs = hours * EBS_PER_HOUR
    s3 = docs * 2 * P["s3_standard_put_copy_per_1000"] / 1000 + attempts * P["s3_standard_get_per_1000"] / 1000
    sqs_requests = docs * 3 + attempts + s["instance_seconds_total"] / LONG_POLL_S
    sqs = sqs_requests * P["sqs_standard_per_million_requests"] / 1e6
    ddb = attempts * P["dynamodb_on_demand_per_million_write_request_units"] / 1e6
    total = ec2 + ipv4 + ebs + s3 + sqs + ddb
    k = 1000 / docs
    return {"instance_type": instance, "instance_hours": round(hours, 4),
            "per_1000_docs_usd": {"ec2": round(ec2 * k, 4), "public_ipv4": round(ipv4 * k, 4),
                                  "ebs_gp3_8gb": round(ebs * k, 4), "s3_requests": round(s3 * k, 4),
                                  "sqs_requests": round(sqs * k, 4), "dynamodb_writes": round(ddb * k, 4),
                                  "total": round(total * k, 4)},
            "compute_share_pct": round(100 * (ec2 + ipv4 + ebs) / total, 1)}


out = {"label": "ESTIMATE from a local Docker/Moto simulation, not an AWS bill",
       "region": "us-east-1", "prices_file": "prices_us-east-1.json", "runs": {}}
for f in sorted(run_dir.glob("*.summary.json")):
    s = json.loads(f.read_text())
    out["runs"][s["name"]] = {t: estimate(s, t) for t in ("t2_micro", "t3_micro")}
    e = out["runs"][s["name"]]["t2_micro"]
    print(f"{s['name']:<45} {e['instance_hours']:.3f} instance-h  "
          f"t2.micro ${e['per_1000_docs_usd']['total']:.3f} / 1,000 docs "
          f"(compute {e['compute_share_pct']}%)")
(run_dir / "cost_estimate.json").write_text(json.dumps(out, indent=2))
