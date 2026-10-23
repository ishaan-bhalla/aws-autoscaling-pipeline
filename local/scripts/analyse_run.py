"""Measure one run while the emulator is still up, and write a summary JSON.

Reads the samples CSV and removals JSON written by autoscale.py, the worker
logs saved next to them, every item in the DynamoDB table and every message
in the dead-letter queue. Reports:
  - processing time (first enqueue to last DynamoDB write), time until the
    group was back to 1 instance, peak instances and instance-hours;
  - per-document latency p50/p95/max (SQS SentTimestamp -> DynamoDB write);
  - throughput in documents per minute;
  - jobs held and jobs abandoned by each removed worker;
  - correctness: every expected file recorded exactly once, nothing extra,
    and each recorded count equal to an independent recount of the file;
  - failure handling: retries, injected crashes, duplicate deliveries
    caught by the idempotent write, and what ended up in the DLQ.

    python analyse_run.py --csv results/2026-10-23/run.csv --name "simple policy"
"""
import argparse
import csv
import json
import re
from collections import Counter
from pathlib import Path

from common import DLQ, TABLE, client, queue_url

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "local" / "data"
TRIM = '.,"\'?!'


def recount(path: Path) -> Counter:
    """The worker's counting rule, re-implemented: split on whitespace,
    lower-case, skip words of 4 bytes or fewer, then trim punctuation."""
    counts = Counter()
    for word in path.read_text(encoding="utf-8", errors="replace").split():
        word = word.lower()
        if len(word.encode()) <= 4:
            continue
        counts[word.strip(TRIM)] += 1
    return counts


def percentile(values: list[float], p: float) -> float:
    """Nearest-rank percentile."""
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(p / 100 * len(s) + 0.5)) - 1))
    return s[k]


def scan_all(ddb) -> list[dict]:
    items, kwargs = [], {"TableName": TABLE}
    while True:
        page = ddb.scan(**kwargs)
        items += page["Items"]
        if "LastEvaluatedKey" not in page:
            return items
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def drain_dlq(sqs) -> list[dict]:
    try:
        url = queue_url(DLQ)
    except sqs.exceptions.QueueDoesNotExist:
        return []
    out, seen = [], set()
    for _ in range(20):
        msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1,
                                   VisibilityTimeout=600, AttributeNames=["All"]).get("Messages", [])
        if not msgs:
            break
        for m in msgs:
            if m["MessageId"] in seen:
                continue
            seen.add(m["MessageId"])
            try:
                key = json.loads(m["Body"])["Records"][0]["s3"]["object"]["key"]
            except (ValueError, KeyError, IndexError, TypeError):
                key = None
            out.append({"message_id": m["MessageId"], "s3_key": key,
                        "body_preview": None if key else m["Body"][:40],
                        "receive_count_on_dlq": int(m["Attributes"]["ApproximateReceiveCount"])})
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--name", required=True)
    ap.add_argument("--notes", default="")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    rows = list(csv.DictReader(csv_path.open()))
    meta = json.loads(csv_path.with_suffix(".removals.json").read_text())
    logs = {p.stem: p.read_text() for p in (csv_path.parent / (csv_path.stem + "_worker_logs")).glob("*.log")}
    all_logs = "\n".join(logs.values())

    ddb, sqs = client("dynamodb"), client("sqs")
    items = scan_all(ddb)
    expected = {f.name for f in DATA.glob("*.txt")}

    # --- timings --------------------------------------------------------
    recs = []
    for it in items:
        recs.append({"file": it["Filename"]["S"].split("/", 1)[1],
                     "enq": int(it["EnqueuedAtMs"]["N"]), "rec": int(it["RecordedAtMs"]["N"]),
                     "receives": int(it["ReceiveCount"]["N"]),
                     "words": {k: int(v["N"]) for k, v in it["Words"]["M"].items()}})
    lat = [(r["rec"] - r["enq"]) / 1000 for r in recs]
    first_enq = min(r["enq"] for r in recs)
    last_rec = max(r["rec"] for r in recs)
    processing_s = (last_rec - first_enq) / 1000

    # instance-seconds: the worker count holds from one sample to the next
    secs = [int(r["seconds"]) for r in rows]
    workers = [int(r["workers"]) for r in rows]
    inst_s = sum(w * (secs[i + 1] - secs[i]) for i, w in enumerate(workers[:-1]))
    # instance-seconds up to the last DynamoDB write only (the burst itself)
    last_rec_s = (last_rec - meta["start_epoch_ms"]) / 1000
    inst_s_processing = sum(w * max(0, min(secs[i + 1], last_rec_s) - secs[i])
                            for i, w in enumerate(workers[:-1]) if secs[i] < last_rec_s)

    # --- correctness ----------------------------------------------------
    got = Counter(r["file"] for r in recs)
    mismatches = []
    for r in recs:
        if r["file"] not in expected:
            continue
        truth = recount(DATA / r["file"])
        bad = {w: (c, truth[w]) for w, c in r["words"].items() if truth[w] != c}
        if bad:
            mismatches.append({"file": r["file"], "word": next(iter(bad)), "recorded_vs_recount": next(iter(bad.values()))})

    removals = meta["removals"]
    summary = {
        "name": args.name, "notes": args.notes, "policy": meta["policy"],
        "samples_csv": csv_path.name,
        "documents_recorded": len(recs),
        "documents_expected": len(expected),
        "missing": sorted(expected - set(got)),
        "unexpected": sorted(set(got) - expected),
        "duplicate_items": sorted(f for f, n in got.items() if n > 1),
        "word_count_mismatches": mismatches,
        "processing_time_s": round(processing_s, 1),
        "back_to_min_size_s": secs[-1],
        "peak_instances": max(workers),
        "scaling_actions": [f"{r['seconds']} s: {r['action']}" for r in rows if r["action"]],
        "instance_seconds_total": inst_s,
        "instance_hours_total": round(inst_s / 3600, 4),
        "instance_seconds_until_last_write": round(inst_s_processing),
        "latency_s": {"p50": round(percentile(lat, 50), 1), "p95": round(percentile(lat, 95), 1),
                      "max": round(max(lat), 1), "min": round(min(lat), 1)},
        "throughput_docs_per_min": round(len(recs) / (processing_s / 60), 2),
        "removals": [{k: v for k, v in r.items() if k != "abandoned_ids"} for r in removals],
        "jobs_abandoned_by_removed_workers": sum(r["abandoned"] for r in removals),
        "queue_in_flight_at_scale_in_decisions": [r["queue_in_flight_at_decision"] for r in removals],
        "documents_recorded_on_a_redelivery": sum(1 for r in recs if r["receives"] > 1),
        "injected_crashes": len(re.findall(r"^CRASH INJECTED", all_logs, re.M)),
        "injected_crashes_before_write": len(re.findall(r"^CRASH INJECTED before-write", all_logs, re.M)),
        "injected_crashes_after_write": len(re.findall(r"^CRASH INJECTED after-write", all_logs, re.M)),
        "duplicate_deliveries_caught": len(re.findall(r"^Duplicate delivery, already recorded", all_logs, re.M)),
        "failed_attempts_logged": len(re.findall(r"Failed to process job", all_logs)) // 2,
        "unparseable_receives_logged": len(re.findall(r"^Failed to parse", all_logs, re.M)),
        "dead_letter_queue": drain_dlq(sqs),
    }
    # Per crashed file: did a later delivery get recorded?
    crashed_keys = re.findall(r"^CRASH INJECTED \S+ \S+ (\S+)", all_logs, re.M)
    summary["crashed_files_later_recorded"] = sum(1 for k in crashed_keys if k in got)
    summary["crashed_files"] = crashed_keys

    out = csv_path.with_suffix(".summary.json")
    out.write_text(json.dumps(summary, indent=2))
    per_doc = csv_path.with_suffix(".documents.csv")
    with per_doc.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["file", "latency_s", "receive_count_recorded"])
        for r in sorted(recs, key=lambda r: r["enq"]):
            w.writerow([r["file"], round((r["rec"] - r["enq"]) / 1000, 2), r["receives"]])
    print(json.dumps({k: v for k, v in summary.items() if k not in ("removals", "dead_letter_queue")}, indent=1))
    print("DLQ:", summary["dead_letter_queue"])


if __name__ == "__main__":
    main()
