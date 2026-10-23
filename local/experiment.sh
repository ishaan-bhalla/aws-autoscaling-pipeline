#!/usr/bin/env bash
# One measured run of the local stack under a named configuration.
#   ./local/experiment.sh NAME [options]
# Options (environment variables):
#   POLICY=simple|target   scaling policy (default simple, the AWS one)
#   IN_EVALS=15            target policy: minutes below target before scaling in
#   HARDENED=0|1           worker failure-handling changes (default 0)
#   VISIBILITY=200         job queue visibility timeout, seconds
#   DLQ=0                  maxReceiveCount for a dead-letter queue (0 = none)
#   CRASH_EVERY=0          inject worker crashes (see app/cmd/worker/config.go)
#   POISON=0 MALFORMED=0   corrupt files / non-S3 messages to add to the load
#   OUT_DIR=results/<today>
# Writes OUT_DIR/NAME.csv, NAME.removals.json, NAME.summary.json,
# NAME.documents.csv and NAME_worker_logs/.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PYTHON:-python3}
NAME=$1
OUT_DIR=${OUT_DIR:-results/$(date +%F)}
mkdir -p "$OUT_DIR"
export WORKER_HARDENED=${HARDENED:-0} WORKER_MESSAGE_VISIBILITY=${VISIBILITY:-200} WORKER_CRASH_EVERY=${CRASH_EVERY:-0}
DC="docker compose -f local/docker-compose.yml"

$DC down -v --remove-orphans >/dev/null 2>&1 || true
docker ps -aq --filter label=com.docker.compose.project=wordfreq | xargs -r docker rm -f >/dev/null
$DC build -q worker
$DC up -d moto
sleep 3
OUT=$(cd "$OUT_DIR" && pwd)/$NAME.csv
(cd local/scripts && $PY fetch_books.py >/dev/null \
  && $PY setup_resources.py --visibility "${VISIBILITY:-200}" --dlq "${DLQ:-0}" \
  && $PY load_test.py --poison "${POISON:-0}" --malformed "${MALFORMED:-0}" \
  && $PY autoscale.py --policy "${POLICY:-simple}" --in-evals "${IN_EVALS:-15}" --out "$OUT" \
  && $PY analyse_run.py --csv "$OUT" --name "$NAME" \
       --notes "policy=${POLICY:-simple} in_evals=${IN_EVALS:-15} hardened=$WORKER_HARDENED visibility=${VISIBILITY:-200} dlq=${DLQ:-0} crash_every=$WORKER_CRASH_EVERY poison=${POISON:-0} malformed=${MALFORMED:-0}")
$DC down -v --remove-orphans >/dev/null 2>&1
docker ps -aq --filter label=com.docker.compose.project=wordfreq | xargs -r docker rm -f >/dev/null
