#!/usr/bin/env bash
# One command for the whole local re-run: emulator, resources, 120 files,
# the scaling policy, and the chart. Takes about 12 minutes.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PYTHON:-python3}

docker compose -f local/docker-compose.yml down -v --remove-orphans >/dev/null 2>&1 || true
docker compose -f local/docker-compose.yml up -d --build moto
sleep 3
(cd local/scripts && $PY fetch_books.py && $PY setup_resources.py && $PY load_test.py && $PY autoscale.py && $PY plot_run.py)
docker compose -f local/docker-compose.yml down -v
