#!/usr/bin/env bash
set -euo pipefail

INPUT_CSV="${1:-/data/yellow_tripdata_sample.csv}"
OUTPUT_DIR="${2:-/artifacts/sample}"

mkdir -p "./artifacts" "./spark-events"

docker compose exec -T \
  -e INPUT_CSV="$INPUT_CSV" \
  -e OUTPUT_DIR="$OUTPUT_DIR" \
  spark /opt/spark/bin/spark-submit /opt/spark/work/pipeline.py