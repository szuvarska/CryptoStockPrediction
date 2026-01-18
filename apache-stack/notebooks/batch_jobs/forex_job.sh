#!/bin/bash
set -euo pipefail

LOG_DIR="/opt/notebooks/logs"
LOG_FILE="$LOG_DIR/forex_pipeline.log"

echo "===============================" >> "$LOG_FILE"
echo "Forex pipeline started: $(date)" >> "$LOG_FILE"
echo "===============================" >> "$LOG_FILE"

echo "[1/1] Forex → Hive" >> "$LOG_FILE"
/usr/local/bin/spark-submit \
  /opt/notebooks/preprocess_to_hive/forex_to_hive.py \
  >> "$LOG_FILE"

echo "Forex pipeline finished successfully: $(date)" >> "$LOG_FILE"
