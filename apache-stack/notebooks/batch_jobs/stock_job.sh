#!/bin/bash
set -euo pipefail

LOG_DIR="/opt/notebooks/logs"
LOG_FILE="$LOG_DIR/stock_pipeline.log"

echo "===============================" >> "$LOG_FILE"
echo "Stock pipeline started: $(date)" >> "$LOG_FILE"
echo "===============================" >> "$LOG_FILE"

echo "[1/2] Stock → Hive" >> "$LOG_FILE"
/usr/local/bin/spark-submit \
  /opt/notebooks/preprocess_to_hive/stock_to_hive.py \
  >> "$LOG_FILE"

echo "[2/2] Aggregates → HBase" >> "$LOG_FILE"
/usr/local/bin/spark-submit \
  /opt/notebooks/Batch_to_HBase/agg_stock_to_HBase.py \
  >> "$LOG_FILE"

echo "Stock pipeline finished successfully: $(date)" >> "$LOG_FILE"
