#!/bin/bash
set -euo pipefail

LOG_DIR="/opt/notebooks/logs"
LOG_FILE="$LOG_DIR/crypto_pipeline.log"

echo "===============================" >> "$LOG_FILE"
echo "Crypto pipeline started: $(date)" >> "$LOG_FILE"
echo "===============================" >> "$LOG_FILE"

echo "[1/2] Crypto → Hive" >> "$LOG_FILE"
/usr/local/bin/spark-submit \
  /opt/notebooks/preprocess_to_hive/crypto_to_hive.py \
  >> "$LOG_FILE"

echo "[2/2] Aggregates → HBase" >> "$LOG_FILE"
/usr/local/bin/spark-submit \
  /opt/notebooks/Batch_to_HBase/agg_crypto_to_HBase.py \
  >> "$LOG_FILE"

echo "Crypto pipeline finished successfully: $(date)" >> "$LOG_FILE"
