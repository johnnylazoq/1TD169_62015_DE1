#!/bin/bash
# Automates the 1/2/3 worker scaling experiments

INPUT="hdfs://g11-master:9000/nyc_taxi_data/weak_3GB/*.parquet"
OUTPUT_BASE="hdfs://g11-master:9000/output_results/scaling"
FORMAT="parquet"
MASTER="spark://g11-master:7077"

echo "=== SCALABILITY BENCHMARK ==="

# Create the logs directory if it doesn't exist
mkdir -p logs

for WORKERS in 1 2 3; do
    echo "--- Running with $WORKERS worker(s) ---"
    spark-submit \
        --master $MASTER \
        --num-executors $WORKERS \
        --executor-memory 2g \
        --executor-cores 2 \
        src/analysis_job.py \
            --input  $INPUT \
            --output $OUTPUT_BASE/run_${WORKERS}workers \
            --format $FORMAT \
            --task   all \
            --cores  2 \
            --memory 2g \
        2>&1 | tee logs/run_${WORKERS}workers.log
done

echo "=== Done. Logs saved to logs/ ==="
