#!/bin/bash
# Automates BOTH horizontal and vertical scaling experiments

INPUT="hdfs://g11-master:9000/nyc_taxi_data/weak_3GB/*.parquet"
OUTPUT_BASE="hdfs://g11-master:9000/output_results/scaling"
FORMAT="parquet"
MASTER="spark://g11-master:7077"

echo "=== SCALABILITY BENCHMARK ==="

# Create the logs directory if it doesn't exist
mkdir -p logs

echo "====================================="
echo " EXPERIMENT 1: HORIZONTAL SCALING    "
echo " (Fixed: 2 Cores/Worker, Varying Nodes)"
echo "====================================="

for WORKERS in 1 2 3; do
    echo "--- Running Horizontal: $WORKERS worker(s) ---"
    
    # Calculate total cores needed across the cluster
    TOTAL_CORES=$((WORKERS * 2))
    
    spark-submit \
        --master $MASTER \
        --executor-memory 2g \
        --executor-cores 2 \
        --total-executor-cores $TOTAL_CORES \
        src/analysis_job.py \
            --input  $INPUT \
            --output $OUTPUT_BASE/horizontal_${WORKERS}workers \
            --format $FORMAT \
            --task   all \
            --cores  2 \
            --memory 2g \
        2>&1 | tee logs/horizontal_${WORKERS}workers.log
done

echo "====================================="
echo " EXPERIMENT 2: VERTICAL SCALING      "
echo " (Fixed: 1 Worker, Varying Cores)    "
echo "====================================="

# Loop through 1, 2, 3, and 4 cores
for CORES in 1 2 3 4; do
    echo "--- Running Vertical: 1 worker with $CORES core(s) ---"
    
    spark-submit \
        --master $MASTER \
        --executor-memory 2g \
        --executor-cores $CORES \
        --total-executor-cores $CORES \
        src/analysis_job.py \
            --input  $INPUT \
            --output $OUTPUT_BASE/vertical_${CORES}cores \
            --format $FORMAT \
            --task   all \
            --cores  $CORES \
            --memory 2g \
        2>&1 | tee logs/vertical_${CORES}cores.log
done

echo "=== Done. All Logs saved to logs/ ==="
