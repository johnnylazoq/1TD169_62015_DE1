#!/bin/bash
# Script to run the 3 scaling tests automatically

# scripts/run_benchmark.sh
#!/bin/bash
# Automates the 1/2/3 worker scaling experiments

INPUT="hdfs:///data/your_dataset"
OUTPUT_BASE="hdfs:///results"
FORMAT="csv"
MASTER="spark://master-node:7077"

echo "=== SCALABILITY BENCHMARK ==="

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
            --cores  2 \
            --memory 2g \
        2>&1 | tee logs/run_${WORKERS}workers.log
done

echo "=== Done. Logs saved to logs/ ==="
