SPARK_MASTER = spark://g11-master:7077
PYTHON_FILE = src/analysis_job.py
INPUT_PATH = hdfs://g11-master:9000/nyc_taxi_data/weak_3GB/*.parquet
OUTPUT_PATH = hdfs://g11-master:9000/user/ubuntu/analysis_output
#DOne by Chendana
# Spark execution resources
EXECUTOR_MEMORY = 4G
TOTAL_CORES = 4

# Script arguments
SCRIPT_CORES = 4
SCRIPT_MEMORY = 4g

.PHONY: run clean help

help:
	@echo "Usage:"
	@echo "  make run    - Run the analysis job on Spark"
	@echo "  make clean  - Remove the output directory from HDFS"

run:
	spark-submit \
		--master $(SPARK_MASTER) \
		--executor-memory $(EXECUTOR_MEMORY) \
		--total-executor-cores $(TOTAL_CORES) \
		$(PYTHON_FILE) \
		--input "$(INPUT_PATH)" \
		--output "$(OUTPUT_PATH)" \
		--format parquet \
		--cores $(SCRIPT_CORES) \
		--memory $(SCRIPT_MEMORY)

clean:
	hdfs dfs -rm -r -f $(OUTPUT_PATH)
