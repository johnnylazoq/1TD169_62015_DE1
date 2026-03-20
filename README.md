## Done by Neha
# Setup instructions & Architecture diagram

## Project Overview

This project implements a Data Engineering pipeline designed to process and analyze large datasets. It is structured to support ETL (Extract, Transform, Load) processes and data analysis tasks, potentially utilizing distributed computing frameworks like Spark or MapReduce.

Specifically, this repository features a high-performance analytical engine built with **PySpark** that processes, transforms, and analyzes large-scale NYC Taxi datasets (up to 3GB / ~40 million records) executing on an Apache Spark standalone cluster.

## Architecture & Core Analytical Modules

The analytical engine uses a dynamic routing pattern, allowing individual modules or the entire suite to be executed via command-line arguments.

1. **Spatio-Temporal Dispatch:** Identifies daily peak traffic hours and maps ride volume distributions across different dispatching bases.
2. **Economic Performance (Monthly Revenue):** Aggregates total fares and tips to calculate monthly gross revenue.
3. **Driver Compensation Breakdown:** Evaluates the relationship between passenger fares, actual driver pay, and regulatory levies across platforms.
4. **Geographic Zone Analysis:** Processes congestion surcharge data to identify geographic zones with the highest fee accumulations.
5. **Anomalous Rides (Outliers):** Extracts trip duration and speed features to identify data errors, unusually long trips, or impossible travel speeds.

## Repository Structure

The repository is organized as follows:

- **src/**: Contains the main application source code.
  - `etl_job.py`: Handles data cleaning and ingestion logic.
  - `analysis_job.py`: Contains the main logic for data analysis (e.g., Spark/MapReduce jobs) and the PySpark execution router.
  - `config.py`: Stores configuration settings, paths, and constants.

- **scripts/**: Shell scripts for automation.
  - `deploy.sh`: Helper script to deploy code to the cluster.
  - `run_benchmark.sh`: Automates scaling tests and performance benchmarking.

- **infrastructure/**: Setup and configuration for infrastructure (e.g., Terraform, Docker).

- **data/**: Directory for local data storage or sample datasets.

- **notebooks/**: Jupyter notebooks for data exploration and prototyping.

- **docs/**: Documentation and additional resources.

## Getting Started

### Prerequisites

Ensure you have the necessary environment set up, including Python and any required libraries specified in the project.

Install the required Python packages:

`pip install -r requirements.txt`

### Getting Sample Data

The project expects data to be located in the `data/` directory or hosted on a Hadoop Distributed File System (HDFS).

1.  **Download Data**: Download the sample dataset from your data source or use a provided script if available.
2.  **Place Data**: enhance the data by placing it into the `data/` folder or uploading it to HDFS.
    - Example Local: `data/sample_data.csv`
    - Example HDFS: `hdfs://g11-master:9000/nyc_taxi_data/`
3.  **Verify**: Ensure the data file is accessible before running the ETL job.

### Running the Jobs

1.  **ETL Job**: Run the ETL process to ingest and clean data.
    
    `python src/etl_job.py`

2.  **Analysis Job (Spark Submission)**: Execute the analysis on the processed data. You can specify individual tasks (`dispatch`, `revenue`, `compensation`, `zones`, `outliers`) or run the entire suite (`all`).
    
    ```bash
    spark-submit --master spark://g11-master:7077 \
      src/analysis_job.py \
      --input hdfs://g11-master:9000/nyc_taxi_data/weak_3GB/*.parquet \
      --output hdfs://g11-master:9000/output_results \
      --format parquet \
      --task all \
      --cores 4 \
      --memory 6g
    ```

3.  **Benchmarking**: Use the provided script to run scaling tests.
    
    `./scripts/run_benchmark.sh`

## Performance Benchmarks

The PySpark pipeline is highly optimized using the Catalyst Optimizer and efficient shuffle partition configurations.

**Latest Benchmark (Task: ALL):**
- **Cluster:** 1 Master, 3 Workers (4 Cores/node, 6GB Memory)
- **Data Volume:** 3GB Parquet (39,745,127 records)
- **Execution Time:** 32.93 seconds

## deployment

To deploy the application to a cluster, use the deployment script:

`./scripts/deploy.sh`