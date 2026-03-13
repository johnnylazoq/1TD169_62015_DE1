# Setup instructions & Architecture diagram

## Project Overview

This project implements a Data Engineering pipeline designed to process and analyze large datasets. It is structured to support ETL (Extract, Transform, Load) processes and data analysis tasks, potentially utilizing distributed computing frameworks like Spark or MapReduce.

The project provides hands-on experience with real-world data engineering challenges by designing
and implementing a scalable data processing solution. Working in teams, you will select a dataset,
architect a distributed system, and demonstrate its scalability through rigorous experimentation.

## Learning Outcomes

- ✓ Apply course concepts (cloud, Hadoop, Spark) to a real problem
- ✓ Design horizontally and/or vertically scalable data pipelines
- ✓ Work collaboratively using Git and modern development practices
- ✓ Conduct systematic scalability experiments
- ✓ Communicate technical solutions through writing and presentations
- ✓ Make architectural trade-off decisions under constraints

## Analysis Objective

Define a simple but realistic data processing task. Examples:

**Good objectives:**
- Filter social media data by location and aggregate counts
- Extract features from log files for anomaly detection
- Compute user activity patterns from click streams
- Process sensor data to identify threshold violations
- Join datasets and compute summary statistics

## Component Options

- **Storage**: HDFS (Primary), Local FS (for staging)
- **Processing**: Apache Spark (PySpark recommended)
- **Orchestration**: Bash scripts, Makefiles
- **Monitoring**: Spark History Server or htop

## Repository Structure

The repository is organized as follows:

- **src/**: Contains the main application source code.
  - `etl_job.py`: Handles data cleaning and ingestion logic.
  - `analysis_job.py`: Contains the main logic for data analysis (e.g., Spark/MapReduce jobs).
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

```bash
pip install -r requirements.txt
```

### Getting Sample Data

The project expects data to be located in the `data/` directory.

1.  **Download Data**: Download the sample dataset from [Your Data Source URL] or use a provided script if available.
2.  **Place Data**: enhance the data by placing it into the `data/` folder.
    - Example: `data/sample_data.csv`
3.  **Verify**: Ensure the data file is accessible before running the ETL job.

### Running the Jobs

1.  **ETL Job**: Run the ETL process to ingest and clean data.
    ```bash
    python src/etl_job.py
    ```

2.  **Analysis Job**: Execute the analysis on the processed data.
    ```bash
    python src/analysis_job.py
    ```

3.  **Benchmarking**: Use the provided script to run scaling tests.
    ```bash
    ./scripts/run_benchmark.sh
    ```

## Deployment

To deploy the application to a cluster, ensure the script is executable and run it:
```bash
chmod +x scripts/deploy.sh
./scripts/deploy.sh
```

