import subprocess
import sys

# --- Configuration ---
SPARK_MASTER = "spark://g11-master:7077"
PYTHON_SCRIPT = "src/analysis_job.py"
INPUT_PATH = "hdfs://g11-master:9000/nyc_taxi_data/weak_3GB/*.parquet"
OUTPUT_PATH = "hdfs://g11-master:9000/user/ubuntu/analysis_output"

# Cluster Resource Configuration
EXECUTOR_MEMORY = "4G"
TOTAL_EXECUTOR_CORES = "4"

# Job Application Arguments (passed to analysis_job.py)
JOB_CORES = "4"
JOB_MEMORY = "4g"

def main():
    """
    Constructs and executes the spark-submit command using subprocess.
    """
    
    # 1. build the command string as a list of arguments
    #    This avoids shell injection issues and is safer than a long string
    cmd = [
        "spark-submit",
        "--master", SPARK_MASTER,
        "--executor-memory", EXECUTOR_MEMORY,
        "--total-executor-cores", TOTAL_EXECUTOR_CORES,
        PYTHON_SCRIPT,
        "--input", INPUT_PATH,
        "--output", OUTPUT_PATH,
        "--format", "parquet",
        "--cores", JOB_CORES,
        "--memory", JOB_MEMORY
    ]

    print("=" * 60)
    print("Launching Spark Job...")
    print(f"Command:\n{' '.join(cmd)}")
    print("=" * 60)

    try:
        # 2. Run the command and wait for it to finish.
        #    check=True raises CalledProcessError if exit code is non-zero
        subprocess.run(cmd, check=True)
        print("\n✅ Job execution completed successfully.")
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Job failed with exit code: {e.returncode}")
        sys.exit(e.returncode)
    except KeyboardInterrupt:
        print("\n⚠️ Job interrupted by user.")
        sys.exit(130)
    except FileNotFoundError:
        print("\n❌ Error: 'spark-submit' command not found. Is Spark installed and in your PATH?")
        sys.exit(1)

if __name__ == "__main__":
    main()
