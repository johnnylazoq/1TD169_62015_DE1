# Main Spark/MapReduce logic
# src/analysis_job.py
import time
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(app_name="DataEngProject", executor_memory="2g", cores=2):
    return (SparkSession.builder
        .appName(app_name)
        .config("spark.executor.memory", executor_memory)
        .config("spark.executor.cores", str(cores))
        .config("spark.sql.shuffle.partitions", "100")
        .config("spark.eventLog.enabled", "true")
        .getOrCreate())

def load_data(spark, input_path, file_format="csv"):
    logger.info(f"Loading data from: {input_path}")
    if file_format == "csv":
        return spark.read.csv(input_path, header=True, inferSchema=True)
    elif file_format == "json":
        return spark.read.json(input_path)
    elif file_format == "parquet":
        return spark.read.parquet(input_path)
    else:
        raise ValueError(f"Unsupported format: {file_format}")

def run_analysis(df):
    """
    Core analysis logic — adapt this to your dataset.
    Example: NYC Taxi — avg fare by hour and passenger count.
    """
    # HVFHV Schema adaptation: 
    # - tpep_pickup_datetime -> pickup_datetime
    # - fare_amount -> base_passenger_fare
    # - trip_distance -> trip_miles
    # - passenger_count does not exist in standard HVFHV data
    return (df
        .filter(F.col("base_passenger_fare") > 0)
        .withColumn("hour", F.hour(F.col("pickup_datetime")))
        .groupBy("hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("base_passenger_fare").alias("avg_fare"),
            F.avg("trip_miles").alias("avg_distance")
        )
        .orderBy("hour"))

def convert_to_parquet(spark, input_path, output_path):
    """Optional: Convert CSV/JSON to Parquet for faster subsequent reads."""
    logger.info("Converting to Parquet...")
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df.write.mode("overwrite").parquet(output_path)
    logger.info(f"Saved Parquet to {output_path}")

def benchmark(spark, input_path, output_path, file_format):
    start = time.time()

    df = load_data(spark, input_path, file_format)
    record_count = df.count()
    logger.info(f"Loaded {record_count:,} records")

    result = run_analysis(df)
    result.write.mode("overwrite").csv(output_path, header=True)

    elapsed = time.time() - start
    logger.info(f"Job completed in {elapsed:.2f} seconds")
    logger.info(f"Records processed: {record_count:,}")
    return elapsed, record_count

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",   required=True,  help="HDFS input path")
    parser.add_argument("--output",  required=True,  help="HDFS output path")
    parser.add_argument("--format",  default="csv",  help="csv | json | parquet")
    parser.add_argument("--cores",   default=2, type=int)
    parser.add_argument("--memory",  default="2g")
    args = parser.parse_args()

    spark = create_spark_session(executor_memory=args.memory, cores=args.cores)
    elapsed, count = benchmark(spark, args.input, args.output, args.format)

    # Print summary for easy log scraping
    print(f"\n{'='*40}")
    print(f"BENCHMARK RESULT")
    print(f"  Time      : {elapsed:.2f}s")
    print(f"  Records   : {count:,}")
    print(f"  Cores/node: {args.cores}")
    print(f"{'='*40}\n")
    spark.stop()