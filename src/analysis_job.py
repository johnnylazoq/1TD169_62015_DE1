"""
Spatio-Temporal Dispatch Analysis: 
We will identify daily peak traffic hours through hourly aggregation, 
filtering trip records to map ride volume distributions across different dispatching bases. 
This will allow us to visualize how service demand shifts between providers during high-traffic intervals.

Economic Performance (Monthly Revenue): 
We will aggregate total fares, tips to calculate the monthly gross revenue, allowing us to correlate financial performance with seasonal events.

Economic Fare Breakdown and Compensation Analysis: We will analyze the relationship between the base_passenger_fare and actual driver_pay across different High-Volume For-Hire Vehicle (HVFHV) platforms (e.g., Uber and Lyft). By aggregating these financial metrics alongside time and distance data, we aim to evaluate effective platform commission trends and assess how regulatory levies—such as the NYS congestion_surcharge and the 2025 cbd_congestion_fee—impact the final passenger cost versus actual driver earnings.

Process congestion surcharge data to identify geographic zones with the highest fee accumulations. 
(Focuses on processing specific columns to identify threshold/maximum values).

To map trip origins and destinations to reveal high-traffic zones, 
underserved areas, and opportunities for infrastructure improvements.

(Extract trip duration and location features to identify anomalous rides. (Focuses on feature extraction to find outliers, such as unusually long trips or data errors) -Need your feedback on this.
"""
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

# Module 1: Dispatch Analysis
def analyze_dispatch(df):
    """
    Identify daily peak traffic hours through hourly aggregation, 
    mapping ride volume distributions across different dispatching bases.
    """
    logger.info("Running Spatio-Temporal Dispatch Analysis...")
    return (df
        .withColumn("hour", F.hour(F.col("pickup_datetime")))
        .groupBy("hour", "dispatching_base_num")
        .agg(F.count("*").alias("trip_count"))
        .orderBy("hour", ascending=False))

# Module 2: Monthly Revenue
def analyze_revenue(df):
    """
    Aggregate total fares and tips to calculate monthly gross revenue.
    """
    logger.info("Running Economic Performance (Monthly Revenue)...")
    return (df
        .withColumn("month", F.month(F.col("pickup_datetime")))
        .groupBy("month")
        .agg(
            F.sum(F.col("base_passenger_fare") + F.col("tips")).alias("monthly_gross_revenue")
        )
        .orderBy("month"))

# Module 3: Driver Compensation Breakdown
def analyze_compensation(df):
    """
    Evaluate the relationship between passenger fare, driver pay, 
    and regulatory levies across different platforms.
    """
    logger.info("Running Driver Compensation Breakdown Analysis...")
    return (df
        .filter(F.col("base_passenger_fare") > 0)
        .groupBy("hvfhs_license_num")
        .agg(
            F.avg("base_passenger_fare").alias("avg_passenger_fare"),
            F.avg("driver_pay").alias("avg_driver_pay"),
            F.avg("congestion_surcharge").alias("avg_congestion_surcharge"),
            F.avg("bcf").alias("avg_black_car_fund") # Assuming 'bcf' is standard in this dataset; CBD fee might not be present yet depending on dataset year
        )
        .orderBy("hvfhs_license_num"))

# Router & Execution
def run_analysis(df, task="all"):
    """
    Core analysis logic — routes to the specific modular function.
    """
    results = {}
    
    if task in ["dispatch", "all"]:
        results["dispatch_analysis"] = analyze_dispatch(df)
    if task in ["revenue", "all"]:
        results["monthly_revenue"] = analyze_revenue(df)
    if task in ["compensation", "all"]:
        results["driver_compensation"] = analyze_compensation(df)
        
    return results

def convert_to_parquet(spark, input_path, output_path):
    """Optional: Convert CSV/JSON to Parquet for faster subsequent reads."""
    logger.info("Converting to Parquet...")
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df.write.mode("overwrite").parquet(output_path)
    logger.info(f"Saved Parquet to {output_path}")

def benchmark(spark, input_path, output_path, file_format, task):
    start = time.time()

    df = load_data(spark, input_path, file_format)
    record_count = df.count()
    logger.info(f"Loaded {record_count:,} records")

    # result_dfs is now a dictionary of DataFrames
    result_dfs = run_analysis(df, task)
    
    # Save each analysis to its own subfolder within the output path
    for analysis_name, result_df in result_dfs.items():
        specific_output_path = f"{output_path}/{analysis_name}"
        logger.info(f"Writing {analysis_name} to {specific_output_path}")
        result_df.write.mode("overwrite").csv(specific_output_path, header=True)

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
    parser.add_argument("--task",    default="dispatch",  help="dispatch | all")
    args = parser.parse_args()

    spark = create_spark_session(executor_memory=args.memory, cores=args.cores)
    elapsed, count = benchmark(spark, args.input, args.output, args.format, args.task)

    # Print summary for easy log scraping
    print(f"\n{'='*40}")
    print(f"BENCHMARK RESULT")
    print(f"  Task      : {args.task.upper()}")
    print(f"  Time      : {elapsed:.2f}s")
    print(f"  Records   : {count:,}")
    print(f"  Cores/node: {args.cores}")
    print(f"{'='*40}\n")
    spark.stop()
