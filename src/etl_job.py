# src/etl_job.py
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_etl(spark, input_path, output_path):
    logger.info(f"Extracting raw data from {input_path}")
    
    # 1. EXTRACT: Read the raw dataset
    # (Assuming the raw data is also parquet or csv, PySpark handles it dynamically here)
    df = spark.read.load(input_path)

    logger.info("Transforming and cleaning data...")
    
    # 2. TRANSFORM: Clean the data (Drop nulls in critical columns, remove negative fares/miles)
    cleaned_df = (df
        .dropna(subset=["pickup_datetime", "dropoff_datetime", "PULocationID", "base_passenger_fare"])
        .filter(F.col("base_passenger_fare") >= 0)
        .filter(F.col("trip_miles") >= 0)
    )

    logger.info(f"Loading cleaned data to {output_path} in Parquet format")
    
    # 3. LOAD: Write to target HDFS path as optimized Parquet partitions
    cleaned_df.write.mode("overwrite").parquet(output_path)
    
    logger.info(f"ETL Job completed successfully. Cleaned records: {cleaned_df.count():,}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Raw data input path on HDFS")
    parser.add_argument("--output", required=True, help="Cleaned Parquet output path on HDFS")
    args = parser.parse_args()

    # We don't need a massive amount of memory just to filter and write
    spark = SparkSession.builder.appName("DataEng_ETL").getOrCreate()
    
    run_etl(spark, args.input, args.output)
    spark.stop()