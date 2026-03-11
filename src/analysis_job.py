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
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, month, sum as _sum, avg

def main():
    if len(sys.argv) != 2:
        print("Usage: spark-submit taxi_analysis.py <hdfs_data_path>")
        sys.exit(1)
        
    data_path = sys.argv[1]

    spark = SparkSession.builder \
        .appName("NYC_Taxi_Scalability_Analysis") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print(f"Loading data from: {data_path}")
    
    # ==========================================
    # START TIMER
    # ==========================================
    start_time = time.time()

    df = spark.read.parquet(data_path)  ##PARQUET
    
    # ------------------------------------------
    # OBJECTIVE 1: Spatio-Temporal Dispatch Analysis
    # ------------------------------------------
    print("\n[1/6] Executing Spatio-Temporal Dispatch Analysis...")
    peak_hours = df.groupBy(hour("pickup_datetime").alias("hour"), "hvfhs_license_num") \
                   .count() \
                   .orderBy("hour", ascending=False)
    peak_hours.show(5)

    # ------------------------------------------
    # OBJECTIVE 2: Economic Performance (Monthly Revenue)
    # ------------------------------------------
    print("[2/6] Executing Monthly Revenue Analysis...")
    # Aggregating fares and tips to calculate gross revenue by month
    monthly_revenue = df.withColumn("month", month("pickup_datetime")) \
                        .groupBy("month") \
                        .agg(
                            _sum(col("base_passenger_fare") + col("tips")).alias("monthly_gross_revenue")
                        ).orderBy("month")
    monthly_revenue.show(5)

    # ------------------------------------------
    # OBJECTIVE 3: Economic Fare Breakdown & Compensation
    # ------------------------------------------
    print("[3/6] Executing Fare Breakdown & Driver Compensation...")
    # Analyzing platform cuts and the impact of congestion surcharges
    # Note: Ensure 'cbd_congestion_fee' exists in your schema or handle its absence
    compensation = df.filter(col("base_passenger_fare") > 0) \
                     .groupBy("hvfhs_license_num") \
                     .agg(
                         _sum("base_passenger_fare").alias("total_base_fare"),
                         _sum("driver_pay").alias("total_driver_payout"),
                         _sum("congestion_surcharge").alias("total_congestion_levies")
                     )
    compensation.show(5)

    # ------------------------------------------
    # OBJECTIVE 4: Geographic Congestion Mapping
    # ------------------------------------------
    print("[4/6] Executing Geographic Congestion Mapping...")
    # Identifying which Pick-Up Location IDs (zones) accumulate the most congestion fees
    congestion_zones = df.groupBy("PULocationID") \
                         .agg(_sum("congestion_surcharge").alias("total_zone_surcharge")) \
                         .orderBy(col("total_zone_surcharge").desc())
    congestion_zones.show(5)

    # ------------------------------------------
    # OBJECTIVE 5: Trip Origin-Destination User Analysis
    # ------------------------------------------
    print("[5/6] Executing High-Traffic Zone Analysis...")
    # Map trip origins and destinations to reveal high-traffic zones
    traffic_analysis = df.groupBy("PULocationID", "DOLocationID") \
                         .count() \
                         .orderBy(col("count").desc())
    traffic_analysis.show(5)

    # ------------------------------------------
    # OBJECTIVE 6: Outlier Detection via Spark SQL
    # ------------------------------------------
    print("[6/6] Executing Spark SQL Anomaly Detection...")
    df.createOrReplaceTempView("taxi_data")
    outlier_query = """
        SELECT hvfhs_license_num, pickup_datetime, trip_miles, trip_time, base_passenger_fare, driver_pay
        FROM taxi_data
        WHERE base_passenger_fare < 0 
           OR trip_time > 18000 
           OR (trip_miles = 0 AND base_passenger_fare > 50)
    """


    outliers = spark.sql(outlier_query)
    outliers.show(5)


    # ==========================================
    # STOP TIMER
    # ==========================================
    end_time = time.time()
    execution_time = end_time - start_time

    print("\n==========================================")
    print(f"EXPERIMENT COMPLETE")
    print(f"TOTAL EXECUTION TIME: {execution_time:.2f} seconds")
    print("==========================================\n")

    spark.stop()

if __name__ == "__main__":
    main()