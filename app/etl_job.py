from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, broadcast, acos, cos, sin, radians
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def main():
    # 1. Initialize Spark with Tuned Configs for 12GB RAM
    spark = SparkSession.builder \
        .appName("NetMob_Research_ETL") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "200") \
        .config("spark.memory.fraction", "0.8") \
        .getOrCreate()

    print("🚀 Spark Session Started: Replication of 'Unraveling Urban CO2' Pipeline")

    # --- PHASE 1: PROCESS TRIPS METADATA ---
    print("⏳ Loading and Cleaning Trips Dataset...")
    
    # Define Schema for Trips (Faster than inferring)
    trips_schema = StructType([
        StructField("trip_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("date", StringType(), True),
        StructField("no_trip", IntegerType(), True),  # Flag for non-travel days
        StructField("no_traces", IntegerType(), True), # Flag for missing GPS
        StructField("main_mode", StringType(), True),
        # We need coordinates to filter the 75m radius (Paper Section E)
        StructField("origin_lat", DoubleType(), True),
        StructField("origin_lon", DoubleType(), True),
        StructField("dest_lat", DoubleType(), True),
        StructField("dest_lon", DoubleType(), True)
    ])

    # Read Trips CSV
    trips_df = spark.read.csv(
        "hdfs://namenode:9000/raw/trips_dataset.csv", 
        header=True, 
        schema=trips_schema
    )

    # APPLY PAPER FILTERS (Section B)
    valid_trips_df = trips_df \
        .filter(col("no_trip") == 0) \
        .filter(col("no_traces") == 0) \
        .filter(col("main_mode").isNotNull()) \
        .select("trip_id", "user_id", "main_mode", "origin_lat", "origin_lon", "dest_lat", "dest_lon")

    print(f"✅ Trips cleaned. Valid Trips Count: {valid_trips_df.count()}")

    # --- PHASE 2: PROCESS RAW GPS TRACES ---
    print("⏳ Loading 50M GPS Records (This is the heavy part)...")

    # Define Schema for GPS
    gps_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("valid_signal", StringType(), True), # Quality check column
        StructField("speed", DoubleType(), True),
        StructField("user_id", StringType(), True) # Needed to join with trips
    ])

    # Read all CSVs in the folder at once
    gps_df = spark.read.csv(
        "hdfs://namenode:9000/raw/gps_dataset/", 
        header=True, 
        schema=gps_schema
    )

    # FILTER 1: GPS Quality (Paper Section B)
    # Only keep points where signal is valid (e.g., 'SPS', 'DGPS')
    clean_gps_df = gps_df.filter(col("valid_signal").isin("SPS", "DGPS"))

    # --- PHASE 3: DISTRIBUTED SPATIAL FILTERING ---
    # We must join GPS points to Trips to check the "75m distance" rule.
    # Strategy: BROADCAST the small trips table to all workers.
    
    print("🔄 Joining GPS with Trips (Broadcast Strategy)...")
    
    joined_df = clean_gps_df.join(
        broadcast(valid_trips_df),
        on="user_id",
        how="inner"
    )

    # FILTER 2: Trimming 75m from Origin/Dest (Paper Section E)
    # Haversine Distance Calculation (Earth Radius ~ 6371km)
    # We drop points where dist(point, origin) < 0.075km OR dist(point, dest) < 0.075km
    
    print("📐 Applying 75m Spatial Trimming...")
    
    # Helper for Haversine (simplified for performance)
    def dist_calc(lat1, lon1, lat2, lon2):
        return acos(
            sin(radians(lat1)) * sin(radians(lat2)) + 
            cos(radians(lat1)) * cos(radians(lat2)) * cos(radians(lon2) - radians(lon1))
        ) * 6371

    final_df = joined_df \
        .withColumn("dist_to_origin", dist_calc(col("lat"), col("lon"), col("origin_lat"), col("origin_lon"))) \
        .withColumn("dist_to_dest", dist_calc(col("lat"), col("lon"), col("dest_lat"), col("dest_lon"))) \
        .filter((col("dist_to_origin") > 0.075) & (col("dist_to_dest") > 0.075))

    # --- PHASE 4: SAVE TO PARQUET ---
    print("💾 Writing Cleaned Data to Parquet (Snappy Compressed)...")
    
    final_df.write \
        .mode("overwrite") \
        .partitionBy("main_mode") \
        .parquet("hdfs://namenode:9000/processed/cleaned_mobility_data.parquet")

    print("✅ ETL Pipeline Complete. Data is ready for ML/App.")
    spark.stop()

if __name__ == "__main__":
    main()