from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, to_timestamp, lit
from pyspark.sql.types import IntegerType, DoubleType, BooleanType
import time

# ============================================================
# SPARK SESSION CONFIGURATION (12GB Memory Budget)
# ============================================================
spark = SparkSession.builder \
    .appName("NetMob25_ETL_Research_Replication") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "3G") \
    .config("spark.driver.memory", "1G") \
    .config("spark.sql.shuffle.partitions", "300") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

print("=" * 60)
print("NetMob25 CO2 Research Pipeline - PySpark Replication")
print(f"Spark Version: {spark.version}")
print("=" * 60)

start_time = time.time()

# ============================================================
# PHASE 1: LOAD RAW DATA FROM HDFS
# ============================================================
print("\n[PHASE 1] Loading raw datasets from HDFS...")

individuals_raw = spark.read.csv(
    "hdfs://namenode:9000/raw/netmob/individuals_dataset.csv",
    header=True,
    inferSchema=True
)

trips_raw = spark.read.csv(
    "hdfs://namenode:9000/raw/netmob/trips_dataset.csv",
    header=True,
    inferSchema=True
)

print(f"✓ Raw Individuals: {individuals_raw.count()} rows")
print(f"✓ Raw Trips: {trips_raw.count()} rows")

# ============================================================
# PHASE 2: FILTER INDIVIDUALS (GPS_RECORD == True)
# ============================================================
print("\n[PHASE 2] Filtering individuals with GPS records...")

# Step 1: Only keep individuals with GPS_RECORD == True
valid_individuals = individuals_raw \
    .filter(col("GPS_RECORD") == "True") \
    .filter(col("ID").isNotNull())

initial_individuals = individuals_raw.count()
valid_count = valid_individuals.count()

print(f"✓ Original individuals: {initial_individuals}")
print(f"✓ Individuals with GPS records: {valid_count}")
print(f"✓ Removed: {initial_individuals - valid_count} ({((initial_individuals - valid_count) / initial_individuals * 100):.1f}%)")

# Extract valid IDs for trip filtering
valid_ids = valid_individuals.select("ID").distinct()

# ============================================================
# PHASE 3: FILTER TRIPS (Replicate Pandas Pipeline)
# ============================================================
print("\n[PHASE 3] Filtering trips (replicating research pipeline)...")

initial_trips = trips_raw.count()
print(f"✓ Original trips: {initial_trips}")

# Step 1: Remove NoTrip and NoTraces
trips_filtered = trips_raw \
    .filter(~col("ID_Trip_Days").isin(["NoTrip", "NoTraces"]))
    
step1_count = trips_filtered.count()
print(f"✓ After removing NoTrip/NoTraces: {step1_count} (removed {initial_trips - step1_count})")

# Step 2: Drop rows with missing Main_Mode
trips_filtered = trips_filtered \
    .filter(col("Main_Mode").isNotNull())
    
step2_count = trips_filtered.count()
print(f"✓ After dropping missing Main_Mode: {step2_count} (removed {step1_count - step2_count})")

# Step 3: Remove trips with missing start or end time
trips_filtered = trips_filtered \
    .filter(col("Time_O").isNotNull()) \
    .filter(col("Time_D").isNotNull())
    
step3_count = trips_filtered.count()
print(f"✓ After removing missing timestamps: {step3_count} (removed {step2_count - step3_count})")

# Step 4: Filter trips to only include those from valid GPS individuals
trips_clean = trips_filtered \
    .join(valid_ids, on="ID", how="inner")
    
final_trips = trips_clean.count()
print(f"✓ After matching with GPS individuals: {final_trips} (removed {step3_count - final_trips})")
print(f"✓ Overall retention: {(final_trips / initial_trips * 100):.1f}%")

# ============================================================
# PHASE 4: DATA CLEANING & TYPE CASTING
# ============================================================
print("\n[PHASE 4] Cleaning and casting data types...")

# Clean Individuals
individuals_clean = valid_individuals \
    .withColumn("AGE", col("AGE").cast(IntegerType())) \
    .withColumn("NB_CAR", col("NB_CAR").cast(IntegerType())) \
    .withColumn("NBPERS_HOUSE", col("NBPERS_HOUSE").cast(IntegerType())) \
    .withColumn("WEIGHT_INDIV", col("WEIGHT_INDIV").cast(DoubleType())) \
    .fillna({
        "NB_CAR": 0,
        "DRIVING_LICENCE": "Unknown",
        "NAVIGO_SUB": "No"
    })

# Clean Trips
trips_clean = trips_clean \
    .withColumn("Duration", col("Duration").cast(IntegerType())) \
    .withColumn("Weight_Day", col("Weight_Day").cast(DoubleType())) \
    .withColumn("Time_O", trim(col("Time_O"))) \
    .withColumn("Time_D", trim(col("Time_D"))) \
    .fillna({"Duration": 0})

print(f"✓ Individuals cleaned: {individuals_clean.count()} rows")
print(f"✓ Trips cleaned: {trips_clean.count()} rows")

# ============================================================
# PHASE 5: WRITE TO PARQUET (SNAPPY COMPRESSION)
# ============================================================
print("\n[PHASE 5] Writing cleaned data to HDFS (Parquet format)...")

individuals_clean.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/processed/individuals_clean.parquet",
    compression="snappy"
)
print("✓ Individuals saved to /processed/individuals_clean.parquet")

trips_clean.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/processed/trips_clean.parquet",
    compression="snappy"
)
print("✓ Trips saved to /processed/trips_clean.parquet")

# ============================================================
# PHASE 6: VALIDATION & SUMMARY STATISTICS
# ============================================================
print("\n[PHASE 6] Validation & Summary...")

# Read back to verify
individuals_final = spark.read.parquet("hdfs://namenode:9000/processed/individuals_clean.parquet")
trips_final = spark.read.parquet("hdfs://namenode:9000/processed/trips_clean.parquet")

print(f"✓ Final Individuals: {individuals_final.count()} rows")
print(f"✓ Final Trips: {trips_final.count()} rows")

# Show sample individuals
print("\n--- Sample Individuals (with GPS) ---")
individuals_final.select("ID", "AGE", "SEX", "NB_CAR", "NAVIGO_SUB", "GPS_RECORD").show(5, truncate=False)

# Show sample trips
print("\n--- Sample Trips (filtered & cleaned) ---")
trips_final.select("ID", "Main_Mode", "Duration", "Area_O", "Area_D").show(5, truncate=False)

# Mode distribution
print("\n--- Transport Mode Distribution ---")
trips_final.groupBy("Main_Mode").count().orderBy(col("count").desc()).show(10)

# ============================================================
# EXECUTION METRICS
# ============================================================
end_time = time.time()
execution_time = end_time - start_time

print("\n" + "=" * 60)
print("ETL PIPELINE COMPLETED (Research Replication)")
print(f"Total Execution Time: {execution_time:.2f} seconds")
print(f"Data Retention: {(final_trips / initial_trips * 100):.1f}% of trips")
print("=" * 60)

spark.stop()
