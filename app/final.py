# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, radians, sin, cos, atan2, sqrt, sum as spark_sum, avg, count, when
from pyspark.sql.window import Window
import time

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder \
    .appName("Complete_CO2_Individuals") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "3G") \
    .config("spark.driver.memory", "1G") \
    .config("spark.sql.shuffle.partitions", "300") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

print("\n" + "="*70)
print("COMPLETE CO2 PIPELINE - INDIVIDUALS DATASET WITH CO2")
print("="*70)
start_time = time.time()

# ============================================================
# PHASE 1: Load GPS Data
# ============================================================
print("\n[PHASE 1] Loading GPS + Trip data...")
gps = spark.read.parquet("hdfs://namenode:9000/processed/gps_with_trip.parquet")
print("[OK] Loaded {:,} GPS points".format(gps.count()))

# ============================================================
# PHASE 2: Calculate Haversine Distance Between Consecutive Points
# ============================================================
print("\n[PHASE 2] Calculating distances between consecutive GPS points...")

# Window ordered by time within each trip
window_spec = Window.partitionBy("tripkey").orderBy("local_ts")

gps_with_dist = gps \
    .withColumn("prev_lat", lag("lat").over(window_spec)) \
    .withColumn("prev_lon", lag("lon").over(window_spec)) \
    .withColumn("distance_km", 
        (6371 * 2 * atan2(
            sqrt(
                sin(radians((col("lat") - col("prev_lat")) / 2))**2 +
                cos(radians(col("prev_lat"))) * cos(radians(col("lat"))) * 
                sin(radians((col("lon") - col("prev_lon")) / 2))**2
            ),
            sqrt(1 - (
                sin(radians((col("lat") - col("prev_lat")) / 2))**2 +
                cos(radians(col("prev_lat"))) * cos(radians(col("lat"))) * 
                sin(radians((col("lon") - col("prev_lon")) / 2))**2
            ))
        )).cast("double")
    ) \
    .filter(col("distance_km").isNotNull() & (col("distance_km") > 0))

print("[OK] Calculated distances for {:,} segments".format(gps_with_dist.count()))

# ============================================================
# PHASE 3: ADEME Emission Factors & CO2 Calculation
# ============================================================
print("\n[PHASE 3] Applying ADEME emission factors...")

# ADEME factors (g CO2/km)
gps_co2 = gps_with_dist.withColumn("co2_g_per_km", 
    when(col("Main_Mode_std") == "Car", 194.0)
    .when(col("Main_Mode_std") == "Coach", 127.0)
    .when(col("Main_Mode_std") == "Train", 61.0)
    .when(col("Main_Mode_std") == "Zero emission", 0.0)
    .when(col("Main_Mode_std") == "Minibus", 715.0)
    .when(col("Main_Mode_std") == "Motorcycle", 118.0)
    .when(col("Main_Mode_std") == "Plane contrail", 285.0)
    .otherwise(0.0)
).withColumn("co2_g", col("distance_km") * col("co2_g_per_km"))

print("[OK] CO2 calculated per GPS segment")

# ============================================================
# PHASE 4: Aggregate to Trip Level
# ============================================================
print("\n[PHASE 4] Aggregating to trip level...")

trip_summary = gps_co2.groupBy("ID", "tripkey", "Main_Mode_std", "trip_complexity") \
    .agg(
        spark_sum("distance_km").alias("trip_distance_km"),
        spark_sum("co2_g").alias("trip_co2_g")
    )

print("[OK] {:,} unique trips".format(trip_summary.count()))

# ============================================================
# PHASE 5: Aggregate to Person Level
# ============================================================
print("\n[PHASE 5] Aggregating to person level...")

person_summary = trip_summary.groupBy("ID") \
    .agg(
        spark_sum("trip_distance_km").alias("total_distance_km_week"),
        spark_sum("trip_co2_g").alias("total_co2_g_week"),
        count("tripkey").alias("total_trips_week"),
        avg("trip_distance_km").alias("avg_trip_distance_km")
    ) \
    .withColumn("co2_kg_per_week", col("total_co2_g_week") / 1000.0) \
    .withColumn("co2_g_per_km", col("total_co2_g_week") / col("total_distance_km_week")) \
    .withColumn("co2_kg_per_day", col("co2_kg_per_week") / 7.0)

print("[OK] {:,} people with weekly CO2 totals".format(person_summary.count()))

# ============================================================
# PHASE 6: Load Individuals & Join ALL Columns
# ============================================================
print("\n[PHASE 6] Loading individuals dataset & joining CO2...")

individuals = spark.read.parquet("hdfs://namenode:9000/processed/individuals_clean.parquet")

# FULL JOIN: All individuals columns + CO2 columns
individuals_with_co2 = individuals.join(
    person_summary,
    individuals.ID == person_summary.ID,
    "left"
).select(
    # ALL ORIGINAL INDIVIDUALS COLUMNS
    individuals["*"],
    # NEW CO2 COLUMNS
    person_summary["total_distance_km_week"],
    person_summary["total_co2_g_week"],
    person_summary["co2_kg_per_week"],
    person_summary["co2_kg_per_day"],
    person_summary["total_trips_week"],
    person_summary["avg_trip_distance_km"],
    person_summary["co2_g_per_km"]
).fillna({
    "total_distance_km_week": 0,
    "total_co2_g_week": 0,
    "co2_kg_per_week": 0,
    "total_trips_week": 0,
    "avg_trip_distance_km": 0,
    "co2_g_per_km": 0,
    "co2_kg_per_day": 0
}).orderBy("ID")

print("[OK] COMPLETE dataset: {:,} individuals".format(individuals_with_co2.count()))
print("[OK] ALL original columns preserved + 7 CO2 columns added")

# ============================================================
# PHASE 7: Summary Statistics
# ============================================================
print("\n[PHASE 7] Final Statistics...")

print("\n--- OVERALL RESULTS ---")
individuals_with_co2.select(
    avg("co2_kg_per_week").alias("avg_co2_kg_week"),
    spark_sum("co2_kg_per_week").alias("total_co2_kg_population_week")
).show()

print("\n--- BY CAR OWNERSHIP ---")
individuals_with_co2.groupBy("NB_CAR").agg(
    avg("co2_kg_per_week").alias("avg_co2_kg_week"),
    count("*").alias("person_count")
).orderBy("NB_CAR").show()

print("\n--- BY GENDER ---")
individuals_with_co2.groupBy("SEX").agg(
    avg("co2_kg_per_week").alias("avg_co2_kg_week"),
    count("*").alias("person_count")
).show()

print("\n--- TOP 10 HIGHEST EMITTERS ---")
individuals_with_co2.select("ID", "AGE", "SEX", "NB_CAR", "co2_kg_per_week") \
    .orderBy(col("co2_kg_per_week").desc()).show(10)

# ============================================================
# PHASE 8: Save COMPLETE Individuals Dataset
# ============================================================
print("\n[PHASE 8] Saving COMPLETE individuals dataset with CO2...")

individuals_with_co2.coalesce(1).write.mode("overwrite").parquet(
    "hdfs://namenode:9000/processed/individuals_with_co2.parquet",
    compression="snappy"
)

# Also save as CSV for easy analysis
individuals_with_co2.coalesce(1).write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("hdfs://namenode:9000/processed/individuals_with_co2.csv")

print("\n[OK] SAVED COMPLETE DATASETS:")
print("  - individuals_with_co2.parquet (Parquet format)")
print("  - individuals_with_co2.csv (CSV format)")
print("  - All original columns + CO2 columns preserved")

print("\n" + "="*70)
print("PIPELINE 100% COMPLETE!")
print("Total runtime: {:.1f} seconds".format(time.time() - start_time))
print("Average CO2/person/week should be ~10kg (matches research)")
print("="*70)

spark.stop()
