from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, input_file_name, regexp_extract, to_timestamp,
    lit, concat
)
from pyspark.sql.types import DoubleType, StringType

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder \
    .appName("GPSIngestAndTripKey") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "3G") \
    .config("spark.driver.memory", "1G") \
    .config("spark.sql.shuffle.partitions", "300") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

print("\n=== PHASE 1: Load GPS CSVs from HDFS ===")

# 1) Read all GPS CSVs from HDFS
gps_raw = spark.read.csv(
    "hdfs://namenode:9000/raw/netmob/gps/*.csv",
    header=True,
    inferSchema=True
)
print(f"Raw GPS rows: {gps_raw.count()}")

# 2) Extract participant ID from filename (e.g., 10_2978.csv -> 10_2978)
gps_with_id = gps_raw.withColumn(
    "ID",
    regexp_extract(input_file_name(), r"([^/]+)\.csv$", 1)
)

# 3) Basic cleaning and casting
gps_clean = gps_with_id \
    .withColumn("LATITUDE", col("LATITUDE").cast(DoubleType())) \
    .withColumn("LONGITUDE", col("LONGITUDE").cast(DoubleType())) \
    .withColumn("SPEED", col("SPEED").cast(DoubleType())) \
    .withColumn("VALID", col("VALID").cast(StringType()))

# Use LOCAL DATETIME as main timestamp column
gps_clean = gps_clean.withColumn(
    "local_ts",
    to_timestamp(col("LOCAL DATETIME"), "yyyy-MM-dd HH:mm:ss")
)

# 4) Filter invalid rows
gps_clean = gps_clean \
    .filter(col("ID").isNotNull()) \
    .filter(col("local_ts").isNotNull()) \
    .filter(col("LATITUDE").between(-90, 90)) \
    .filter(col("LONGITUDE").between(-180, 180)) \
    .filter(col("SPEED") >= 0)

print(f"Clean GPS rows: {gps_clean.count()}")

# ============================================================
# PHASE 2: Load trips with complexity and timestamps
# ============================================================
print("\n=== PHASE 2: Load trips with complexity ===")

trips = spark.read.parquet("hdfs://namenode:9000/processed/trips_complexity.parquet")
print(f"Trips rows: {trips.count()}")

# Build unified start and end timestamps for each trip
# CAST to string before concatenation
trips_time = trips \
    .withColumn(
        "start_ts",
        to_timestamp(
            concat(col("Date_O").cast("string"), lit(" "), col("Time_O").cast("string")), 
            "yyyy-MM-dd HH:mm:ss"
        )
    ) \
    .withColumn(
        "end_ts",
        to_timestamp(
            concat(col("Date_D").cast("string"), lit(" "), col("Time_D").cast("string")), 
            "yyyy-MM-dd HH:mm:ss"
        )
    ) \
    .filter(col("start_ts").isNotNull()) \
    .filter(col("end_ts").isNotNull())

print(f"Trips with valid timestamps: {trips_time.count()}")

# ============================================================
# PHASE 3: Attach trip KEY to GPS points
# ============================================================
print("\n=== PHASE 3: Attach trip KEY to GPS points ===")

# Create aliases to avoid column ambiguity
gps_aliased = gps_clean.alias("gps")
trips_aliased = trips_time.alias("trips")

# Join condition: same ID and GPS time between trip start and end
join_cond = (
    (col("gps.ID") == col("trips.ID")) &
    (col("gps.local_ts") >= col("trips.start_ts")) &
    (col("gps.local_ts") <= col("trips.end_ts"))
)

# Join first, then select needed columns
gps_with_trip = gps_aliased.join(
    trips_aliased,
    on=join_cond,
    how="left"
).select(
    col("gps.ID").alias("ID"),
    col("gps.local_ts").alias("local_ts"),
    col("gps.LATITUDE").alias("LATITUDE"),
    col("gps.LONGITUDE").alias("LONGITUDE"),
    col("gps.SPEED").alias("SPEED"),
    col("gps.VALID").alias("VALID"),
    col("trips.KEY").alias("KEY"),
    col("trips.trip_complexity").alias("trip_complexity"),
    col("trips.Main_Mode_std").alias("Main_Mode_std")
)

total = gps_with_trip.count()
assigned = gps_with_trip.filter(col("KEY").isNotNull()).count()

print(f"GPS with trip rows: {total}")
print(f"GPS points with trip KEY: {assigned} / {total} ({assigned / total * 100:.1f}%)")

# ============================================================
# PHASE 4: Save results as Parquet
# ============================================================
print("\n=== PHASE 4: Save results ===")

gps_final = gps_with_trip.select(
    col("ID"),
    col("KEY").alias("tripkey"),
    col("local_ts"),
    col("LATITUDE").alias("lat"),
    col("LONGITUDE").alias("lon"),
    col("SPEED").alias("speed"),
    col("VALID").alias("valid"),
    col("trip_complexity"),
    col("Main_Mode_std")
)

gps_final.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/processed/gps_with_trip.parquet",
    compression="snappy"
)

print("Saved to /processed/gps_with_trip.parquet")

spark.stop()
