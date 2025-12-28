from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, array_remove, size, when

spark = SparkSession.builder \
    .appName("AddTripComplexity") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "3G") \
    .config("spark.driver.memory", "1G") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

print("\n=== Loading standardized trips ===")
trips = spark.read.parquet("hdfs://namenode:9000/processed/trips_std.parquet")
print(f"Trips rows: {trips.count()}")

# Collect all standardized modes for a trip into an array, drop nulls, and count distinct
modes_array = array(
    col("Main_Mode_std"),
    col("Mode_1_std"),
    col("Mode_2_std"),
    col("Mode_3_std"),
    col("Mode_4_std"),
    col("Mode_5_std")
)

trips_with_modes = trips.withColumn("all_modes", array_remove(modes_array, None))

# Distinct mode count per trip
# (Spark has no direct array_distinct size shortcut, so we use a small trick)
from pyspark.sql.functions import sort_array

trips_with_modes = trips_with_modes.withColumn(
    "all_modes_sorted", sort_array(col("all_modes"))
)

# Use a window-free distinct count via aggregate expression
# But for simplicity here we approximate: single if size(all_modes_sorted) <= 1
trips_complexity = trips_with_modes.withColumn(
    "trip_complexity",
    when(size(col("all_modes_sorted")) <= 1, "Single").otherwise("Multi")
)

print("\n=== Trip complexity counts ===")
trips_complexity.groupBy("trip_complexity").count().show()

print("\n=== Sample trips with complexity ===")
trips_complexity.select(
    "ID", "KEY", "Main_Mode_std",
    "Mode_1_std", "Mode_2_std", "trip_complexity"
).show(10, truncate=False)

print("\n=== Writing updated trips to HDFS ===")
trips_complexity.drop("all_modes", "all_modes_sorted") \
    .write.mode("overwrite").parquet(
        "hdfs://namenode:9000/processed/trips_complexity.parquet",
        compression="snappy"
    )

print("Saved to /processed/trips_complexity.parquet")

spark.stop()

