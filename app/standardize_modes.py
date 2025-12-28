from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder \
    .appName("StandardizeTransportModes") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "3G") \
    .config("spark.driver.memory", "1G") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

print("\n=== Loading cleaned trips ===")
trips = spark.read.parquet("hdfs://namenode:9000/processed/trips_clean.parquet")
print(f"Trips rows: {trips.count()}")

def standardize(col_expr):
    return when(col_expr.isNull(), None) \
        .when(col_expr.isin("PRIV_CAR_DRIVER", "PRIV_CAR_PASSENGER", "TAXI", "LIGHT_COMM_VEHICLE"), "Car") \
        .when(col_expr.isin("BUS", "TRAMWAY"), "Coach") \
        .when(col_expr == "ON_DEMAND", "Minibus") \
        .when(col_expr == "TWO_WHEELER", "Motorcycle") \
        .when(col_expr.isin("TRAIN", "TRAIN_EXPRESS", "SUBWAY"), "Train") \
        .when(col_expr == "PLANE", "Plane contrail") \
        .when(col_expr.isin("WALKING", "BIKE", "ELECT_BIKE", "ELECT_SCOOTER"), "Zero emission") \
        .otherwise(None)

print("\n=== Adding standardized mode columns ===")
trips_std = trips \
    .withColumn("Main_Mode_std", standardize(col("Main_Mode"))) \
    .withColumn("Mode_1_std", standardize(col("Mode_1"))) \
    .withColumn("Mode_2_std", standardize(col("Mode_2"))) \
    .withColumn("Mode_3_std", standardize(col("Mode_3"))) \
    .withColumn("Mode_4_std", standardize(col("Mode_4"))) \
    .withColumn("Mode_5_std", standardize(col("Mode_5")))

print("Sample standardized modes:")
trips_std.select("Main_Mode", "Main_Mode_std", "Mode_1", "Mode_1_std").show(10, truncate=False)

print("\n=== Writing standardized trips back to HDFS ===")
trips_std.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/processed/trips_std.parquet",
    compression="snappy"
)

print("Saved to /processed/trips_std.parquet")

spark.stop()
