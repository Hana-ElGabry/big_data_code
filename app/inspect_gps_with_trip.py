from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("InspectGPSWithTrip") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "3G") \
    .config("spark.driver.memory", "1G") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

df = spark.read.parquet("hdfs://namenode:9000/processed/gps_with_trip.parquet")

print("\n=== Schema ===")
df.printSchema()

print("\n=== Dimensions ===")
print(f"Rows: {df.count()}")
print(f"Columns: {len(df.columns)}")

print("\n=== Sample rows ===")
df.select("ID", "tripkey", "local_ts", "lat", "lon", "speed",
          "trip_complexity", "Main_Mode_std").show(10, truncate=False)

spark.stop()
