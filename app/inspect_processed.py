from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("InspectProcessedNetMob") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "3G") \
    .config("spark.driver.memory", "1G") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

print("\n=== Individuals Sample ===")
ind = spark.read.parquet("hdfs://namenode:9000/processed/individuals_clean.parquet")
ind.printSchema()
ind.select("ID","AGE","SEX","NB_CAR","GPS_RECORD").show(5, truncate=False)

print("\n=== Trips Sample ===")
trips = spark.read.parquet("hdfs://namenode:9000/processed/trips_clean.parquet")
trips.printSchema()
trips.select("ID","Main_Mode","Duration","Area_O","Area_D").show(5, truncate=False)

spark.stop()
