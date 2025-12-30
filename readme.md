

# 🌍 NetMob Big Data Pipeline: CO2 Emissions Analysis

Complete distributed data pipeline for analyzing urban transport CO2 emissions from GPS trajectories using Apache Spark and HDFS.

---

## 📊 Pipeline Overview

This project processes **47.6 million GPS points** from 3,324 individuals to calculate weekly CO2 emissions based on:
- GPS trajectory distances (Haversine formula)
- Transport mode classification
- ADEME emission factors (French environmental standards)

**Final Output:** CSV file with individual-level CO2 metrics ready for analysis.

---

## 📁 Project Structure & File Descriptions

### **Configuration Files**
- **`docker-compose.yml`** - Docker cluster setup: Hadoop (NameNode, DataNode) + Spark (1 Master, 2 Workers)
- **`hadoop.env`** - Hadoop environment configuration
- **`.gitignore`** - Excludes datasets and temporary files

### **Data Processing Scripts (app/)**

#### **Pipeline Stage 1: Data Cleaning & Filtering**
- **`etl_job.py`** 
  - Loads raw CSV datasets from HDFS
  - Filters individuals with `GPS_RECORD == True` only
  - Removes invalid trips (NoTrip, NoTraces, missing timestamps)
  - Matches trips with GPS-enabled individuals
  - **Outputs:** `individuals_clean.parquet`, `trips_clean.parquet`

#### **Pipeline Stage 2: Transport Mode Standardization**
- **`standardize_modes.py`**
  - Converts raw transport modes to standardized categories
  - **Categories:** Car, Coach, Minibus, Motorcycle, Train, Plane contrail, Zero emission
  - Standardizes all mode columns (Main_Mode, Mode_1-5)
  - **Outputs:** `trips_std.parquet`

#### **Pipeline Stage 3: Trip Complexity Classification**
- **`add_trip_complexity.py`**
  - Counts distinct transport modes per trip
  - Classifies as "Single" (unimodal) or "Multi" (intermodal)
  - **Outputs:** `trips_complexity.parquet`

#### **Pipeline Stage 4: GPS-Trip Matching**
- **`gps_ingest_and_tripkey.py`**
  - Loads all GPS CSV files (47.6M points)
  - Extracts participant ID from filenames
  - Temporal join: matches GPS points to trips by ID + timestamp range
  - Attaches trip metadata (mode, complexity) to each GPS point
  - **Outputs:** `gps_with_trip.parquet`

#### **Pipeline Stage 5: CO2 Emissions Calculation (FINAL)**
- **`final.py`** - **Complete 8-phase CO2 calculation pipeline**
  
  **Phase 1:** Load GPS trajectory data (47.6M points)
  
  **Phase 2:** Calculate distances between consecutive GPS points
  - Uses Haversine formula with window functions
  - Groups by trip key, orders by timestamp
  - Produces 41.3M distance segments
  
  **Phase 3:** Apply ADEME emission factors
  - Car: 194 g/km
  - Train: 61 g/km
  - Coach: 127 g/km
  - Motorcycle: 118 g/km
  - Zero emission (Walk/Bike): 0 g/km
  - Calculates CO2 per segment
  
  **Phase 4:** Aggregate to trip level
  - Sum distances and CO2 by trip
  - Produces 66,503 unique trips
  
  **Phase 5:** Aggregate to person level
  - Calculate weekly totals per individual
  - Compute averages: kg/week, kg/day, g/km
  - Produces 3,324 individuals
  
  **Phase 6:** Join with demographics
  - Merge with individuals dataset
  - Preserve all original columns
  - Add 7 new CO2 metrics
  
  **Phase 7:** Summary statistics
  - Overall averages
  - By car ownership
  - By gender
  - Top 10 emitters
  
  **Phase 8:** Save results
  - **Outputs:** 
    - `individuals_with_co2.parquet` (compressed)
    - `individuals_with_co2.csv` (1.1MB, for analysis)

#### **Validation Scripts**
- **`inspect_gps_with_trip.py`** - Validates GPS-trip matching results

### **Data Directories**
- **`data/`** - Local data folder (mounted to Docker containers)
  - `individuals_dataset.csv` - Demographics (age, gender, car ownership, etc.)
  - `trips_dataset.csv` - Trip records with origin, destination, modes
  - `gps_dataset/` - Individual GPS trace files (e.g., `10_2978.csv`)

---

## ⚡ Quick Start Guide

### 1. Prerequisites
- **Docker Desktop** (12GB+ RAM allocated)
- **Git**

### 2. Initial Setup (First Time Only)

```bash
# 1. Clone repository
git clone https://github.com/Hana-ElGabry/big_data_code
cd code

# 2. Download datasets from Google Drive
# Link: https://drive.google.com/drive/folders/1-VGYqwBsTQxxUTKw2jPoUATbaHUh8MHs
# Place files in data/ folder:
#   data/individuals_dataset.csv
#   data/trips_dataset.csv
#   data/gps_dataset/*.csv (all GPS files)

# 3. Start Docker cluster
docker-compose up -d

# 4. Wait 30 seconds for services to initialize
```

---

## 🚀 Execute Complete Pipeline

Run these commands in sequence to process data from raw CSVs to final CO2 analysis:

### **Stage 1: ETL & Data Cleaning**
```bash
docker exec spark-master spark-submit --master spark://spark-master:7077 /app/etl_job.py
```
- Filters GPS-enabled individuals
- Removes invalid trips
- **Output:** `individuals_clean.parquet`, `trips_clean.parquet`

### **Stage 2: Standardize Transport Modes**
```bash
docker exec spark-master spark-submit --master spark://spark-master:7077 /app/standardize_modes.py
```
- Maps raw modes to ADEME categories
- **Output:** `trips_std.parquet`

### **Stage 3: Add Trip Complexity**
```bash
docker exec spark-master spark-submit --master spark://spark-master:7077 /app/add_trip_complexity.py
```
- Classifies Single vs Multi-modal trips
- **Output:** `trips_complexity.parquet`

### **Stage 4: GPS-Trip Matching**
```bash
docker exec spark-master spark-submit --master spark://spark-master:7077 /app/gps_ingest_and_tripkey.py
```
- Loads 47.6M GPS points
- Joins with trips by timestamp
- **Output:** `gps_with_trip.parquet`

### **Stage 5: CO2 Calculation (FINAL STAGE)**
```bash
docker exec spark-master spark-submit --master spark://spark-master:7077 /app/final.py
```
- Calculates Haversine distances (41.3M segments)
- Applies ADEME emission factors
- Aggregates to person level
- **Output:** `individuals_with_co2.parquet` and `individuals_with_co2.csv`

**Expected runtime:** ~2 minutes  
**Expected output:** "PIPELINE 100% COMPLETE!"

---

## � Retrieve Results from HDFS

After pipeline completion, get the CSV file to your local machine:

```bash
# 1. Check file exists in HDFS
docker exec namenode hdfs dfs -ls /processed/individuals_with_co2.csv

# 2. Copy from HDFS to container
docker exec namenode hdfs dfs -get /processed/individuals_with_co2.csv /tmp/

# 3. Copy from container to local machine
docker cp namenode:/tmp/individuals_with_co2.csv .
```

The CSV will be in `individuals_with_co2.csv/part-00000-*.csv` (1.1MB file with headers).

---

## 🔍 Pipeline Flow Diagram

```
Raw Data (HDFS /raw/)
├── individuals_dataset.csv
├── trips_dataset.csv  
└── gps_dataset/*.csv (47.6M points)
        ↓
   [etl_job.py]
   Filter GPS-enabled individuals
   Remove invalid trips
        ↓
/processed/individuals_clean.parquet (3,324 people)
/processed/trips_clean.parquet (66,503 trips)
        ↓
   [standardize_modes.py]
   Map raw modes → ADEME categories (Monitor running jobs)
- **Spark Worker 1:** http://localhost:8081
- **Spark Worker 2:** http://localhost:8082
- **HDFS NameNode:** http://localhost:9870 (Browse HDFS filesystem)

---

## 🛠️ Common Commands

### View Files in HDFS
```bash
# List all processed files
docker exec namenode hdfs dfs -ls /processed

# Check file size
docker exec namenode hdfs dfs -du -h /processed/individuals_with_co2.csv
```

### Copy Files from HDFS
```bash
# To container /tmp
docker exec namenode hdfs dfs -get /processed/individuals_with_co2.csv /tmp/

# From container to local
docker cp namenode:/tmp/individuals_with_co2.csv .
```

### Monitor Pipeline Execution
```bash
# Watch logs in real-time
docker logs -f spark-master

# Check job progress
# Visit http://localhost:8080 during execution
```

### Restart/Stop Cluster
```bash
# Stop all services
docker-compose down

# Start fresh
docker-compose up -d

# Remove all data (WARNING: deletes HDFS data)
docker-compose down -v
```

---

## 📝 Technical Specifications

### Cluster Configuration
- **Total Memory:** 12GB
  - NameNode: 1GB
  - DataNode: 2GB  
  - Spark Master: 1GB
  - Spark Worker 1: 3GB
  - Spark Worker 2: 3GB
  - Streamlit: 1GB
- **Shuffle Partitions:** 300 (optimized for 6GB executor memory)
- **Compression:** Snappy for all Parquet files
- **Python Version:** 2.7 in containers (requires `.format()` not f-strings)

### Data Pipeline Metrics
- **Input:** 47,666,815 GPS points from 3,324 individuals
- **Processing:** 41,289,967 distance segments calculated
- **Output:** 66,503 trips aggregated to 3,324 individual CO₂ profiles
- **Data Retention:** Only GPS-enabled individuals (~70% of original dataset)
- **Runtime:** ~2 minutes for complete pipeline

### Output Schema
The final CSV contains all original demographic columns plus:
- `total_distance_km_week` - Total distance traveled
- `total_co2_g_week` - Total CO₂ in grams
- `co2_kg_per_week` - CO₂ in kilograms per week
- `co2_kg_per_day` - Average daily CO₂
- `total_trips_week` - Number of trips
- `avg_trip_distance_km` - Average trip distance
- `co2_g_per_km` - CO₂ intensity (grams per kilometer
```

---

## 📈 Understanding the CO2 Calculation

### How It Works

1. **Distance Calculation**
   - Uses Haversine formula to calculate great-circle distance between consecutive GPS points
   - Formula: $d = 2r \arctan2(\sqrt{a}, \sqrt{1-a})$ where $a = \sin^2(\frac{\Delta\phi}{2}) + \cos\phi_1 \cdot \cos\phi_2 \cdot \sin^2(\frac{\Delta\lambda}{2})$
   - Earth radius (r) = 6,371 km

2. **ADEME Emission Factors** (grams CO₂ per kilometer)
   - Car: 194 g/km
   - Coach/Bus: 127 g/km
   - Train: 61 g/km
   - Motorcycle: 118 g/km
   - Minibus: 715 g/km
   - Plane contrail: 285 g/km
   - Zero emission (Walk/Bike): 0 g/km

3. **Aggregation Levels**
   - **Segment:** CO₂ = distance × emission_factor
   - **Trip:** Sum all segments within same trip key
   - **Person:** Sum all trips for individual, calculate weekly/daily averages

### Expected Results

- **Average CO₂ per person:** ~10 kg/week (aligns with urban transport research)
- **Data coverage:** 3,324 individuals, 66,503 trips, 41.3M distance segments

---

## 📊 Monitoring & Access
OutOfMemoryError during execution
**Solution:** 
- Increase Docker Desktop memory allocation (Settings > Resources)
- Reduce shuffle partitions in script config
- Process data in smaller batches

### HDFS Connection Refused
**Solution:** 
- Wait 30-60 seconds after `docker-compose up` for services to initialize
- Check NameNode status: `docker exec namenode hdfs dfsadmin -report`
- Restart cluster: `docker-compose restart`

### Missing Parquet Files
**Solution:** 
- Run pipeline stages in correct sequence - each depends on previous output
- Verify HDFS directory: `docker exec namenode hdfs dfs -ls /processed`
- Check Spark job completed successfully in logs

### SyntaxError: invalid syntax (Python compatibility)
**Solution:**
- Scripts use Python 2.7 syntax (`.format()` instead of f-strings)
- All scripts already fixed for compatibility
- If adding new code, avoid f-strings and ensure UTF-8 encoding declaration

### CSV file is a directory
**Solution:**
- Spark writes CSV as directory with part files
- Actual data is in `part-00000-*.csv` file
- Use `coalesce(1)` to create single output file (already implemented)

### Pipeline runs but output empty
**Solution:**
- Check input data loaded correctly
- Verify GPS points have valid timestamps
- Ensure trips have matching IDs with individuals
- Check joins aren't filtering out all data

---

## 📚 Additional Resources

- **Apache Spark Documentation:** https://spark.apache.org/docs/latest/
- **HDFS Commands Reference:** https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HDFSCommands.html
- **ADEME Emission Factors:** French environmental agency standards
- **Haversine Formula:** https://en.wikipedia.org/wiki/Haversine_formula

---

## 👥 Contributors

This pipeline was developed to replicate urban CO₂ emissions research using distributed big data processing.

---

## 📄 License

This project is for educational and research purposes.
## 🛠️ Common Commands

### View HDFS Files
```bash
docker exec -it namenode hdfs dfs -ls /processed
```

### Copy from HDFS to Local
```bash
docker exec -it namenode hdfs dfs -get /processed/gps_with_trip.parquet /data/
```

### Check Spark Job Status
```bash
docker exec -it spark-master /spark/bin/spark-submit --status <application-id>
```

### Stop Cluster
```bash
docker-compose down
```

---

## 📝 Notes

- **Memory:** Total cluster: 12GB (NameNode: 1GB, DataNode: 2GB, Master: 1GB, 2 Workers: 3GB each, Streamlit: 1GB)
- **Shuffle Partitions:** Configured for optimal performance (200-300 partitions)
- **Compression:** All parquet files use Snappy compression for efficiency
- **Data Retention:** Pipeline keeps only GPS-enabled individuals (~70-80% of original data)

---

## 🐛 Troubleshooting

### Issue: OutOfMemoryError
**Solution:** Increase Docker memory allocation or reduce shuffle partitions

### Issue: HDFS Connection Refused
**Solution:** Wait 30s after `docker-compose up` for services to initialize

### Issue: Missing Parquet Files
**Solution:** Run pipeline stages in sequence - each depends on previous output



