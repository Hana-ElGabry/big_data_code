

# 🌍 NetMob Big Data Migration (Spark + HDFS)

Migrating the "Urban CO2 Emissions" pipeline from local Pandas to a distributed Docker cluster.

## ⚡ Quick Start for New Teammates

### 1. Prerequisites
* **Docker Desktop** (Allocated 12GB+ RAM in Docker settings).
* **Git**.

### 2. Setup the Workspace
The huge dataset is ignored by Git. You must set it up manually.

```bash
# 1. Clone the repository
git clone <https://github.com/Hana-ElGabry/big_data_code>
cd code 
```

# 2. Create the data directories and make sure that the data is in the .gitignore file
#Here is the data download link:
[NetMob Dataset](https://drive.google.com/drive/folders/1-VGYqwBsTQxxUTKw2jPoUATbaHUh8MHs?usp=sharing)
inside the data you will find the 3 databses gps_dataset folder and 2 csv files
you will have to create 2 folders as shown below:

this is how the folder should look like 
data/
    - datanode
    - namenode
    - gps_dataset
    - individuals_dataset.csv
    - trips_dataset.csv

# 3 Launch the Cluster

```bash
docker-compose up -d
```

# 4. upload to hdfs
```bash
# Upload Trips
docker exec -it namenode hdfs dfs -put /hadoop/dfs/name/trips_dataset.csv /raw/

# Upload GPS Traces
docker exec -it namenode hdfs dfs -mkdir -p /raw/gps_dataset
docker exec -it namenode hdfs dfs -put /hadoop/dfs/name/gps_dataset/* /raw/gps_dataset/
```

# 5 Run the Spark Job

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 1G \
  --executor-memory 3G \
  /app/etl_job.py
```