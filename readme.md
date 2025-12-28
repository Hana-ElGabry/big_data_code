

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
# 🚴 NetMob Big Data Project

## 1. Setup (First Time Only)
1. Install Docker Desktop.
2. Clone this repo.
3. Put the raw CSV files into `data/gps_dataset/` and `data/trips_dataset.csv`.
4. Run:
   ```bash
   docker-compose up -d
```