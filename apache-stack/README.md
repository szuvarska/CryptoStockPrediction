# Apache Big Data Stack

This folder contains the Infrastructure-as-Code (IaC) setup used to provision a complete environment for the project using Docker .

---

## Services & Ports

The `docker-compose.yml` file defines the following services:

| Service                 | Container Name    | Host Port       | Internal Port | Description                                           |
| ----------------------- | ----------------- | --------------- | ------------- | ----------------------------------------------------- |
| **Shiny App**     | `shiny-app`     | **8000**  | 8000          | Visualization dashboard (Python).                     |
| **NiFi**          | `nifi`          | **8080**  | 8080          | Data ingestion UI.                                    |
| **Spark Master**  | `spark-master`  | **8087**  | 8080          | Spark cluster UI (mapped to 8087 to avoid conflicts). |
| **Spark Jupyter** | `spark-jupyter` | **8888**  | 8888          | PySpark notebook environment.                         |
| **HBase Master**  | `hbase`         | **16010** | 16010         | HBase Web UI.                                         |
| **HDFS NameNode** | `namenode`      | **9870**  | 9870          | Hadoop File System UI.                                |
| **Postgres**      | `hive-postgres` | 5432            | 5432          | Database for Hive Metastore.                          |
| **Hive Server2**  | `hive-server2`  | 10000           | 10000         | Hive JDBC interface.                                  |
| **Kafka**         | `kafka`         | 29092           | 29092         | Message broker.                                       |

---

## Setup Instructions

### 1. Start the Environment

Build all images (including the Shiny app) and start the containers in detached mode:

```
docker-compose up -d --build
```

---

### 2. Configure HDFS

Before data can be stored, required HDFS directories and permissions must be created.

Run the helper script:

```
bash hdfs-setup.sh
```

This script:

* Waits for the NameNode to exit safe mode
* Creates the following directories:
  * `/tmp`
  * `/user/hive/warehouse`

---

### 3. Verify Services

Ensure all containers are running correctly:

```
docker ps
```

Check logs for specific services if needed:

```
docker logs -f spark-master
docker logs -f nifi
```

---

## Notebooks

The `notebooks/` directory is mounted into the `spark-jupyter` container at:

```
/opt/notebooks
```

Contents include:

- **ETL → Hive:** `preprocess_to_hive/` - notebooks and `*.py` scripts that create Hive tables, clean and transform source data, and write to the Hive warehouse (e.g., `crypto_to_hive.py`, `stock_to_hive.py`, `forex_to_hive.py`, `utils.py`).

- **Batch → HBase:** `Batch_to_HBase/` - aggregation notebooks and helper scripts (`agg_crypto_to_HBase.py`, `agg_stock_to_HBase.py`) that produce batch views stored in HBase for fast lookups.

- **Stream preprocessing:** scripts used by streaming jobs to prepare records for HBase ingestion (`crypto_to_hbase.py`, `stock_to_hbase.py`).

- **Batch jobs / Scheduling:** `batch_jobs/` - shell jobs and crontab examples (`crypto_job.sh`, `stock_job.sh`, `crontab_config.txt`) for running periodic batch tasks.

- **EDA & Analysis:** `EDA.ipynb`, `EDA_nulls.ipynb` - are helper notebooks for problem exploration and plotting.

- **ML experiments:** `Test_ml.ipynb` and `retrain.ipynb` contain model training, evaluation, and retraining examples.

---

## Configuration Files

* `hive-conf/hive-site.xml`
  Shared Hive configuration mounted into Spark, Hive, and Shiny containers to ensure consistent metastore connectivity.
* `Dockerfile.shiny`
  Defines the environment for the web application.
  Built on top of the Spark image to enable PySpark job submission.
