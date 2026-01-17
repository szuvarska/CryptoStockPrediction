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

* **ETL to Hive**
  * `preprocess_to_hive/*.ipynb` - creation of Hive tables, preprocessing for all data sources and ingesting them to Hive
* **Batch views to Hbase**
  * `agg_x_to_HBase.ipynb`
* **Stream Preprocessing Scripts**
  * `x_to_hbase.py`
* **EDA**
  * `EDA.ipynb`
* **Machine Learning**
  * `Test_ml.ipynb`

---

## Configuration Files

* `hive-conf/hive-site.xml`
  Shared Hive configuration mounted into Spark, Hive, and Shiny containers to ensure consistent metastore connectivity.
* `Dockerfile.shiny`
  Defines the environment for the web application.
  Built on top of the Spark image to enable PySpark job submission.
