from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F
from urllib.parse import urlparse
from datetime import date
from utils import find_new_paths
from datetime import datetime
import sys

spark = (SparkSession.builder
    .appName("Batch Preprocess")
    .master("spark://spark-master:7077")
    .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/nifi/crypto-prices")
    .config("spark.cores.max", "1")
    .config("spark.executor.cores", "1")
    .enableHiveSupport()
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.hadoop.hive.exec.dynamic.partition", "true")
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
    .getOrCreate())

spark.sql("USE DATABASE CryptoPredictions")
run_ts = datetime.now()
print(f"Running crypto_to_hive script at {run_ts}")

BASE_PATH = "hdfs://namenode:8020/nifi/crypto-prices/"
META_PATH = "hdfs://namenode:8020/nifi/metadata/crypto_prices_last_path.txt"

paths_to_read, new_dirs = find_new_paths(spark, BASE_PATH, META_PATH)

if len(new_dirs) == 0:
    print(f"INFO - No new dirs - shutting down")
    sys.exit(0)

def rename_and_transform(df):
    # Wyciągnięcie symbolu
    df = df.withColumn(
        "Symbol",
        F.split("symbol", ":")[1].substr(1, 3)
    )

    # Timestamp → Datetime
    df = df.withColumn(
        "Datetime",
        F.from_unixtime(F.col("timestamp")).cast("timestamp")
    )
    
    # Mapowanie: stara_nazwa → nowa_nazwa
    rename_map = {
        "current_price": "CurrentPrice",
        "open": "OpeningPrice",
        "low": "LowestDayPrice",
        "high": "HighestDayPrice",
        "previous_close": "PreviousClosingPrice"
    }

    # Zmiana nazw
    df = df.withColumnsRenamed(rename_map)
    
    df = df.withColumn("PartitionDate", F.to_date("Datetime"))
    df= df.filter(df.PartitionDate.isNotNull())
    
    return df.select(
        "Symbol",
        "CurrentPrice",
        "OpeningPrice",
        "LowestDayPrice",
        "HighestDayPrice",
        "PreviousClosingPrice",
        "Datetime",
        "PartitionDate"
    )

df = spark.read.parquet(*paths_to_read)
df = rename_and_transform(df)

print("INFO - DataFrame created")
# Sanity Check
bounds = (
    df.select(
        F.min("Datetime").alias("first_datetime"),
        F.max("Datetime").alias("last_datetime")
    )
    .collect()[0]
)

print(f"[SANITY CHECK] Datetime range: {bounds.first_datetime} → {bounds.last_datetime}")

(df.write
  .mode("append")
  .format("hive")
  .partitionBy("PartitionDate")
  .saveAsTable("CryptocurrencySnapshot"))

latest_dir = max(new_dirs)

spark.createDataFrame(
    [(latest_dir,)],
    ["last_processed_path"]
).write.mode("overwrite").text(META_PATH)

print(f"Created last path file: {latest_dir}")


end_ts = datetime.now()
print(f"INFO - Ending at {end_ts}")
print(f"-------Added Data To Hive-------")

spark.stop()