from pyspark.sql import SparkSession
import pyspark
from pyspark.sql import functions as F
from urllib.parse import urlparse
from datetime import date
from pyspark.sql.types import LongType
from py4j.java_gateway import java_import
from hdfs import InsecureClient
from utils import find_new_paths
from datetime import datetime
import sys
from tqdm import tqdm

spark = (SparkSession.builder
    .appName("Batch Preprocess")
    .master("spark://spark-master:7077")
    .config("spark.cores.max", "1")
    .config("spark.executor.cores", "1")
    .enableHiveSupport()
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.hadoop.hive.exec.dynamic.partition", "true")
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
    .getOrCreate())

spark.sql("USE DATABASE CryptoPredictions")

BASE_PATH = "hdfs://namenode:8020/nifi/stock-prices/"
META_PATH = "hdfs://namenode:8020/nifi/metadata/stock_prices_last_path.txt"

paths_to_read, new_dirs = find_new_paths(spark, BASE_PATH, META_PATH)
if len(new_dirs) == 0:
    print(f"INFO - No new dirs - shutting down")
    sys.exit(0)

# Wczytanie nowych folderów do DataFrame
df = spark.read.parquet(*paths_to_read)

# Spark zapisuje plik źródłowy w kolumnie __file__ (od Spark 3.x)
df_with_file = df.withColumn("__file__", F.input_file_name())

# Pobranie schematów per plik
files = df_with_file.select("__file__").distinct().collect()

bad_files = []
print("INFO - Checking for bad files")
for row in files:
    file_path = row["__file__"]
    df_file = spark.read.parquet(file_path)
    field_names = df_file.schema.fieldNames()
    if "lastVolume" not in field_names:
        bad_files.append(file_path)
        print(f"{file_path} -> missing column: lastVolume")
        continue

    dtype = df_file.schema["lastVolume"].dataType
    if not isinstance(dtype, LongType):
        bad_files.append(file_path)
        print(f"{file_path} -> lastVolume wrong type: {dtype}")

# Access Hadoop FileSystem
conf = spark._jsc.hadoopConfiguration()
uri = spark._jvm.java.net.URI("hdfs://namenode:8020")
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)

Path = spark._jvm.org.apache.hadoop.fs.Path

bad_files_folder = "hdfs://namenode:8020/nifi/bad-stock-prices"
bad_folder_path = Path(bad_files_folder)

if not fs.exists(bad_folder_path):
    fs.mkdirs(bad_folder_path)
    
for file_path in bad_files:
    src = Path(file_path)
    # Construct destination path (keep original filename)
    filename = file_path.split("/")[-1]
    dst = Path(f"{bad_files_folder}/{filename}")
    fs.rename(src, dst)  # moves the file

# The dataframe needs to be reloaded if there were any bad files
if bad_files:
    df = spark.read.parquet(*paths_to_read)

def transform_index_snapshot(df):
    res = (
        df
        # Dopasowanie nazw
        .withColumn("IndexName", F.col("exchange"))
        .withColumn("Datetime", F.to_timestamp("fetch_timestamp"))
        .withColumn("CurrentPrice", F.col("lastPrice"))
        .withColumn("CurrentVolume", F.col("lastVolume"))
        .withColumn("OpeningPrice", F.col("open"))
        .withColumn("LowestDayPrice", F.col("dayLow"))
        .withColumn("HighestDayPrice", F.col("dayHigh"))
        .withColumn("LowestYearlyPrice", F.col("yearLow"))
        .withColumn("HighestYearlyPrice", F.col("yearHigh"))
        .withColumn("FiftyDayAveragePrice", F.col("fiftyDayAverage"))
        .withColumn("TenDayAverageVolume", F.col("tenDayAverageVolume"))
        .withColumn("ThreeMonthAverageVolume", F.col("threeMonthAverageVolume"))
        .withColumn("TwoHundredDaysAveragePrice", F.col("twoHundredDayAverage"))
        .withColumn("YearOverYearPriceChange", F.col("yearChange"))
        # przeniosłem return na koniec -> tu przeszkadza we wczytywaniu nowych danych do hive.
        .withColumn("PartitionDate", F.to_date("fetch_timestamp"))
    )

    final_cols = [
        "IndexName",
        "Datetime",
        "CurrentPrice",
        "CurrentVolume",
        "OpeningPrice",
        "LowestDayPrice",
        "HighestDayPrice",
        "LowestYearlyPrice",
        "HighestYearlyPrice",
        "FiftyDayAveragePrice",
        "TwoHundredDaysAveragePrice",
        "TenDayAverageVolume",
        "ThreeMonthAverageVolume",
        "YearOverYearPriceChange",
        "PartitionDate"
    ]

    return res.select(*final_cols)

df_transformed = transform_index_snapshot(df)
print("INFO - DataFrame created")

# Sanity Check
bounds = (
    df_transformed.select(
        F.min("Datetime").alias("first_datetime"),
        F.max("Datetime").alias("last_datetime")
    )
    .collect()[0]
)

print(f"[SANITY CHECK] Datetime range: {bounds.first_datetime} → {bounds.last_datetime}")

(df_transformed.write
    .mode("append")
    .format("hive")
    .partitionBy("PartitionDate")
    .saveAsTable("IndexSnapshot"))

latest_dir = max(new_dirs)

spark.createDataFrame(
    [(latest_dir,)],
    ["last_processed_path"]
).write.mode("overwrite").text(META_PATH)

print(f"Created last path file: {latest_dir}")

end_ts = datetime.now()
print(f"INFO - Ending script at {end_ts}")
spark.stop()