from pyspark.sql import SparkSession
import pyspark
import re
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F
from urllib.parse import urlparse
from pyspark.sql.types import *
from utils import find_new_paths
import sys
from datetime import datetime

spark = (
    SparkSession.builder 
    .appName("preprocess_batch") 
    .master("spark://spark-master:7077") 
    .config("spark.cores.max", "1")
    .config("spark.executor.cores", "1")
    .enableHiveSupport()
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.hadoop.hive.exec.dynamic.partition", "true")
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
    .getOrCreate()
    )

spark.sql("USE DATABASE CryptoPredictions")

BASE_PATH = "hdfs://namenode:8020/nifi/forex"
META_PATH  = "hdfs://namenode:8020/nifi/metadata/forex_last_timestamp.txt"

# Read last timestamp from file
def read_last_timestamp(spark, meta_path):
    try:
        return (
            spark.read.text(meta_path)
                 .first()[0]
        )
    except Exception:
        # First run fallback
        return "2025-11-25 00:00:00"

last_ts = read_last_timestamp(spark, META_PATH)
print("INFO - Last processed timestamp:", last_ts)

def list_hdfs_files(spark, base_path):
    conf = spark._jsc.hadoopConfiguration()
    uri = spark._jvm.java.net.URI(base_path)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
    path = spark._jvm.org.apache.hadoop.fs.Path(base_path)

    return [
        f.getPath().getName()
        for f in fs.listStatus(path)
        if f.isFile() and f.getPath().getName().endswith(".parquet")
    ]

files = list_hdfs_files(spark, BASE_PATH)

# Process only the ones who match the regex name
def extract_timestamp(fname):
    match = re.search(r"forex_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})", fname)
    if not match:
        return None
    date_part, time_part = match.groups()
    return f"{date_part} {time_part.replace('-', ':')}"

df_files = spark.createDataFrame(
    [(f, extract_timestamp(f)) for f in files if extract_timestamp(f)],
    ["file_name", "file_ts"]
).withColumn(
    "file_ts", F.to_timestamp("file_ts")
) 
last_ts_lit = F.to_timestamp(F.lit(last_ts))

# Find new files
new_files_df = df_files.filter(F.col("file_ts") > last_ts_lit)

new_file_paths = [
    f"{BASE_PATH}/{row.file_name}"
    for row in new_files_df.collect()
]

if new_file_paths:    
    df = spark.read.parquet(*new_file_paths)
    print("INFO - Loaded data")    
else:
    print("No new files to process - shutting down")
    sys.exit(0)

def unpack_results(df, results_col='results', ticker_col='ticker'):
    """
    Rozpakowuje kolumnę zagnieżdżonych wyników giełdowych i tworzy z niej 
    kolumny analityczne w DataFrame Spark.

    Parametry:
    ----------
    df : pyspark.sql.DataFrame
        DataFrame zawierający kolumnę z wynikami (np. z API finansowego).
    results_col : str, opcjonalnie
        Nazwa kolumny zawierającej zagnieżdżone wyniki (default 'results').
    ticker_col : str, opcjonalnie
        Nazwa kolumny z symbolami instrumentów finansowych (default 'ticker').

    Zwraca:
    -------
    pyspark.sql.DataFrame
        DataFrame z rozpakowanymi kolumnami: VolumeTraded, OpeningPrice,
        HighestDailyPrice, LowestDailyPrice, ClosingPrice, AveragedPrice,
        NumberOfTrades, Date, PartitionDate, CurrencyTo.
        Niepotrzebne kolumny źródłowe są usunięte.
    """

    # Rozpakowanie pierwszego elementu z listy wyników
    df = df.withColumn("res", F.col(results_col).getItem(0))

    # Tworzenie osobnych kolumn z wartościami giełdowymi
    df = df.withColumn("VolumeTraded", F.col("res.v")) \
           .withColumn("OpeningPrice", F.col("res.o")) \
           .withColumn("HighestDailyPrice", F.col("res.h")) \
           .withColumn("LowestDailyPrice", F.col("res.l")) \
           .withColumn("ClosingPrice", F.col("res.c")) \
           .withColumn("AveragedPrice", F.col("res.vw")) \
           .withColumn("timestamp", F.col("res.t")) \
           .withColumn("NumberOfTrades", F.col("res.n"))

    # Czyszczenie kolumny ticker i wyodrębnienie walut
    df = df.withColumn("ticker_clean", F.regexp_replace(F.col(ticker_col), "^C:", "")) \
           .withColumn("CurrencyFrom", F.col("ticker_clean").substr(1, 3)) \
           .withColumn("CurrencyTo", F.expr("substring(ticker_clean, 4, length(ticker_clean))"))

    # Konwersja timestamp na datę i stworzenie kolumny do partycjonowania
    df = df.withColumn("Date", F.to_date(F.from_unixtime(F.col("timestamp") / 1000))) \
           .withColumn("PartitionDate", F.col("Date"))

    # Usunięcie kolumn pomocniczych i zbędnych
    df = df.drop('results', 'res', 'ticker_clean', 'ticker', 
                 'queryCount', 'resultsCount', 'adjusted', 
                 'status', 'request_id', 'count', 'timestamp', 'CurrencyFrom')

    return df
    
unpacked = unpack_results(df)
unpacked = unpacked.where(unpacked.PartitionDate.isNotNull()) # Usuwamy nulle

(unpacked.write
  .mode("append")
  .format("hive")
  .partitionBy("PartitionDate")
  .saveAsTable("USDExchangeRates"))

print("Saved to the table")

if new_file_paths:
    new_max_ts = (
        new_files_df.agg(F.max("file_ts").alias("max_ts"))
                    .first()["max_ts"]
    )
    spark.createDataFrame(
        [(new_max_ts.strftime("%Y-%m-%d %H:%M:%S"),)],
        ["last_processed_timestamp"]
    ).write.mode("overwrite").text(META_PATH)

    print(f"Updated last timestamp: {new_max_ts}")
    
end_ts = datetime.now()
print(f"Ending script at {end_ts}")
spark.stop()