import happybase
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime
from tqdm import tqdm
from pyspark.sql import Row
from pyspark.sql.window import Window
import sys
spark = (
    SparkSession.builder
    .appName("Batch Preprocess")
    .master("spark://spark-master:7077")
    .enableHiveSupport()
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.cores.max", "1")
    .config("spark.executor.cores", "1")
    .getOrCreate()
)

run_ts = datetime.now()
print(f"Running crypto_to_hbase script at {run_ts}")

# Make sure the checkpoint table exists
spark.sql("""
CREATE TABLE IF NOT EXISTS cryptopredictions.batch_checkpoint(
    table_name STRING,
    last_processed_date DATE,
    run_ts TIMESTAMP
)
STORED AS PARQUET;
""")

# Read checkpoint for your table
checkpoint_df = (
    spark.table("cryptopredictions.batch_checkpoint")
    .filter(F.col("table_name") == "cryptocurrencysnapshot")
)

# Get the latest checkpoint
latest_checkpoint_row = checkpoint_df.orderBy(F.col("last_processed_date").desc()).limit(1).collect()

if latest_checkpoint_row:
    checkpoint = latest_checkpoint_row[0]["last_processed_date"]
    print(f"Checkpoint: {checkpoint}")
else:
    checkpoint = None  # No checkpoint yet

crypto_df = spark.table("cryptopredictions.cryptocurrencysnapshot")
if checkpoint:
    crypto_df = crypto_df.filter(F.col("PartitionDate") > checkpoint)
        
    if crypto_df.count() == 0:
        print("No new data - nothing will be added to HBase")
        print("Shutting down")
        sys.exit(0)

# Sanity Check
bounds = (
    crypto_df.select(
        F.min("Datetime").alias("first_datetime"),
        F.max("Datetime").alias("last_datetime")
    )
    .collect()[0]
)

print(f"[SANITY CHECK] Datetime range: {bounds.first_datetime} → {bounds.last_datetime}")

def aggregate_crypto(df, window_duration, granularity_label):
    return (
        df
        .groupBy(
            "Symbol",
            F.window("Datetime", window_duration).alias("w")
        )
        .agg(
            F.first("CurrentPrice").alias("open"),
            F.max("CurrentPrice").alias("high"),
            F.min("CurrentPrice").alias("low"),
            F.last("CurrentPrice").alias("close")
        )
        .withColumn("granularity", F.lit(granularity_label))
        .withColumn("timestamp", F.col("w.start"))
        .drop("w")
    )
    
crypto_1m  = aggregate_crypto(crypto_df, "1 minute", "1m")
crypto_10m = aggregate_crypto(crypto_df, "10 minutes", "10m")
crypto_1d  = aggregate_crypto(crypto_df, "1 day", "1d")

def add_smas(df, periods=[7,30]):
    for period in periods:
        window_spec = Window.partitionBy("Symbol", "granularity").orderBy("timestamp").rowsBetween(-(period-1), 0)
        df = df.withColumn(f"SMA_{period}", F.avg("close").over(window_spec))
    return df

crypto_1m  = add_smas(crypto_1m)
crypto_10m = add_smas(crypto_10m)
crypto_1d  = add_smas(crypto_1d)

crypto_agg = crypto_1m.unionByName(crypto_10m).unionByName(crypto_1d)

def row_to_hbase(row):
    row_key = f"{row['Symbol']}#{row['timestamp']}#{row['granularity']}"
    return (
        row_key.encode(),
        {
            b"ohlc:open": str(row['open']).encode(),
            b"ohlc:high": str(row['high']).encode(),
            b"ohlc:low": str(row['low']).encode(),
            b"ohlc:close": str(row['close']).encode(),
            b'indicators:SMA_7': str(row['SMA_7']).encode(),
            b'indicators:SMA_30': str(row['SMA_30']).encode()
        }
    )
     
# To HBase
connection = happybase.Connection(host='hbase')
table = connection.table('crypto_index_aggregates')

rows = crypto_agg.collect()
max_date = crypto_df.agg(F.max("PartitionDate")).collect()[0][0]

batch_succeded = None

try:
    for row in tqdm(rows):
        key, data = row_to_hbase(row)
        table.put(key, data)
        last_successful_date = row['timestamp']
    batch_succeded = True

except Exception as e:
    print(f"Unexpected error: {e}. HBase insertion incomplete.")

finally:
    if batch_succeded is None:
        if last_successful_date:
            checkpoint_date = last_successful_date # Failed during loop
    elif batch_succeded == True:
        checkpoint_date = max_date

    if checkpoint_date != checkpoint:
        new_checkpoint = spark.createDataFrame([Row(
            table_name="cryptocurrencysnapshot",
            last_processed_date=checkpoint_date,
            run_ts=run_ts
        )])
    
        (new_checkpoint.write
            .mode("append")
            .format("hive")
            .saveAsTable("cryptopredictions.batch_checkpoint"))
        print(f"Checkpoint updated. Date: {checkpoint_date}")

print("------------Script ended------------")
connection.close()
spark.stop()