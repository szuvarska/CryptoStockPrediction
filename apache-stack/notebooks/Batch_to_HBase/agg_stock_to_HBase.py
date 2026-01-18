import happybase
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row
from pyspark.sql import Window
import sys
from datetime import datetime
from tqdm import tqdm

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

# Make sure the checkpoint table exists
spark.sql("""
CREATE TABLE IF NOT EXISTS cryptopredictions.batch_checkpoint (
    table_name STRING,
    last_processed_date DATE,
    run_ts TIMESTAMP
)
STORED AS PARQUET;
""")

# Read checkpoint for your table
checkpoint_df = (
    spark.table("cryptopredictions.batch_checkpoint")
    .filter(F.col("table_name") == "indexsnapshot")
)

# Get the latest checkpoint
latest_checkpoint_row = checkpoint_df.orderBy(F.col("last_processed_date").desc()).limit(1).collect()

if latest_checkpoint_row:
    checkpoint = latest_checkpoint_row[0]["last_processed_date"]
    print("Checkpoint:", checkpoint)
else:
    checkpoint = None  # No checkpoint yet
    print("No checkpoint yet")
    
stock_df = spark.table("cryptopredictions.indexsnapshot")
if checkpoint:
    stock_df = stock_df.filter(F.col("PartitionDate") > checkpoint)
    if stock_df.count() == 0:
        print("No new data")
        sys.exit(0)
        
# Sanity Check
bounds = (
    stock_df.select(
        F.min("Datetime").alias("first_datetime"),
        F.max("Datetime").alias("last_datetime")
    )
    .collect()[0]
)

print(f"[SANITY CHECK] Datetime range: {bounds.first_datetime} → {bounds.last_datetime}")

def aggregate_crypto(df, window_duration, granularity_label):
    if granularity_label != "1d":
        return (
            df
            .groupBy(
                "IndexName",
                F.window("Datetime", window_duration).alias("w")
            )
            .agg(
                F.first("CurrentPrice").alias("open"),
                F.max("HighestDayPrice").alias("high"),
                F.min("LowestDayPrice").alias("low"),
                F.last("CurrentPrice").alias("close"),
                F.last("CurrentVolume").alias("max_volume")
            )
            .withColumn("granularity", F.lit(granularity_label))
            .withColumn("timestamp", F.col("w.start"))
            .drop("w")
        )
    elif granularity_label == "1d":
        return (
                df
                .groupBy(
                    "IndexName",
                    F.window("Datetime", window_duration).alias("w")
                )
                .agg(
                    F.first("CurrentPrice").alias("open"),
                    F.max("HighestDayPrice").alias("high"),
                    F.min("LowestDayPrice").alias("low"),
                    F.last("CurrentPrice").alias("close"),
                    F.last("CurrentVolume").alias("max_volume"),
                    # Indicators -> created at around 4:30 -> that's why we take last
                    F.last("FiftyDayAveragePrice").alias("ma_50_price"),
                    F.last("TwoHundredDaysAveragePrice").alias("ma_200_price"),
                    F.last("TenDayAverageVolume").alias("ma_10_volume"),
                    F.last("ThreeMonthAverageVolume").alias("ma_3m_volume"),
                    F.last("YearOverYearPriceChange").alias("yoy_price_change"),
                )
                .withColumn("granularity", F.lit(granularity_label))
                .withColumn("timestamp", F.col("w.start"))
                .drop("w")
            )
        
# Aggregate per granularity
stock_1m  = aggregate_crypto(stock_df, "1 minute", "1m")
stock_10m = aggregate_crypto(stock_df, "10 minutes", "10m")
stock_1d  = aggregate_crypto(stock_df, "1 day", "1d")

# Add missing daily indicators to intraday
def add_daily_indicator_nulls(df):
    return (
        df
        .withColumn("ma_50_price", F.lit(None).cast(T.DoubleType()))
        .withColumn("ma_200_price", F.lit(None).cast(T.DoubleType()))
        .withColumn("ma_10_volume", F.lit(None).cast(T.LongType()))
        .withColumn("ma_3m_volume", F.lit(None).cast(T.LongType()))
        .withColumn("yoy_price_change", F.lit(None).cast(T.DoubleType()))
    )
stock_1m  = add_daily_indicator_nulls(stock_1m)
stock_10m = add_daily_indicator_nulls(stock_10m)

# Add SMA 7 & SMA 30 (ALL granularities)
def add_smas(df, periods=[7,30]):
    for period in periods:
        window_spec = Window.partitionBy("IndexName", "granularity").orderBy("timestamp").rowsBetween(-(period-1), 0)
        df = df.withColumn(f"SMA_{period}", F.avg("close").over(window_spec))
    return df

stock_1m  = add_smas(stock_1m)
stock_10m = add_smas(stock_10m)
stock_1d  = add_smas(stock_1d)

# Union all granularities
stock_agg = stock_1m.unionByName(stock_10m).unionByName(stock_1d)


def row_to_hbase(row):
    row_key = f"{row['IndexName']}#{row['timestamp']}#{row['granularity']}"
    if row['granularity'] == "1d":
        return (
            row_key.encode(),
            {
                b"ohlc:open": str(row['open']).encode(),
                b"ohlc:high": str(row['high']).encode(),
                b"ohlc:low": str(row['low']).encode(),
                b"ohlc:close": str(row['close']).encode(),
                b"indicators:sma_7_price": str(row['SMA_7']).encode(),
                b"indicators:sma_7_price": str(row['SMA_30']).encode(),
                b"indicators:ma_50_price": str(row['ma_50_price']).encode(),
                b"indicators:ma_200_price": str(row['ma_200_price']).encode(),
                b"indicators:ma_10_volume": str(row['ma_10_volume']).encode(),
                b"indicators:yoy_price_change": str(row['yoy_price_change']).encode(),
            }
        )
    else:
        return (
            row_key.encode(),
            {
                b"ohlc:open": str(row['open']).encode(),
                b"ohlc:high": str(row['high']).encode(),
                b"ohlc:low": str(row['low']).encode(),
                b"ohlc:close": str(row['close']).encode(),
                b"indicators:sma_7_price": str(row['SMA_7']).encode(),
                b"indicators:sma_7_price": str(row['SMA_30']).encode(),
            }
        )
      
# To HBase
run_ts = datetime.now()

connection = happybase.Connection(host='hbase')
table = connection.table('crypto_index_aggregates')
rows = stock_agg.collect()
for row in tqdm(rows):
    key, data = row_to_hbase(row)
    table.put(key, data)

max_date = stock_df.agg(F.max("PartitionDate")).collect()[0][0]

new_checkpoint = spark.createDataFrame([Row(
    table_name="indexsnapshot",
    last_processed_date=max_date,
    run_ts=run_ts
)])

(new_checkpoint.write
    .mode("append")
    .format("hive")
    .saveAsTable("cryptopredictions.batch_checkpoint"))

print("HBase insertion and checkpoint update succeeded!")

end_ts = datetime.now()
print(f"Stock Hive -> Hbase ended at {end_ts}")
connection.close()
spark.stop()