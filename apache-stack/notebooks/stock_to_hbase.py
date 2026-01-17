from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, from_json, col    
from pyspark.sql.types import *
from datetime import datetime
import happybase

# Initialize Spark
spark = (
    SparkSession.builder
    .appName("StockKafkaToHBase")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Define schema based on stock-price JSON
schema = StructType([
    StructField("exchange", StringType()),
    StructField("lastPrice", DoubleType()),
    StructField("fetch_timestamp", StringType())
])

# Read from Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "stock-prices")
    .option("startingOffsets", "latest")
    .load()
)

split_rows = df.select(
    explode(split(col("value").cast("string"), "\n")).alias("json_str")
)
parsed = split_rows.select(from_json(col("json_str"), schema).alias("j")).select("j.*")

def write_to_hbase(batch_df, batch_id):
    connection = happybase.Connection(host='hbase')
    table = connection.table('prices')
    for row in batch_df.collect():
        ticker = row.exchange
        if ticker == None:
            continue
        date = datetime.strptime(row.fetch_timestamp, "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d")
        row_key = f"stock#{ticker}#{date}"
        ts_col = datetime.strptime(row.fetch_timestamp, "%Y-%m-%d %H:%M:%S").isoformat()
        price = row.lastPrice

        # Store lastPrice under column family 'prices' with timestamped column
        table.put(row_key, {
            f'prices:{ts_col}'.encode(): str(price).encode()
        })

    connection.close()

# Start streaming query
query = (
    parsed.writeStream
    .foreachBatch(write_to_hbase)
    .outputMode("update")
    .start()
)

query.awaitTermination()
