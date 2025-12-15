from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import happybase
from datetime import datetime

# ----------------------------------------
# Spark Session
# ----------------------------------------
spark = SparkSession.builder \
    .appName("StockKafkaToHBase") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ----------------------------------------
# Kafka message schema
# (adjust fields ONLY if your producer differs)
# ----------------------------------------
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# ----------------------------------------
# Read from Kafka (STOCK TOPIC)
# ----------------------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock-prices") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# ----------------------------------------
# HBase writer function
# ----------------------------------------
def write_to_hbase(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    connection = happybase.Connection('hbase')
    table = connection.table('market_data')

    rows = []
    for row in batch_df.collect():
        symbol = row['symbol']
        price = row['price']
        ts = row['timestamp']

        date = datetime.utcfromtimestamp(ts / 1000).strftime('%Y%m%d')

        # ✅ REQUIRED ROW KEY FORMAT
        row_key = f"stock#{symbol}#{date}"

        rows.append((
            row_key.encode(),
            {
                b'cf:price': str(price).encode(),
                b'cf:timestamp': str(ts).encode()
            }
        ))

    if rows:
        table.batch().put_many(rows)

    connection.close()

# ----------------------------------------
# Start streaming
# ----------------------------------------
query = json_df.writeStream \
    .foreachBatch(write_to_hbase) \
    .outputMode("update") \
    .start()

query.awaitTermination()
