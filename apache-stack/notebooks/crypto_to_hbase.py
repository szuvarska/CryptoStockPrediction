from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from datetime import datetime
import happybase

spark = (
    SparkSession.builder
    .appName("CryptoKafkaToHBase")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("symbol", StringType()),
    StructField("current_price", DoubleType()),
    StructField("timestamp", LongType())
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "crypto-prices")
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    df.select(from_json(col("value").cast("string"), schema).alias("j"))
      .select("j.*")
)

def write_to_hbase(batch_df, batch_id):
    connection = happybase.Connection(host='hbase')
    table = connection.table('market_prices')

    for row in batch_df.collect():
        symbol = row.symbol.replace("BINANCE:", "")
        date = datetime.utcfromtimestamp(row.timestamp).strftime("%Y%m%d")
        row_key = f"crypto#{symbol}#{date}"

        ts_col = datetime.utcfromtimestamp(row.timestamp).isoformat()
        price = row.current_price

        existing = table.row(row_key)

        if b'stats:high' in existing:
            old_high = float(existing[b'stats:high'])
            old_low = float(existing[b'stats:low'])
            new_high = max(old_high, price)
            new_low = min(old_low, price)
            open_price = existing[b'stats:open']
        else:
            new_high = price
            new_low = price
            open_price = str(price).encode()

        table.put(row_key, {
            f'prices:{ts_col}'.encode(): str(price).encode(),
            b'stats:open': open_price,
            b'stats:high': str(new_high).encode(),
            b'stats:low': str(new_low).encode(),
            b'stats:close': str(price).encode(),
        })

    connection.close()

query = (
    parsed.writeStream
    .foreachBatch(write_to_hbase)
    .outputMode("update")
    .start()
)

query.awaitTermination()
