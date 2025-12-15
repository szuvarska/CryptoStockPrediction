from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType
import happybase

kafka_bootstrap = "kafka:9092"
topics = ["crypto-prices", "stock-prices"]
hbase_host = "hbase"

spark = SparkSession.builder \
    .appName("CryptoStockToHBase") \
    .getOrCreate()

schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("timestamp", LongType())

def write_to_hbase(batch_df, batch_id, table_name):
    connection = happybase.Connection(hbase_host)
    table = connection.table(table_name)
    for row in batch_df.collect():
        row_key = f"{row['symbol']}_{row['timestamp']}"
        table.put(row_key.encode(), {b"data:price": str(row['price']).encode()})
    connection.close()

for topic in topics:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
                  .select(from_json(col("json"), schema).alias("data")) \
                  .select("data.*")

    table_name = "crypto" if "crypto" in topic else "stock"
    parsed_df.writeStream \
        .foreachBatch(lambda df, batch_id: write_to_hbase(df, batch_id, table_name)) \
        .outputMode("append") \
        .start()

spark.streams.awaitAnyTermination()
