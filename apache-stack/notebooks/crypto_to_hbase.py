from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.feature import VectorAssembler
from datetime import datetime
import happybase

spark = (
    SparkSession.builder
    .appName("CryptoKafkaToHBaseWithPrediction")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("symbol", StringType()),
    StructField("timestamp", LongType()),
    StructField("NIM", DoubleType()),
    StructField("SNP", DoubleType()),
    StructField("DJI", DoubleType()),
    StructField("SOL", DoubleType()),
    StructField("ETH", DoubleType()),
    StructField("current_price", DoubleType())
])

model = LinearRegressionModel.load("hdfs://namenode:8020/models/btc_model")

feature_cols = ["NIM", "SNP", "DJI", "SOL", "ETH"]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

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

features_df = assembler.transform(parsed)
predicted_df = model.transform(features_df)

def write_to_hbase(batch_df, batch_id):
    connection = happybase.Connection(host="hbase")
    table = connection.table("market_prices")

    for row in batch_df.toLocalIterator():
        symbol = row.symbol
        date = datetime.utcfromtimestamp(row.timestamp).strftime("%Y%m%d")
        row_key = f"crypto#{symbol}#{date}"

        ts_col = datetime.utcfromtimestamp(row.timestamp).isoformat()
        prediction = float(row.prediction)
        price = float(row.current_price) if row.current_price else prediction

        existing = table.row(row_key)

        if b"stats:high" in existing:
            old_high = float(existing[b"stats:high"])
            old_low = float(existing[b"stats:low"])
            new_high = max(old_high, price)
            new_low = min(old_low, price)
            open_price = existing[b"stats:open"]
        else:
            new_high = price
            new_low = price
            open_price = str(price).encode()

        table.put(row_key, {
            f"predictions:{ts_col}".encode(): str(prediction).encode(),
            b"stats:open": open_price,
            b"stats:high": str(new_high).encode(),
            b"stats:low": str(new_low).encode(),
            b"stats:close": str(price).encode(),
            b"stats:last_prediction": str(prediction).encode()
        })

    connection.close()

query = (
    predicted_df.writeStream
    .foreachBatch(write_to_hbase)
    .outputMode("update")
    .start()
)

query.awaitTermination()
