from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from datetime import datetime
import happybase
from pyspark.ml.regression import LinearRegressionModel, GBTRegressionModel
from pyspark.ml.feature import VectorAssembler

spark = (
    SparkSession.builder
    .appName("CryptoKafkaToHBase")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

#model = LinearRegressionModel.load("hdfs://namenode:8020/models/btc_model")
model = GBTRegressionModel.load("hdfs://namenode:8020/models/btc_model_GBTR_2")

feature_cols = ["NIM", "SNP", "DJI", "SOL", "ETH"]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

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

# =====================================================
# FOREACH BATCH
# =====================================================

def write_to_hbase(batch_df, batch_id):
    connection = happybase.Connection(host='hbase')
    table = connection.table('prices')
    latest_prices = {}

    for row in batch_df.collect():
        symbol = row.symbol.replace("BINANCE:", "")
        date = datetime.utcfromtimestamp(row.timestamp).strftime("%Y%m%d")
        row_key = f"crypto#{symbol}#{date}"
        ts_col = datetime.utcfromtimestamp(row.timestamp).isoformat()
        price = row.current_price

        if symbol == 'BTCUSDT':
            for feature in feature_cols:
                if feature in ["NIM", "DJI", "SNP"]:
                    prefix = f"stock#{feature}#"
                else:
                    prefix = f"crypto#{feature}USDT#"

                rows = table.scan(row_prefix=prefix.encode())
                latest_row_key, latest_row_data = sorted(rows, key=lambda x: x[0], reverse=True)[0]
                del rows
                last_col = sorted(latest_row_data.keys())[-1]
                latest_prices[feature] = float(latest_row_data[last_col])
                del latest_row_key, latest_row_data, last_col

            if len(latest_prices) == len(feature_cols):
                new_df = spark.createDataFrame(
                    [tuple(latest_prices[col] for col in feature_cols)],
                    feature_cols
                )
                new_df = assembler.transform(new_df)
                pred_df = model.transform(new_df)
                predicted_btc = float(pred_df.select("prediction").collect()[0][0])

                table.put(
                    row_key,
                    {
                        f'prices:{ts_col}'.encode(): str(price).encode(),
                        f'predictions:{ts_col}'.encode(): str(predicted_btc).encode(),
                    }
                )
        else:
            table.put(
                row_key,
                {f'prices:{ts_col}'.encode(): str(price).encode()}
            )

    connection.close()


# =====================================================
# STREAM
# =====================================================

query = (
    parsed.writeStream
    .foreachBatch(write_to_hbase)
    .outputMode("update")
    .start()
)

query.awaitTermination()
