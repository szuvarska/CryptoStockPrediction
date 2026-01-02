
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, coalesce, lit
from pyspark.sql.types import *
from datetime import datetime
import happybase
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------- 
# Spark Session
# ------------------------------------------------- 
spark = (
    SparkSession.builder
    .appName("StockKafkaToHBase")
    .master("spark://spark-master:7077")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------- 
# Kafka JSON Schema (NULL-SAFE)
# ------------------------------------------------- 
schema = StructType([
    StructField("currency", StringType(), True),
    StructField("dayHigh", DoubleType(), True),
    StructField("dayLow", DoubleType(), True),
    StructField("exchange", StringType(), True),
    StructField("fiftyDayAverage", DoubleType(), True),
    StructField("lastPrice", DoubleType(), True),
    StructField("lastVolume", LongType(), True),
    StructField("marketCap", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("previousClose", DoubleType(), True),
    StructField("quoteType", StringType(), True),
    StructField("regularMarketPreviousClose", DoubleType(), True),
    StructField("shares", StringType(), True),
    StructField("tenDayAverageVolume", LongType(), True),
    StructField("threeMonthAverageVolume", LongType(), True),
    StructField("timezone", StringType(), True),
    StructField("twoHundredDayAverage", DoubleType(), True),
    StructField("yearChange", DoubleType(), True),
    StructField("yearHigh", DoubleType(), True),
    StructField("yearLow", DoubleType(), True),
    StructField("ticker", StringType(), True),
    StructField("fetch_timestamp", StringType(), True)
])

# ------------------------------------------------- 
# Read from Kafka
# ------------------------------------------------- 
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "stock-prices")
    .option("startingOffsets", "latest")
    .load()
)

# ------------------------------------------------- 
# Parse JSON & Timestamp with better error handling
# ------------------------------------------------- 
parsed = (
    df.select(from_json(col("value").cast("string"), schema).alias("j"))
    .select("j.*")
    .withColumn(
        "event_ts",
        to_timestamp(col("fetch_timestamp"), "yyyy-MM-dd HH:mm:ss")
    )
    # Add debug column to track nulls
    .withColumn("is_valid", 
        (col("event_ts").isNotNull()) & (col("lastPrice").isNotNull())
    )
)

# ------------------------------------------------- 
# Write to HBase with better error handling
# ------------------------------------------------- 
def write_to_hbase(batch_df, batch_id):
    """Write batch to HBase with comprehensive error handling"""
    
    # Filter out invalid records and log them
    valid_df = batch_df.filter(col("is_valid"))
    invalid_count = batch_df.count() - valid_df.count()
    
    if invalid_count > 0:
        logger.warning(f"Batch {batch_id}: Filtered {invalid_count} invalid records")
        # Log sample of invalid records for debugging
        invalid_df = batch_df.filter(~col("is_valid"))
        invalid_df.select("ticker", "event_ts", "lastPrice").show(truncate=False)
    
    if valid_df.count() == 0:
        logger.info(f"Batch {batch_id}: No valid records to write")
        return
    
    connection = None
    try:
        connection = happybase.Connection('hbase', timeout=10000)
        table = connection.table('market_prices')
        
        write_count = 0
        error_count = 0
        
        for row in valid_df.collect():
            try:
                ticker = row.ticker.replace("^", "")
                exchange = row.exchange
                date = row.event_ts.strftime("%Y%m%d")
                row_key = f"stock#{exchange}#{ticker}#{date}"
                ts_col = row.event_ts.isoformat()
                price = float(row.lastPrice)
                
                # Fetch existing row
                existing = table.row(row_key.encode())
                
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
                
                # Prepare data for write
                data = {
                    f'prices:{ts_col}'.encode(): str(price).encode(),
                    b'stats:open': open_price,
                    b'stats:high': str(new_high).encode(),
                    b'stats:low': str(new_low).encode(),
                    b'stats:close': str(price).encode(),
                }
                
                table.put(row_key.encode(), data)
                write_count += 1
                
                logger.info(f"Batch {batch_id}: Wrote {row_key}")
                
            except Exception as e:
                error_count += 1
                logger.error(f"Batch {batch_id}: Error writing {row.ticker}: {str(e)}")
        
        logger.info(f"Batch {batch_id}: Completed with {write_count} successes, {error_count} errors")
        
    except Exception as e:
        logger.error(f"Batch {batch_id}: HBase connection error: {str(e)}")
    finally:
        if connection:
            connection.close()

# ------------------------------------------------- 
# Start Streaming Query
# ------------------------------------------------- 
query = (
    parsed
    .writeStream
    .foreachBatch(write_to_hbase)
    .outputMode("update")
    .start()
)

query.awaitTermination()
