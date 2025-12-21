import pandas as pd
import happybase
import numpy as np
import socket
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegressionModel
from datetime import datetime, timedelta
from line_profiler import profile

# --- SPARK CONNECTION HELPER ---

def get_spark_session(app_name):
    try:
        try:
            socket.gethostbyname("spark-master")
            master_url = "spark://spark-master:7077"
        except:
            master_url = "spark://localhost:7077"

        spark = (
            SparkSession.builder
            .appName(app_name)
            .master(master_url)
            .config("spark.cores.max", "1")
            .config("spark.executor.cores", "1")
            .enableHiveSupport()
            .config("hive.metastore.uris", "thrift://hive-metastore:9083")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        spark.sql("USE CryptoPredictions")
        return spark
    except Exception as e:
        print(f"Error creating Spark session: {e}")
        return None

# --- HBASE CONNECTION HELPER ---

def get_hbase_connection(host='hbase'):
    """Establishes a connection to HBase using HappyBase."""
    try:
        connection = happybase.Connection(host=host)
        connection.open()
        return connection
    except Exception as e:
        print(f"Error connecting to HBase: {e}")
        return None

@profile
def load_unified_data():
    conn = get_hbase_connection()
    if not conn: return pd.DataFrame()

    table = conn.table('crypto_index_aggregates')

    # --- STEP 1: Find the Global Max Timestamp ---
    # We scan the table briefly to find the most recent entry
    # Scanning in reverse is the fastest way to find the 'latest' row
    print("Finding latest data point in HBase...")
    latest_rows = list(table.scan(limit=5, reverse=True))
    if not latest_rows:
        conn.close()
        return pd.DataFrame()

    # Extract max_ts from the key of the first row returned by reverse scan
    # Key format: Symbol#Timestamp#Granularity
    max_ts_str = latest_rows[0][0].decode().split('#')[1]
    max_ts = pd.to_datetime(max_ts_str)

    # --- STEP 2: Define Relative Boundaries ---
    t_24h_limit = max_ts - timedelta(hours=24)
    t_7d_limit = max_ts - timedelta(days=7)

    crypto_symbols = ['BTC', 'ETH', 'SOL']
    stock_symbols = ['SNP', 'DJI', 'NIM']
    all_symbols = crypto_symbols + stock_symbols

    data_rows = []

    # --- STEP 3: Tiered Scan ---
    for symbol in all_symbols:
        asset_type = 'Crypto' if symbol in crypto_symbols else 'Stock'

        # Scan with prefix for efficiency
        for key, value in table.scan(row_prefix=f"{symbol}#".encode()):
            parts = key.decode().split('#')
            if len(parts) != 3: continue

            ts = pd.to_datetime(parts[1])
            gran = parts[2]

            # Logic: 1m for last 24h, 10m for last 7d (minus 24h), 1d for rest
            keep = False
            if gran == '1m' and ts > t_24h_limit:
                keep = True
            elif gran == '10m' and t_7d_limit < ts <= t_24h_limit:
                keep = True
            elif gran == '1d' and ts <= t_7d_limit:
                keep = True

            if keep:
                data_rows.append({
                    'Datetime': ts,
                    'Symbol': symbol,
                    'Type': asset_type,

                    # OHLC Mapping
                    'CurrentPrice': float(value.get(b'ohlc:close', 0)),
                    'OpeningPrice': float(value.get(b'ohlc:open', 0)),
                    'HighestDayPrice': float(value.get(b'ohlc:high', 0)),
                    'LowestDayPrice': float(value.get(b'ohlc:low', 0)),

                    # Stock Indicators
                    'FiftyDayAveragePrice': float(value.get(b'indicators:ma_50_price', np.nan)),
                    'TwoHundredDaysAveragePrice': float(value.get(b'indicators:ma_200_price', np.nan)),

                    # Volume
                    'VolumeTraded': float(value.get(b'ohlc:max_volume', 0))
                })

    conn.close()
    if not data_rows: return pd.DataFrame()

    # --- STEP 4: Merge and Pad Moving Averages ---
    df = pd.DataFrame(data_rows).sort_values(['Symbol', 'Datetime'])

    # Fill NaNs: Group by symbol so BTC doesn't get SNP's averages
    # ffill() carries the last known daily MA forward into the 1m/10m rows
    # bfill() ensures the very first rows aren't empty if data starts mid-day
    df['FiftyDayAveragePrice'] = df.groupby('Symbol')['FiftyDayAveragePrice'].ffill().bfill()
    df['TwoHundredDaysAveragePrice'] = df.groupby('Symbol')['TwoHundredDaysAveragePrice'].ffill().bfill()

    return df


def load_forex_data() -> pd.DataFrame:
    spark = get_spark_session("LoadForex")
    if not spark: return pd.DataFrame()
    try:
        # Assuming USDExchangeRates has a 'Date' column we cast to string or timestamp
        query = "SELECT cast(Date as string) as Datetime_Str, VolumeTraded FROM USDExchangeRates"
        df = spark.sql(query).toPandas()
        if not df.empty:
            df['Datetime'] = pd.to_datetime(df['Datetime_Str'])
            df['VolumeTraded'] = pd.to_numeric(df['VolumeTraded'], errors='coerce')
            df = df.sort_values("Datetime")
        return df
    finally:
        pass


def load_spark_model(path: str):
    spark = get_spark_session("LoadModel1")
    # docelowo "hdfs://namenode:8020/models/btc_model"
    # Model przyjmuje dane:
    #   |timestamp|BTC|NIM|SNP|DJI|SOL|ETH|BTC_next_close|NIM_next_close|
    #   SNP_next_close|DJI_next_close|SOL_next_close|ETH_next_close|
    # i przewiduje BTC_next_close w oknie 1m
    return LinearRegressionModel.load(path)

# --- Preprocess do modelu ---

import happybase
from datetime import datetime

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lead, first
from pyspark.ml.feature import VectorAssembler

def prepare_single_row_for_prediction(
    spark: SparkSession,
    hbase_host: str = "hbase",
    table_name: str = "crypto_index_aggregates",
):
    symbols = ["BTC", "NIM", "SNP", "DJI", "SOL", "ETH"]
    feature_cols = ["NIM", "SNP", "DJI", "SOL", "ETH"]
    label_col = "BTC_next_close"

    # --- HBase ---
    connection = happybase.Connection(host=hbase_host)
    table = connection.table(table_name)

    rows = []
    for key, data in table.scan():
        if b"#1m" not in key:
            continue

        try:
            symbol, ts_str, interval = key.decode().split("#")
            timestamp = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue

        row_dict = {
            "symbol": symbol,
            "timestamp": timestamp,
            "interval": interval,
        }

        for col_name, val in data.items():
            name = col_name.decode()
            try:
                row_dict[name] = float(val.decode())
            except ValueError:
                row_dict[name] = val.decode()

        rows.append(Row(**row_dict))

    df = spark.createDataFrame(rows)

    # --- Filtr symboli ---
    df = df.filter(col("symbol").isin(symbols))

    # --- next_close ---
    window = Window.partitionBy("symbol").orderBy("timestamp")
    df = df.withColumn("next_close", lead("ohlc:close", 1).over(window))

    # --- Pivot close ---
    df_close = (
        df.groupBy("timestamp")
        .pivot("symbol", symbols)
        .agg(first("ohlc:close"))
    )

    # --- Pivot next_close ---
    df_next = (
        df.groupBy("timestamp")
        .pivot("symbol", symbols)
        .agg(first("next_close"))
    )

    for sym in symbols:
        if sym in df_next.columns:
            df_next = df_next.withColumnRenamed(sym, f"{sym}_next_close")

    df_pivot = df_close.join(df_next, on="timestamp", how="inner")

    for c in df_pivot.columns:
        if c != "timestamp":
            df_pivot = df_pivot.withColumn(c, col(c).cast("double"))

    df_pivot = df_pivot.dropna(subset=feature_cols + [label_col])
 
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )

    df_ml = assembler.transform(df_pivot)
    df_ml = df_ml.select(
        "timestamp",
        "features",
        col(label_col).alias("label")
    )

    last_row = (
        df_ml
        .orderBy(col("timestamp").desc())
        .limit(1)
    )

    return last_row