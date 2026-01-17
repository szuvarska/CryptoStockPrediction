import pandas as pd
import happybase
import numpy as np
import socket
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegressionModel
from datetime import datetime, timedelta

# --- LOCAL IMPORTS ---
from config import HBASE_HOST, SPARK_MASTER, HIVE_METASTORE, CRYPTO_SYMBOLS, ALL_SYMBOLS
from utils import should_keep_record

# --- SPARK CONNECTION HELPER ---

def get_spark_session(app_name):
    try:
        try:
            socket.gethostbyname("spark-master")
            master_url = SPARK_MASTER
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

def get_hbase_connection(host=HBASE_HOST):
    """Establishes a connection to HBase using HappyBase."""
    try:
        connection = happybase.Connection(host=host)
        connection.open()
        return connection
    except Exception as e:
        print(f"Error connecting to HBase: {e}")
        return None


# --- PART 1: HEAVY HISTORY LOAD (Run Once) ---
def load_historical_data(limit=None):
    """
    Fetches historical aggregates (1m, 10m, 1d) from 'crypto_index_aggregates'.

    Args:
        limit (int, optional): If set, fetches only the last N rows per symbol.
                               Crucial for fast testing.
    """
    conn = get_hbase_connection()
    if not conn: return pd.DataFrame()

    table = conn.table('crypto_index_aggregates')

    # 1. Find Max Timestamp (Scan reverse limit 5)
    latest_rows = list(table.scan(limit=5, reverse=True))
    if not latest_rows:
        conn.close()
        return pd.DataFrame()

    max_ts_str = latest_rows[0][0].decode().split('#')[1]
    max_ts = pd.to_datetime(max_ts_str)

    # 2. Define Boundaries
    t_24h_limit = max_ts - timedelta(hours=24)
    t_7d_limit = max_ts - timedelta(days=7)

    data_rows = []

    # 3. Scan History
    for symbol in ALL_SYMBOLS:
        asset_type = 'Crypto' if symbol in CRYPTO_SYMBOLS else 'Stock'

        # CONFIG:
        # If limit is set, we scan REVERSE to get the newest N rows.
        scan_kwargs = {'row_prefix': f"{symbol}#".encode()}

        if limit:
            scan_kwargs['limit'] = limit
            scan_kwargs['reverse'] = True

        for key, value in table.scan(**scan_kwargs):
            parts = key.decode().split('#')
            if len(parts) != 3: continue

            ts = pd.to_datetime(parts[1])
            # Enforce Naive Timestamp (Crucial for subtraction safety)
            if ts.tz is not None: ts = ts.tz_localize(None)

            gran = parts[2]

            # Use Helper Logic
            if should_keep_record(ts, gran, t_24h_limit, t_7d_limit, force_keep=bool(limit)):
                data_rows.append({
                    'Datetime': ts,
                    'Symbol': symbol,
                    'Type': asset_type,
                    'CurrentPrice': float(value.get(b'ohlc:close', 0)),
                    'OpeningPrice': float(value.get(b'ohlc:open', 0)),
                    'HighestDayPrice': float(value.get(b'ohlc:high', 0)),
                    'LowestDayPrice': float(value.get(b'ohlc:low', 0)),
                    'FiftyDayAveragePrice': float(value.get(b'indicators:ma_50_price', np.nan)),
                    'TwoHundredDaysAveragePrice': float(value.get(b'indicators:ma_200_price', np.nan)),
                    'VolumeTraded': float(value.get(b'ohlc:max_volume', 0))
                })

    conn.close()
    if not data_rows: return pd.DataFrame()

    df = pd.DataFrame(data_rows).sort_values(['Symbol', 'Datetime'])

    # Fill MAs for continuity
    df['FiftyDayAveragePrice'] = df.groupby('Symbol')['FiftyDayAveragePrice'].ffill().bfill()
    df['TwoHundredDaysAveragePrice'] = df.groupby('Symbol')['TwoHundredDaysAveragePrice'].ffill().bfill()

    return df


# --- PART 2: LIGHTWEIGHT REAL-TIME POLL (Run Frequently) ---
def get_latest_ticks():
    """
    Fetches ONLY the current instantaneous price from the 'prices' table.
    Uses direct RowKey lookups (O(1)) instead of Scans for minimal latency.
    """
    conn = get_hbase_connection()
    if not conn: return pd.DataFrame()

    table = conn.table('prices')
    today_str = datetime.now().strftime("%Y%m%d")

    new_rows = []

    for symbol in ALL_SYMBOLS:
        asset_type = 'crypto' if symbol in CRYPTO_SYMBOLS else 'stock'

        # Handle USDT Suffix mismatch (Writer uses BTCUSDT, App uses BTC)
        search_symbol = f"{symbol}USDT" if asset_type == 'crypto' else symbol

        # KEY CONSTRUCTION: Direct lookup
        row_key = f"{asset_type}#{search_symbol}#{today_str}".encode()

        try:
            row = table.row(row_key)
        except Exception:
            continue

        if not row: continue

        # The columns are timestamps (e.g., prices:2025-01-01T12:00:00)
        # They are sorted lexicographically, so the last key is the latest time.
        sorted_cols = sorted(row.keys())
        if not sorted_cols: continue

        latest_col = sorted_cols[-1]
        try:
            latest_val = float(row[latest_col])
        except (ValueError, TypeError):
            continue

        # Parse timestamp from column name 'prices:2025...'
        # Column format is usually 'prices:iso_timestamp'
        try:
            ts_str = latest_col.decode().split(':', 1)[1]
            ts = pd.to_datetime(ts_str)
            if ts.tz is not None: ts = ts.tz_localize(None)
        except Exception:
            continue

        # Create a row compatible with the history dataframe
        # Note: We save it as 'symbol' (BTC) not 'search_symbol' (BTCUSDT)
        # so it aligns with the historical data.
        new_rows.append({
            'Datetime': ts,
            'Symbol': symbol,
            'Type': 'Crypto' if asset_type == 'crypto' else 'Stock',
            'CurrentPrice': latest_val,
            # For a single tick, Open/High/Low are effectively the current price
            'OpeningPrice': latest_val,
            'HighestDayPrice': latest_val,
            'LowestDayPrice': latest_val,
            # Leave MAs NaN, they will be filled by ffill in the app
            'FiftyDayAveragePrice': np.nan,
            'TwoHundredDaysAveragePrice': np.nan,
            'VolumeTraded': 0
        })

    conn.close()
    return pd.DataFrame(new_rows)

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