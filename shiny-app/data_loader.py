import pandas as pd
import happybase
import numpy as np
import socket
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegressionModel
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

# --- LOCAL IMPORTS ---
from config import HBASE_HOST, SPARK_MASTER, HIVE_METASTORE, CRYPTO_SYMBOLS, ALL_SYMBOLS
from utils import filter_data_vectorized

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
            # .config("hive.metastore.uris", "thrift://hive-metastore:9083")
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

def fetch_single_symbol_history(symbol, limit, asset_type):
    """
    Worker function to fetch history for a single symbol.
    Runs in its own thread with its own HBase connection.
    """
    conn = get_hbase_connection()
    if not conn: return []

    table = conn.table('crypto_index_aggregates')
    rows_data = []

    try:
        scan_kwargs = {'row_prefix': f"{symbol}#".encode()}
        if limit:
            scan_kwargs['limit'] = limit
            scan_kwargs['reverse'] = True

        for key, value in table.scan(**scan_kwargs):
            try:
                # Fast manual parsing
                key_str = key.decode()
                parts = key_str.split('#')
                if len(parts) != 3: continue

                # Store raw strings/floats to minimize processing in-thread
                rows_data.append({
                    'Datetime': parts[1],  # Parsed later in bulk
                    'Granularity': parts[2],
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
            except Exception:
                continue
    finally:
        conn.close()

    return rows_data


def load_historical_data(limit=None):
    """
    Loads history using Parallel Threads (IO-bound optimization).
    """
    # 1. Determine Global Time Boundaries (Single cheap lookup)
    conn = get_hbase_connection()
    if not conn: return pd.DataFrame()

    try:
        table = conn.table('crypto_index_aggregates')
        latest_rows = list(table.scan(limit=5, reverse=True))
        if not latest_rows: return pd.DataFrame()

        max_ts_str = latest_rows[0][0].decode().split('#')[1]
        max_ts = pd.to_datetime(max_ts_str)
        t_24h_limit = max_ts - timedelta(hours=24)
        t_7d_limit = max_ts - timedelta(days=7)
    finally:
        conn.close()

    all_raw_rows = []

    # 2. Parallel Execution
    # Spin up one thread per symbol (e.g., 6 threads) to fetch data concurrently
    with ThreadPoolExecutor(max_workers=len(ALL_SYMBOLS)) as executor:
        future_to_symbol = {}
        for symbol in ALL_SYMBOLS:
            asset_type = 'Crypto' if symbol in CRYPTO_SYMBOLS else 'Stock'
            future = executor.submit(fetch_single_symbol_history, symbol, limit, asset_type)
            future_to_symbol[future] = symbol

        for future in as_completed(future_to_symbol):
            try:
                data = future.result()
                all_raw_rows.extend(data)
            except Exception as e:
                print(f"Error fetching {future_to_symbol[future]}: {e}")

    # 3. Post-Processing (Vectorized)
    if not all_raw_rows: return pd.DataFrame()

    df = pd.DataFrame(all_raw_rows)

    # Bulk Datetime Conversion
    df['Datetime'] = pd.to_datetime(df['Datetime'])
    if df['Datetime'].dt.tz is not None:
        df['Datetime'] = df['Datetime'].dt.tz_localize(None)

    # Vectorized Filter (skip if limit is set for testing)
    if not limit:
        df = filter_data_vectorized(df, t_24h_limit, t_7d_limit)

    # Cleanup
    if 'Granularity' in df.columns:
        df = df.drop(columns=['Granularity'])

    df = df.sort_values(['Symbol', 'Datetime'])
    df['FiftyDayAveragePrice'] = df.groupby('Symbol')['FiftyDayAveragePrice'].ffill().bfill()
    df['TwoHundredDaysAveragePrice'] = df.groupby('Symbol')['TwoHundredDaysAveragePrice'].ffill().bfill()

    return df


# --- PART 2: LIGHTWEIGHT REAL-TIME POLL (Run Frequently) ---

def get_latest_ticks():
    """
    Fetches latest ticks using Batch Lookup (Latency optimization).
    """
    conn = get_hbase_connection()
    if not conn: return pd.DataFrame()

    table = conn.table('prices')
    today_str = datetime.now().strftime("%Y%m%d")

    # 1. Pre-calculate all keys
    keys_map = {}  # Map row_key -> (symbol, asset_type)
    keys_list = []

    for symbol in ALL_SYMBOLS:
        asset_type = 'crypto' if symbol in CRYPTO_SYMBOLS else 'stock'
        search_symbol = f"{symbol}USDT" if asset_type == 'crypto' else symbol
        row_key = f"{asset_type}#{search_symbol}#{today_str}".encode()

        keys_list.append(row_key)
        keys_map[row_key] = (symbol, asset_type)

    # 2. Batch Fetch (Single Network Round-Trip)
    rows = table.rows(keys_list)

    new_rows_data = []

    # 3. Process Batch Results
    for row_key, data in rows:
        symbol, raw_asset_type = keys_map.get(row_key)

        # Sort columns to find latest tick
        sorted_cols = sorted(data.keys())
        if not sorted_cols: continue

        latest_col = sorted_cols[-1]
        try:
            latest_val = float(data[latest_col])

            ts_str = latest_col.decode().split(':', 1)[1]
            ts = pd.to_datetime(ts_str)
            if ts.tz is not None: ts = ts.tz_localize(None)

            new_rows_data.append({
                'Datetime': ts,
                'Symbol': symbol,
                'Type': 'Crypto' if raw_asset_type == 'crypto' else 'Stock',
                'CurrentPrice': latest_val,
                'OpeningPrice': latest_val,
                'HighestDayPrice': latest_val,
                'LowestDayPrice': latest_val,
                'FiftyDayAveragePrice': np.nan,
                'TwoHundredDaysAveragePrice': np.nan,
                'VolumeTraded': 0
            })
        except Exception:
            continue

    conn.close()
    return pd.DataFrame(new_rows_data)

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