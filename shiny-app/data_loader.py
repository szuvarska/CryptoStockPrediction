import pandas as pd
import happybase
import numpy as np
import socket
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegressionModel

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

def parse_row_key(row_key_bytes):
    """
    Parses HBase row key.
    Expected Format: Symbol#Timestamp#Granularity
    Example: BTC#2025-11-20 00:45:00#1m
    """
    try:
        decoded = row_key_bytes.decode('utf-8')
        parts = decoded.split('#')
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        return None, None, None
    except:
        return None, None, None


def _scan_hbase_table(table_name, symbols=None, target_granularity='1m'):
    """
    Generic function to scan crypto/stock data from HBase.
    """
    conn = get_hbase_connection()
    if not conn:
        return pd.DataFrame()

    data = []
    try:
        table = conn.table(table_name)

        # Strategy: If symbols are known, use prefix scanning (faster).
        # Otherwise, scan the whole table.
        scanners = []
        if symbols:
            for sym in symbols:
                # Prefix scan for specific symbol
                scanners.append(table.scan(row_prefix=f"{sym}#".encode()))
        else:
            scanners = [table.scan()]

        for scanner in scanners:
            for key, value in scanner:
                symbol, timestamp_str, granularity = parse_row_key(key)

                # Filter by granularity (e.g., only load '1m' for charts or '1d' for indicators)
                if target_granularity and granularity != target_granularity:
                    continue

                try:
                    # Map HBase columns (bytes) to Pandas columns
                    # Note: Using .get() with defaults to handle missing columns safely
                    row = {
                        'Symbol': symbol,
                        'Datetime': pd.to_datetime(timestamp_str),

                        # OHLC Mapping
                        'CurrentPrice': float(value.get(b'ohlc:close', 0)),
                        'OpeningPrice': float(value.get(b'ohlc:open', 0)),
                        'HighestDayPrice': float(value.get(b'ohlc:high', 0)),
                        'LowestDayPrice': float(value.get(b'ohlc:low', 0)),

                        # Stock Indicators (Only present in '1d' granularity)
                        'FiftyDayAveragePrice': float(value.get(b'indicators:ma_50_price', 'nan')),
                        'TwoHundredDaysAveragePrice': float(value.get(b'indicators:ma_200_price', 'nan')),

                        # Volume
                        'VolumeTraded': float(value.get(b'ohlc:max_volume', 0))
                    }
                    data.append(row)
                except ValueError:
                    continue  # Skip malformed rows

        df = pd.DataFrame(data)
        if not df.empty:
            df = df.sort_values("Datetime")

        return df

    except Exception as e:
        print(f"Error scanning HBase table {table_name}: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

# --- DATA LOADERS ---

def load_crypto_data(table_name_ignored=None) -> pd.DataFrame:
    """
    Loads Crypto data from 'crypto_index_aggregates'.
    Targets '1m' granularity for high-resolution dashboard charts.
    """
    # We ignore the Hive table name argument to keep function signature compatible with app.py
    return _scan_hbase_table(
        table_name='crypto_index_aggregates',
        symbols=['BTC', 'ETH', 'SOL'],
        target_granularity='1m'
    )

def load_stock_data() -> pd.DataFrame:
    """
    Loads Stock data from 'crypto_index_aggregates'.
    Targets '1d' granularity because MA_50 and MA_200 are only calculated daily.
    """
    return _scan_hbase_table(
        table_name='crypto_index_aggregates',
        symbols=['SNP', 'DJI', 'NIM'],
        target_granularity='1d'
    )


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
