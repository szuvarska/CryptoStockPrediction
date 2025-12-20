import pandas as pd
import socket
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegressionModel


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


def load_crypto_data(hive_table_name: str) -> pd.DataFrame:
    spark = get_spark_session(f"ShinyLoad-{hive_table_name}")
    if not spark: return pd.DataFrame()
    try:
        query = f"""
                    SELECT 
                        cast(Datetime as string) as Datetime_Str, 
                        CurrentPrice,
                        OpeningPrice,
                        HighestDayPrice,
                        LowestDayPrice,
                        Symbol 
                    FROM {hive_table_name} 
                    ORDER BY Datetime DESC
                """
        df_spark = spark.sql(query)
        df_pandas = df_spark.toPandas()

        if not df_pandas.empty:
            df_pandas['Datetime'] = pd.to_datetime(df_pandas['Datetime_Str'])
            df_pandas['Symbol'] = df_pandas['Symbol'].astype(str).str.upper().str.strip()

            # Ensure numeric types for all price columns
            cols = ['CurrentPrice', 'OpeningPrice', 'HighestDayPrice', 'LowestDayPrice']
            for c in cols:
                df_pandas[c] = pd.to_numeric(df_pandas[c], errors='coerce')

            df_pandas = df_pandas.sort_values("Datetime")

        return df_pandas
    finally:
        pass


def load_stock_data() -> pd.DataFrame:
    spark = get_spark_session("LoadStock")
    if not spark: return pd.DataFrame()
    try:
        query = "SELECT cast(Datetime as string) as Datetime_Str, CurrentPrice, FiftyDayAveragePrice, TwoHundredDaysAveragePrice FROM IndexSnapshot WHERE IndexName = 'SNP' ORDER BY Datetime DESC"
        df = spark.sql(query).toPandas()
        if not df.empty:
            df['Datetime'] = pd.to_datetime(df['Datetime_Str'])
            for c in ['CurrentPrice', 'FiftyDayAveragePrice', 'TwoHundredDaysAveragePrice']:
                df[c] = pd.to_numeric(df[c], errors='coerce')
            df = df.sort_values("Datetime")
        return df
    finally:
        pass


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
    spark = get_spark_session("LoadModel")
    # docelowo "hdfs://namenode:8020/models/btc_model"
    # Model przyjmuje dane:
    #   |timestamp|BTC|NIM|SNP|DJI|SOL|ETH|BTC_next_close|NIM_next_close|
    #   SNP_next_close|DJI_next_close|SOL_next_close|ETH_next_close|
    # i przewiduje BTC_next_close w oknie 1m
    return LinearRegressionModel.load(path)
