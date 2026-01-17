import os

# --- Connection Settings ---
HBASE_HOST = os.getenv("HBASE_HOST", "hbase")
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
HIVE_METASTORE = os.getenv("HIVE_URI", "thrift://hive-metastore:9083")

# --- Asset Definitions ---
# Dictionary maps Symbol -> Display Name (for UI)
CRYPTO_ASSETS = {
    "BTC": "Bitcoin",
    "ETH": "Ethereum",
    "SOL": "Solana"
}

STOCK_ASSETS = {
    "SNP": "S&P 500",
    "NIM": "NASDAQ",
    "DJI": "Dow Jones"
}

# Combined dictionary for easy lookups
ALL_ASSETS = {**CRYPTO_ASSETS, **STOCK_ASSETS}

# Lists for iteration
CRYPTO_SYMBOLS = list(CRYPTO_ASSETS.keys())
STOCK_SYMBOLS = list(STOCK_ASSETS.keys())
ALL_SYMBOLS = CRYPTO_SYMBOLS + STOCK_SYMBOLS