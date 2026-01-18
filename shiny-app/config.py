import os
import pandas as pd

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

# --- Time Range & Frequency Settings ---
TIME_RANGE_CONFIG = {
    "1H": {
        "label": "Last 1 Hour",
        "offset": pd.Timedelta(hours=1),
        "freq": "1T",
        "change_label": "Change (1h)",
        "lookahead": pd.Timedelta(minutes=10)
    },
    "24H": {
        "label": "Last 24 Hours",
        "offset": pd.Timedelta(hours=24),
        "freq": "10T",
        "change_label": "Change (24h)",
        "lookahead": pd.Timedelta(hours=6)
    },
    "7D": {
        "label": "Last 7 Days",
        "offset": pd.Timedelta(days=7),
        "freq": "1H",
        "change_label": "Change (7d)",
        "lookahead": pd.Timedelta(days=1)
    },
    "30D": {
        "label": "Last 30 Days",
        "offset": pd.Timedelta(days=30),
        "freq": "6H",
        "change_label": "Change (30d)",
        "lookahead": pd.Timedelta(days=7)
    },
    "ALL": {
        "label": "All Available",
        "offset": None,
        "freq": "1D",
        "change_label": "Change (All Time)",
        "lookahead": pd.Timedelta(days=7)
    }
}

# Helper for UI Select Input (Key -> Label)
TIME_RANGE_CHOICES = {k: v["label"] for k, v in TIME_RANGE_CONFIG.items()}