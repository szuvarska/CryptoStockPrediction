import pandas as pd
from shiny import App, ui, reactive, render
from shinywidgets import output_widget, render_plotly  # Correct imports
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_sub, current_date
import os
import socket


# --- 1. PYSPARK CONFIGURATION AND DATA LOADING ---
def load_data_from_hive(hive_table_name: str) -> pd.DataFrame:
    spark = None
    try:
        try:
            socket.gethostbyname("spark-master")
            master_url = "spark://spark-master:7077"
        except:
            master_url = "spark://localhost:7077"

        print(f"Connecting to Spark Master at: {master_url}")

        spark = (
            SparkSession.builder
            .appName(f"ShinyDataLoad-{hive_table_name}")
            .master(master_url)
            .config("spark.cores.max", "1")
            .config("spark.executor.cores", "1")
            .enableHiveSupport()
            .config("hive.metastore.uris", "thrift://hive-metastore:9083")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        spark.sql("USE CryptoPredictions")

        # Cast to string to avoid PyArrow timestamp issues
        query = f"""
            SELECT 
                cast(Datetime as string) as Datetime_Str, 
                CurrentPrice, 
                Symbol 
            FROM {hive_table_name} 
            WHERE PartitionDate >= date_sub(current_date(), 30)
        """
        df_spark = spark.sql(query)
        df_pandas = df_spark.toPandas()

        if not df_pandas.empty:
            df_pandas['Datetime'] = pd.to_datetime(df_pandas['Datetime_Str'])
            df_pandas = df_pandas.sort_values("Datetime")

        return df_pandas

    except Exception as e:
        print(f"ERROR: {e}")
        return pd.DataFrame()
    finally:
        if spark:
            spark.stop()


# --- 2. SHINY APP UI DEFINITION ---
app_ui = ui.page_fluid(
    ui.tags.head(
        ui.tags.title("Crypto Prediction Prototype"),
        ui.tags.style("""
            .app-title {
                font-size: 2.5rem;
                font-weight: 700;
                color: #0d6efd;
                padding-bottom: 10px;
                border-bottom: 2px solid #0d6efd;
                margin-bottom: 20px;
            }
            .sidebar-bg {
                background-color: #f8f9fa;
            }
        """)
    ),
    ui.panel_title(ui.tags.div("Crypto Prediction Prototype (HDFS/Hive Test)", class_="app-title")),

    ui.layout_sidebar(
        # SYNTAX FIX: Positional arguments (content) FIRST, keyword args (class_) LAST
        ui.sidebar(
            ui.h4("Data Controls"),
            ui.input_select(
                "crypto_select",
                "Select Cryptocurrency:",
                {
                    "BTC": "Bitcoin (BTC)",
                    "ETH": "Ethereum (ETH)",
                    "SOL": "Solana (SOL)"
                },
                selected="BTC"
            ),
            ui.hr(),
            ui.h5("Status"),
            ui.output_text("status"),
            class_="sidebar-bg",  # Keyword arg fixed here
            width=300
        ),
        ui.row(
            ui.column(12,
                      ui.h3("Price Trend (Last 7 Days)"),
                      ui.card(
                          # FIX: Use output_widget in UI, render_plotly in Server
                          output_widget("price_chart", width="100%", height="500px")
                      )
                      ),
            ui.column(12,
                      ui.h3("Raw Data Sample (Last 20 Rows)"),
                      ui.card(
                          ui.output_table("raw_table")
                      )
                      )
        )
    )
)


# --- 3. SHINY APP SERVER LOGIC ---
def server(input, output, session):
    # Reactive value to hold the loaded Pandas DataFrame
    all_data = reactive.Value(None)

    @reactive.Effect
    def _():
        # Runs once when the app starts
        print("Initial data load triggered...")
        df = load_data_from_hive("CryptocurrencySnapshot")
        all_data.set(df)

    @render.text
    def status():
        df = all_data.get()
        if df is None:
            return "⏳ Connecting to Spark..."
        elif df.empty:
            return "🔴 Data load failed or table is empty."
        else:
            count = len(df)
            symbols = df['Symbol'].unique() if 'Symbol' in df.columns else []
            return f"🟢 Loaded {count} rows. Symbols: {', '.join(symbols)}"

    @reactive.Calc
    def filtered_data():
        df = all_data.get()
        if df is not None and not df.empty:
            # Filter by selected symbol
            return df[df['Symbol'] == input.crypto_select()]
        return pd.DataFrame()

    @render_plotly
    def price_chart():
        df = filtered_data()
        if df.empty:
            return px.scatter(title="Waiting for data...")

        # Create Plotly figure
        fig = px.line(
            df,
            x="Datetime",
            y="CurrentPrice",
            title=f"{input.crypto_select()} Price Analysis",
            template="plotly_white",
            labels={"CurrentPrice": "Price (USD)", "Datetime": "Time"}
        )
        fig.update_traces(line=dict(width=3, color="#0d6efd"))
        fig.update_layout(margin=dict(l=20, r=20, t=40, b=20))
        return fig

    @render.table
    def raw_table():
        df = filtered_data()
        if df.empty:
            return pd.DataFrame({'Status': ['No Data']})
        # Return tail for display
        return df.sort_values("Datetime", ascending=False).head(20)


app = App(app_ui, server)