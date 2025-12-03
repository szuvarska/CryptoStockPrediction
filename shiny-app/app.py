import pandas as pd
import numpy as np
from shiny import App, ui, reactive, render
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_sub, current_date
import socket


# --- 1. DATA LOADING ---
def load_data_from_hive(hive_table_name: str) -> pd.DataFrame:
    spark = None
    try:
        try:
            socket.gethostbyname("spark-master")
            master_url = "spark://spark-master:7077"
        except:
            master_url = "spark://localhost:7077"

        # Suppress initial noise
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
        spark.sparkContext.setLogLevel("ERROR")
        spark.sql("USE CryptoPredictions")

        query = f"""
            SELECT 
                cast(Datetime as string) as Datetime_Str, 
                CurrentPrice, 
                Symbol 
            FROM {hive_table_name} 
            ORDER BY Datetime DESC
        """
        df_spark = spark.sql(query)
        df_pandas = df_spark.toPandas()

        if not df_pandas.empty:
            df_pandas['Datetime'] = pd.to_datetime(df_pandas['Datetime_Str'])
            df_pandas['Symbol'] = df_pandas['Symbol'].astype(str).str.upper().str.strip()
            df_pandas['CurrentPrice'] = pd.to_numeric(df_pandas['CurrentPrice'], errors='coerce')
            df_pandas = df_pandas.sort_values("Datetime")

        return df_pandas

    except Exception as e:
        print(f"ERROR: {e}")
        return pd.DataFrame()
    finally:
        # Silent shutdown to avoid traceback noise
        if spark:
            try:
                spark.stop()
            except:
                pass


# --- 2. HELPER: PLOT TO HTML (The Fix) ---
def render_plotly_html(fig, height=None):
    """Converts a Plotly figure to an HTML string for robust rendering."""
    if fig is None: return ui.HTML("")
    # responsive=True makes it fill the container
    html = fig.to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True})
    # Wrap in a div with defined height if needed
    style = f"height: {height};" if height else "width: 100%; height: 100%;"
    return ui.HTML(f'<div style="{style}">{html}</div>')


# --- 3. CUSTOM CSS ---
custom_css = """
    h2 { border-bottom: 2px solid #0d6efd; padding-bottom: 10px; color: #0d6efd; }
    .card-header { font-weight: bold; }
    .table th, .table td { text-align: center; vertical-align: middle; }
    .table th { background-color: #f8f9fa; }
    /* Padding for EDA Header */
    .tab-header { margin-top: 30px; margin-bottom: 20px; padding-left: 15px; border-left: 5px solid #0d6efd; background-color: #f8f9fa; padding-top: 10px; padding-bottom: 10px; }
"""

# --- 4. UI ---
app_ui = ui.page_fluid(
    ui.head_content(ui.tags.style(custom_css)),
    ui.h2("Crypto Analytics"),

    ui.navset_tab(
        # --- TAB 1: DASHBOARD ---
        ui.nav_panel("Dashboard",
                     ui.h3("Stock Market Overview", class_="tab-header"),
                     ui.layout_sidebar(
                         ui.sidebar(
                             ui.h4("Filters"),
                             ui.input_select("crypto_select", "Asset:",
                                             {"BTC": "Bitcoin", "ETH": "Ethereum", "SOL": "Solana"}, selected="BTC"),
                             ui.input_select("time_range", "Time Range:",
                                             {"24H": "Last 24 Hours", "7D": "Last 7 Days", "30D": "Last 30 Days",
                                              "ALL": "All Available"}, selected="7D"),
                             ui.hr(),
                             ui.output_text("status_text")
                         ),
                         ui.row(
                             ui.column(4, ui.card(ui.card_header("Current Price (USD)"),
                                                  ui.output_text("val_price", inline=True),
                                                  style="text-align: center; min-height: 120px;")),
                             ui.column(4,
                                       ui.card(ui.card_header("Change (%)"), ui.output_text("val_change", inline=True),
                                               style="text-align: center; min-height: 120px;")),
                             ui.column(4, ui.card(ui.card_header("Volatility (StdDev)"),
                                                  ui.output_text("val_vol", inline=True),
                                                  style="text-align: center; min-height: 120px;")),
                         ),
                         ui.br(),
                         ui.card(
                             ui.card_header("Price Trend Analysis"),
                             # CHANGED: output_ui instead of output_widget
                             ui.output_ui("price_chart_view"),
                             full_screen=True
                         )
                     )
                     ),

        # --- TAB 2: EDA (Integrated) ---
        ui.nav_panel("EDA",
                     ui.h3("Exploratory Data Analysis", class_="tab-header"),
                     ui.layout_sidebar(
                         ui.sidebar(
                             ui.h4("Filters"),
                             ui.input_select("eda_asset", "Asset:",
                                             {"BTC": "Bitcoin", "ETH": "Ethereum", "SOL": "Solana"}, selected="BTC")
                         ),
                         ui.row(
                             ui.card(
                                 ui.card_header("Correlation Matrix"),
                                 ui.output_ui("corr_chart_view")
                             ),
                             ui.card(
                                 ui.card_header("Return Distribution"),
                                 ui.output_ui("dist_chart_view")
                             )
                         )
                     )
                     ),

        # --- TAB 3: RAW DATA ---
        ui.nav_panel("Raw Data",
                     ui.h3("Raw Data", class_="tab-header"),
                     ui.layout_sidebar(
                         ui.sidebar(
                             ui.h4("Filters"),
                             ui.input_select("raw_asset", "Asset:",
                                             {"ALL": "All", "BTC": "Bitcoin", "ETH": "Ethereum", "SOL": "Solana"},
                                             selected="ALL"),
                             ui.download_button("download_csv", "Download CSV")
                         ),
                         ui.card(ui.card_header("Historical Data Table"), ui.output_table("raw_table"))
                     )
                     )
    )
)


# --- 5. SERVER ---
def server(input, output, session):
    all_data = reactive.Value(None)

    @reactive.Effect
    def _():
        df = load_data_from_hive("CryptocurrencySnapshot")
        all_data.set(df)

    @reactive.Calc
    def dashboard_data():
        df = all_data.get()
        if df is not None and not df.empty:
            df_sub = df[df['Symbol'] == input.crypto_select()]

            # Simplified Date Filter
            if input.time_range() == "24H":
                cutoff = df_sub['Datetime'].max() - pd.Timedelta(hours=24)
                return df_sub[df_sub['Datetime'] >= cutoff].sort_values("Datetime")
            elif input.time_range() == "7D":
                cutoff = df_sub['Datetime'].max() - pd.Timedelta(days=7)
                return df_sub[df_sub['Datetime'] >= cutoff].sort_values("Datetime")
            elif input.time_range() == "30D":
                cutoff = df_sub['Datetime'].max() - pd.Timedelta(days=30)
                return df_sub[df_sub['Datetime'] >= cutoff].sort_values("Datetime")

            return df_sub.sort_values("Datetime")
        return pd.DataFrame()

    # --- PLOT 1: PRICE TREND (HTML) ---
    @render.ui
    def price_chart_view():
        df = dashboard_data()
        if df.empty: return ui.div("No Data Available", style="color:gray; text-align:center; padding:50px;")

        fig = px.line(df, x="Datetime", y="CurrentPrice", template="plotly_white")
        fig.update_layout(
            title=dict(text=f"{input.crypto_select()} Trend", x=0.01),
            xaxis_title="Date", yaxis_title="Price ($)",
            margin=dict(l=60, r=20, t=40, b=20),
            hovermode="x unified",
            height=500
        )
        fig.update_xaxes(tickformat="%b %d %H:%M")
        fig.update_yaxes(tickprefix="$")
        return render_plotly_html(fig, height="500px")

    # --- PLOT 2: CORRELATION (HTML) ---
    @render.ui
    def corr_chart_view():
        df = all_data.get()
        if df is None or df.empty: return ui.div("No Data")

        pivot_df = df.pivot_table(index='Datetime', columns='Symbol', values='CurrentPrice', aggfunc='mean')
        returns_df = pivot_df.pct_change().dropna()

        if returns_df.empty: return ui.div("Not enough overlapping data for correlation")

        fig = px.imshow(
            returns_df.corr(),
            text_auto=".2f", aspect="auto", color_continuous_scale="RdBu_r", zmin=-1, zmax=1,
            title="Asset Correlation Matrix (Daily Returns)",
            labels=dict(x="Asset", y="Asset", color="Corr")
        )
        # Increased Left Margin for Y-axis Labels as requested
        fig.update_layout(height=400, margin=dict(l=100, r=20, t=60, b=20))
        fig.update_yaxes(ticksuffix="  ")

        return render_plotly_html(fig, height="400px")

    # --- PLOT 3: DISTRIBUTION (HTML) ---
    @render.ui
    def dist_chart_view():
        df = all_data.get()
        if df is None or df.empty: return ui.div("No Data")

        asset_df = df[df['Symbol'] == input.eda_asset()].copy()
        if asset_df.empty: return ui.div("No Data for Asset")

        asset_df['Return'] = asset_df['CurrentPrice'].pct_change() * 100

        fig = px.histogram(
            asset_df, x="Return", nbins=40,
            title=f"{input.crypto_select()} Return Distribution",
            template="plotly_white",
            color_discrete_sequence=["#0d6efd"]
        )
        fig.update_layout(
            height=400, margin=dict(l=60, r=20, t=60, b=20), bargap=0.1,
            xaxis_title="Return (%)", yaxis_title="Frequency"
        )
        return render_plotly_html(fig, height="400px")

    # --- METRICS ---
    @render.text
    def val_price():
        df = dashboard_data()
        return f"${df['CurrentPrice'].iloc[-1]:,.2f}" if not df.empty else "$ -"

    @render.text
    def val_change():
        df = dashboard_data()
        if df.empty: return "-"
        chg = ((df['CurrentPrice'].iloc[-1] - df['CurrentPrice'].iloc[0]) / df['CurrentPrice'].iloc[0]) * 100
        return f"{chg:+.2f}%"

    @render.text
    def val_vol():
        df = dashboard_data()
        return f"${df['CurrentPrice'].std():.2f}" if not df.empty else "-"

    @render.text
    def status_text():
        df = all_data.get()
        return f"🟢 Loaded {len(df)} rows" if df is not None and not df.empty else "Connecting..."

    # --- TABLES ---
    @render.table
    def raw_table():
        df = all_data.get()
        if df is None or df.empty: return pd.DataFrame()
        if input.raw_asset() != "ALL": df = df[df['Symbol'] == input.raw_asset()]

        disp = df.copy()
        disp['Datetime'] = disp['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        disp['CurrentPrice'] = disp['CurrentPrice'].apply(lambda x: f"${x:,.2f}")
        return disp[['Datetime', 'Symbol', 'CurrentPrice']].sort_values("Datetime", ascending=False).head(50)

    @render.download(filename="data.csv")
    def download_csv():
        yield all_data.get().drop(columns=['Datetime_Str'], errors='ignore').to_csv(index=False)


app = App(app_ui, server)
