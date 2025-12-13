import pandas as pd
import numpy as np
from shiny import App, ui, reactive, render
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_sub, current_date
import socket
from eda_plots import plot_correlation_matrix, plot_return_distribution, plot_stock_indicators, plot_forex_volume


# --- 1. DATA LOADING ---
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

def load_data_from_hive(hive_table_name: str) -> pd.DataFrame:
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
    finally: pass

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
    finally: pass

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
    finally: pass


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
    
    /* Padding for EDA Header */
    .tab-header { margin-top: 30px; margin-bottom: 20px; padding-left: 15px; border-left: 5px solid #0d6efd; background-color: #f8f9fa; padding-top: 10px; padding-bottom: 10px; }
    .vbox-ui .card-body { padding: 0; }    
    
    .navbar { background-color: #1a1d29 !important; border-bottom: 1px solid #2d3342; }
    .navbar-brand { font-weight: 700; letter-spacing: 1px; display: flex; align-items: center; gap: 10px; color: #000 !important; }
           
    body { background-color: #f4f6f9; }
    .card-header { font-weight: bold; background-color: white; border-bottom: 1px solid #eee; }
    .table th, .table td { text-align: center; vertical-align: middle; }
    .table th { background-color: #f8f9fa; }
    
    .metric-value { font-size: 2rem; font-weight: bold; }
    .metric-label { color: #6c757d; font-size: 0.9rem; text-transform: uppercase; }
    
    .app-footer {
        position: fixed; bottom: 0; left: 0; width: 100%;
        background-color: #1a1d29; color: #8a8d99;
        text-align: center; padding: 10px; font-size: 0.8rem;
        border-top: 1px solid #2d3342; z-index: 9999;
    }
"""

chart_icon_svg = """
<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#00bc8c" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
  <path d="M3 3v18h18"/>
  <path d="M18.7 8l-5.1 5.2-2.8-2.7L7 14.3"/>
</svg>
"""

# --- 4. UI ---
app_ui = ui.page_navbar(
    ui.head_content(ui.tags.style(custom_css)),

    # BRANDING: HTML Span with SVG

    # --- TAB 1: DASHBOARD ---
    ui.nav_panel("Dashboard",
                 ui.h3("Dashboard", class_="tab-header"),
                 ui.layout_sidebar(
                     ui.sidebar(
                         ui.h4("Filters"),
                         ui.input_select("crypto_select", "Asset:",
                                         {"BTC": "Bitcoin", "ETH": "Ethereum", "SOL": "Solana"}, selected="BTC"),
                         ui.input_select("time_range", "Time Range:",
                                         {"1H": "Last 1 Hour", "24H": "Last 24 Hours", "7D": "Last 7 Days",
                                          "30D": "Last 30 Days",
                                          "ALL": "All Available"}, selected="1H"),
                         ui.hr(),
                         ui.output_text("status_text")
                     ),
                     ui.row(
                         ui.column(4, ui.card(ui.card_header("Current Price"), ui.output_ui("vbox_price"),
                                              style="text-align: center; min-height: 120px;")),
                         ui.column(4, ui.card(ui.output_ui("vbox_change_header"), ui.output_ui("vbox_change"),
                                              style="text-align: center; min-height: 120px;"), class_="vbox-ui"),
                         ui.column(4, ui.card(ui.card_header("Volatility"), ui.output_ui("vbox_vol"),
                                              style="text-align: center; min-height: 120px;")),
                     ),
                     ui.br(),
                     ui.card(
                         ui.card_header("Overview: Price Trend"),
                         ui.output_ui("price_chart_view"),
                         full_screen=True
                     ),
                     ui.br(),
                     ui.card(
                         ui.card_header("Deep Dive: Price Action & Indicators"),
                         ui.output_ui("candle_chart_view"),
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
                     ),
                     ui.row(
                         ui.card(
                             ui.card_header("S&P 500 Trends (Golden Cross)"),
                                 ui.output_ui("stock_chart_view")
                         ),
                         ui.card(
                             ui.card_header("Forex Trading Volume"),
                                 ui.output_ui("forex_chart_view")
                         )
                     )
                 )
                 ),

    # --- TAB 3: MODEL EVALUATION ---
    ui.nav_panel("Model Eval",
                 ui.h3("Model Scorecard", class_="tab-header"),
                 ui.row(
                     ui.column(3, ui.card(ui.div("RMSE", class_="metric-label"), ui.output_ui("eval_rmse"),
                                          style="text-align: center;")),
                     ui.column(3, ui.card(ui.div("MAPE", class_="metric-label"), ui.output_ui("eval_mape"),
                                          style="text-align: center;")),
                     ui.column(3, ui.card(ui.div("Directional Acc", class_="metric-label"), ui.output_ui("eval_dir"),
                                          style="text-align: center;")),
                     ui.column(3, ui.card(ui.div("R-Squared", class_="metric-label"), ui.output_ui("eval_r2"),
                                          style="text-align: center;")),
                 ),
                 ui.br(),
                 ui.row(
                     ui.column(6, ui.card(ui.card_header("Actual vs. Predicted"), ui.output_ui("eval_pred_chart"))),
                     ui.column(6, ui.card(ui.card_header("Residuals over Time"), ui.output_ui("eval_resid_chart")))
                 )
                 ),

    # --- TAB 4: MONITORING ---
    ui.nav_panel("Monitoring",
                 ui.h3("System Health Monitor", class_="tab-header"),
                 ui.row(
                     ui.column(4, ui.card(ui.card_header("Pipeline Status"), ui.output_ui("mon_status"),
                                          style="text-align:center; min-height:150px;")),
                     ui.column(4, ui.card(ui.card_header("Data Freshness"), ui.output_ui("mon_freshness"),
                                          style="text-align:center; min-height:150px;")),
                     ui.column(4, ui.card(ui.card_header("Total Records"), ui.output_ui("mon_count"),
                                          style="text-align:center; min-height:150px;")),
                 ),
                 ui.br(),
                 ui.card(
                     ui.card_header("Ingestion Latency Log (Last 20 Batches)"),
                     ui.output_table("mon_table")
                 )
                 ),

    # --- TAB 5: RAW DATA ---
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
                 ),
    title=ui.span(
        ui.HTML(chart_icon_svg),
        " CRYPTO ANALYTICS"
    ),
    id="nav",
    window_title="Crypto Analytics | Live Dashboard",
    footer=ui.div(
        ui.span("© 2025 Fantastic Four "),
        ui.span("System Status: Operational", style="color: #00cc96; font-weight: bold;"),
        class_="app-footer"
    )
)


# --- 5. SERVER ---
def server(input, output, session):
    all_data = reactive.Value(None)
    stock_data = reactive.Value(None)
    forex_data = reactive.Value(None)

    @reactive.Effect
    def _():
        all_data.set(load_data_from_hive("CryptocurrencySnapshot"))
        stock_data.set(load_stock_data())
        forex_data.set(load_forex_data())

    # Base Filter Logic
    @reactive.Calc
    def dashboard_data_raw():
        df = all_data.get()
        if df is not None and not df.empty:
            df_sub = df[df['Symbol'] == input.crypto_select()].copy()

            # Date Filter
            if input.time_range() == "24H":
                cutoff = df_sub['Datetime'].max() - pd.Timedelta(hours=24)
                df_sub = df_sub[df_sub['Datetime'] >= cutoff]
            elif input.time_range() == "7D":
                cutoff = df_sub['Datetime'].max() - pd.Timedelta(days=7)
                df_sub = df_sub[df_sub['Datetime'] >= cutoff]
            elif input.time_range() == "30D":
                cutoff = df_sub['Datetime'].max() - pd.Timedelta(days=30)
                df_sub = df_sub[df_sub['Datetime'] >= cutoff]
            elif input.time_range() == "1H":
                cutoff = df_sub['Datetime'].max() - pd.Timedelta(hours=1)
                df_sub = df_sub[df_sub['Datetime'] >= cutoff]

            return df_sub.sort_values("Datetime")
        return pd.DataFrame()

    # Resampled Logic for Candlesticks
    @reactive.Calc
    def dashboard_data_resampled():
        df = dashboard_data_raw()
        if df.empty: return df

        # Dynamic Resampling Frequency
        if input.time_range() == "1H":
            freq = "1min"
        elif input.time_range() == "24H":
            freq = "15min"
        elif input.time_range() == "30D":
            freq = "4H"
        elif input.time_range() == "7D":
            freq = "1H"
        else:
            freq = "4H"

        # Resample to create legitimate OHLC bars from the price stream
        df_res = df.set_index("Datetime").resample(freq).agg({
            "OpeningPrice": "first",
            "HighestDayPrice": "max",
            "LowestDayPrice": "min",
            "CurrentPrice": "last"
        }).dropna().reset_index()

        df_res = df_res.reset_index()

        # Calculate SMA on resampled data
        df_res['SMA7'] = df_res['CurrentPrice'].rolling(window=7, min_periods=1).mean()
        df_res['SMA30'] = df_res['CurrentPrice'].rolling(window=30, min_periods=1).mean()

        return df_res

    # --- MOCK DATA FOR EVALUATION ---
    @reactive.Calc
    def mock_eval_data():
        df = dashboard_data_raw()
        if df.empty: return pd.DataFrame()

        # Create fake predictions (Actual + Noise)
        eval_df = df[['Datetime', 'CurrentPrice']].copy()
        eval_df.rename(columns={'CurrentPrice': 'Actual'}, inplace=True)

        # Add noise +/- 2%
        noise = np.random.normal(0, eval_df['Actual'] * 0.02, len(eval_df))
        eval_df['Predicted'] = eval_df['Actual'] + noise
        eval_df['Residual'] = eval_df['Actual'] - eval_df['Predicted']

        return eval_df

    # --- PLOT 1: SIMPLE LINE ---
    @render.ui
    def price_chart_view():
        df = dashboard_data_raw()
        if df.empty: return ui.div("No Data Available", style="color:gray; text-align:center; padding:50px;")

        # Calculate point-to-point change
        # Shift price by 1 to compare current vs previous
        df['PrevPrice'] = df['CurrentPrice'].shift(1)
        # 1 = Up (Green), 0 = Down (Red) - Default to Green for first point
        df['Color'] = np.where(df['CurrentPrice'] >= df['PrevPrice'].fillna(0), '#00cc96', '#ef553b')

        fig = go.Figure()

        # 1. Main Line (Grey/Neutral)
        fig.add_trace(go.Scatter(
            x=df['Datetime'], y=df['CurrentPrice'],
            mode='lines',
            line=dict(color='#cccccc', width=1),
            hoverinfo='skip'  # Markers handle hover
        ))

        # 2. Colored Markers (Green/Red)
        fig.add_trace(go.Scatter(
            x=df['Datetime'], y=df['CurrentPrice'],
            mode='markers',
            marker=dict(
                size=4,
                color=df['Color'],  # Array of colors
                opacity=0.8
            ),
            name="Price"
        ))

        fig.update_layout(
            title=None, xaxis_title=None, yaxis_title="Price ($)",
            margin=dict(l=60, r=20, t=20, b=20),
            hovermode="x unified",
            height=400,
            showlegend=False,
            template="plotly_white"
        )
        fig.update_xaxes(tickformat="%b %d %H:%M")
        fig.update_yaxes(tickprefix="$")

        return render_plotly_html(fig, height="400px")

    # --- PLOT 2: CANDLESTICK ---
    @render.ui
    def candle_chart_view():
        df = dashboard_data_resampled()
        if df.empty: return ui.div("No Data Available", style="color:gray; text-align:center; padding:50px;")

        # Base: Candlestick
        fig = go.Figure(data=[go.Candlestick(
            x=df['Datetime'],
            open=df['OpeningPrice'],
            high=df['HighestDayPrice'],
            low=df['LowestDayPrice'],
            close=df['CurrentPrice'],
            name=input.crypto_select(),
            text=[
                f"Opening Price: {o:,.2f}<br>Highest Day Price: {h:,.2f}<br>Lowest Day Price: {l:,.2f}<br>Current Price: {c:,.2f}"
                for o, h, l, c in
                zip(df['OpeningPrice'], df['HighestDayPrice'], df['LowestDayPrice'], df['CurrentPrice'])],
            hoverinfo="x+text"
        )])

        fig.add_trace(go.Scatter(x=df['Datetime'], y=df['SMA7'], mode='lines', name=f'SMA 7',
                                 line=dict(color='purple', width=1.5)))
        fig.add_trace(go.Scatter(x=df['Datetime'], y=df['SMA30'], mode='lines', name=f'SMA 30',
                                 line=dict(color='blue', width=1.5)))

        # Layout
        fig.update_layout(
            title=dict(text=f"{input.crypto_select()} Price Action", x=0.01),
            yaxis_title="Price (USD)",
            margin=dict(l=60, r=20, t=40, b=40),  # Increased bottom margin for rangeslider
            hovermode="x unified",
            height=500,
            xaxis_rangeslider_visible=False,  # Hide the mini-slider to save space
            xaxis=dict(type='date'),
            template="plotly_white"
        )
        fig.update_xaxes(tickformat="%b %d %H:%M")
        fig.update_yaxes(tickprefix="$")

        return render_plotly_html(fig, height="500px")

    # --- EDA PLOTS ---
    @render.ui
    def corr_chart_view():
        return render_plotly_html(plot_correlation_matrix(all_data.get()))

    @render.ui
    def dist_chart_view():
        return render_plotly_html(plot_return_distribution(all_data.get(), input.crypto_select()))

    @render.ui
    def stock_chart_view():
        return render_plotly_html(plot_stock_indicators(stock_data.get()))

    @render.ui
    def forex_chart_view():
        return render_plotly_html(plot_forex_volume(forex_data.get()))

    # --- METRICS ---
    @render.ui
    def vbox_price():
        df = dashboard_data_raw()
        val = f"${df['CurrentPrice'].iloc[-1]:,.2f}" if not df.empty else "$ -"
        return ui.div(val, class_="metric-value")

    @render.ui
    def vbox_change_header():
        # Dynamic Header based on selection
        label_map = {"1H": "Change (1h)", "24H": "Change (24h)", "7D": "Change (7D)", "30D": "Change (30D)",
                     "ALL": "Change (Total)"}
        return ui.div(label_map.get(input.time_range(), "Change"), class_="card-header")

    @render.ui
    def vbox_change():
        df = dashboard_data_raw()
        if df.empty: return ui.div("-", class_="metric-value")

        start = df['CurrentPrice'].iloc[0]
        end = df['CurrentPrice'].iloc[-1]
        chg = ((end - start) / start) * 100

        color = "#00cc96" if chg >= 0 else "#ef553b"
        symbol = "▲" if chg >= 0 else "▼"

        return ui.div(
            f"{symbol} {abs(chg):.2f}%",
            class_="metric-value",
            style=f"color: {color};"
        )

    @render.ui
    def vbox_vol():
        df = dashboard_data_raw()
        val = f"${df['CurrentPrice'].std():.2f}" if not df.empty else "-"
        return ui.div(val, class_="metric-value")

    @render.text
    def status_text():
        df = all_data.get()
        return f"🟢 Loaded {len(df)} rows" if df is not None and not df.empty else "Connecting..."

    @render.ui
    def eval_rmse():
        df = mock_eval_data()
        if df.empty: return "-"
        rmse = np.sqrt(((df['Actual'] - df['Predicted']) ** 2).mean())
        return ui.div(f"${rmse:,.2f}", class_="metric-value", style="color:#ef553b")

    @render.ui
    def eval_mape():
        df = mock_eval_data()
        if df.empty: return "-"
        mape = (abs((df['Actual'] - df['Predicted']) / df['Actual']).mean()) * 100
        return ui.div(f"{mape:.2f}%", class_="metric-value", style="color:#ffa500")

    @render.ui
    def eval_dir():
        # Directional Accuracy (Did we predict the sign of change correctly?)
        df = mock_eval_data()
        if df.empty: return "-"
        # Simple mockup: 65% accuracy
        return ui.div("65.2%", class_="metric-value", style="color:#00cc96")

    @render.ui
    def eval_r2():
        return ui.div("0.89", class_="metric-value")

    @render.ui
    def eval_pred_chart():
        df = mock_eval_data()
        if df.empty: return ui.div("No Data")

        # Plot Actual vs Predicted (Time Series)
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(x=df['Datetime'], y=df['Actual'], mode='lines', name='Actual', line=dict(color='#00cc96')))
        fig.add_trace(go.Scatter(x=df['Datetime'], y=df['Predicted'], mode='lines', name='Predicted',
                                 line=dict(color='#ef553b', dash='dot')))

        fig.update_layout(title="Actual vs Predicted Prices", margin=dict(l=40, r=20, t=40, b=20),
                          hovermode="x unified", template="plotly_white", height=400)
        return render_plotly_html(fig, height="400px")

    @render.ui
    def eval_resid_chart():
        df = mock_eval_data()
        if df.empty: return ui.div("No Data")

        fig = px.scatter(df, x="Datetime", y="Residual", title="Residual Errors over Time", template="plotly_white")
        fig.add_hline(y=0, line_dash="dash", line_color="gray")
        fig.update_layout(margin=dict(l=40, r=20, t=40, b=20), height=400)
        return render_plotly_html(fig, height="400px")

    @render.ui
    def mon_status():
        df = all_data.get()
        if df is None or df.empty: return ui.div("OFFLINE", class_="metric-value status-crit")

        last_time = df['Datetime'].max()
        time_diff = pd.Timestamp.now() - last_time

        # Logic: If data is older than 2 hours (for demo purposes) -> Warn
        # In prod, this might be 5 minutes
        if time_diff > pd.Timedelta(hours=24):
            return ui.div("STALE", class_="metric-value status-crit")
        elif time_diff > pd.Timedelta(hours=2):
            return ui.div("LAGGING", class_="metric-value status-warn")
        else:
            return ui.div("ONLINE", class_="metric-value status-ok")

    @render.ui
    def mon_freshness():
        df = all_data.get()
        if df is None or df.empty: return ui.div("-", class_="metric-value")
        last_time = df['Datetime'].max()
        # Format: "2 mins ago" or "5 hours ago"
        diff = pd.Timestamp.now() - last_time
        if diff.days > 0:
            val = f"{diff.days} days ago"
        elif diff.seconds > 3600:
            val = f"{diff.seconds // 3600} hours ago"
        else:
            val = f"{diff.seconds // 60} mins ago"
        return ui.div(val, class_="metric-value")

    @render.ui
    def mon_count():
        df = all_data.get()
        count = len(df) if df is not None else 0
        return ui.div(f"{count:,.0f}", class_="metric-value")

    @render.table
    def mon_table():
        df = all_data.get()
        if df is None or df.empty: return pd.DataFrame()
        # Simulate an "Ingestion Log" by showing the latest timestamps per asset
        latest_per_asset = df.groupby('Symbol')['Datetime'].max().reset_index()
        latest_per_asset['Status'] = 'OK'
        latest_per_asset['Latency (ms)'] = np.random.randint(50, 500, size=len(latest_per_asset))
        return latest_per_asset

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
