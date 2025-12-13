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

        # CHANGED: Added Open/High/Low columns for Candlestick charts
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

    except Exception as e:
        print(f"ERROR: {e}")
        return pd.DataFrame()
    finally:
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
    
    /* Padding for EDA Header */
    .tab-header { margin-top: 30px; margin-bottom: 20px; padding-left: 15px; border-left: 5px solid #0d6efd; background-color: #f8f9fa; padding-top: 10px; padding-bottom: 10px; }
    .vbox-ui .card-body { padding: 0; }    
    
    .navbar { background-color: #1a1d29 !important; border-bottom: 1px solid #2d3342; }
    .navbar-brand { font-weight: 700; letter-spacing: 1px; display: flex; align-items: center; gap: 10px; color: #000 !important; }
           
    body { background-color: #f4f6f9; }
    .card-header { font-weight: bold; background-color: white; border-bottom: 1px solid #eee; }
    .table th, .table td { text-align: center; vertical-align: middle; }
    .table th { background-color: #f8f9fa; }
    
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
                                         {"1H": "Last 1 Hour", "24H": "Last 24 Hours", "7D": "Last 7 Days", "30D": "Last 30 Days",
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

    @reactive.Effect
    def _():
        df = load_data_from_hive("CryptocurrencySnapshot")
        all_data.set(df)

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
            text=[f"Opening Price: {o:,.2f}<br>Highest Day Price: {h:,.2f}<br>Lowest Day Price: {l:,.2f}<br>Current Price: {c:,.2f}"
                  for o, h, l, c in zip(df['OpeningPrice'], df['HighestDayPrice'], df['LowestDayPrice'], df['CurrentPrice'])],
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
    @render.ui
    def vbox_price():
        df = dashboard_data_raw()
        val = f"${df['CurrentPrice'].iloc[-1]:,.2f}" if not df.empty else "$ -"
        return ui.div(val, class_="metric-value")

    @render.ui
    def vbox_change_header():
        # Dynamic Header based on selection
        label_map = {"1H": "Change (1h)", "24H": "Change (24h)", "7D": "Change (7D)", "30D": "Change (30D)", "ALL": "Change (Total)"}
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
