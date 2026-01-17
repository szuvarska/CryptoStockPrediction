import pandas as pd
import numpy as np
from shiny import App, ui, reactive, render
from faicons import icon_svg
from pathlib import Path
import os
import asyncio
import traceback
from datetime import datetime

# --- Local Imports (Refactored Modules) ---
from data_loader import load_forex_data, load_spark_model, load_historical_data, get_latest_ticks
from plots.eda_plots import (
    plot_correlation_matrix,
    plot_return_distribution,
    plot_stock_indicators,
    plot_forex_volume
)
from plots.dashboard_plots import plot_price_trend, plot_candlestick
from plots.eval_plots import plot_actual_vs_predicted, plot_residuals

# --- HELPER: PLOT TO HTML ---
def render_plotly_html(fig, height="100%"):
    """Converts a Plotly figure to an HTML string for robust rendering."""
    if fig is None: return ui.HTML("")
    html = fig.to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True, 'displayModeBar': False})
    return ui.HTML(f'<div style="height: {height}; width: 100%;">{html}</div>')

# --- 4. UI ---
app_ui = ui.page_sidebar(
# --- GLOBAL SIDEBAR ---
    ui.sidebar(
        # Only show filters if NOT on Monitoring or Raw Data tabs
        ui.panel_conditional(
            "input.tabs != 'Monitoring' && input.tabs != 'Testy Michała'",
            ui.h4("Filters", class_="sidebar-title"),
            ui.input_select(
                "crypto_select", "Asset:",
                {"BTC": "Bitcoin", "ETH": "Ethereum", "SOL": "Solana", "SNP": "S&P 500", "NIM": "NASDAQ", "DJI": "Dow Jones"},
                selected="BTC"
            ),
            ui.input_select(
                "time_range", "Time Range:",
                {"1H": "Last 1 Hour", "24H": "Last 24 Hours", "7D": "Last 7 Days", "30D": "Last 30 Days", "ALL": "All Available"},
                selected="24H"
            ),
        ),
        ui.hr(),
        ui.h6("System Health"),
        ui.output_ui("sidebar_status"),
        width=275,
        open="desktop"
    ),

    ui.head_content(
        ui.tags.link(rel="stylesheet", href="styles.css"),
        ui.tags.link(rel="icon", href="https://cdn-icons-png.flaticon.com/512/1822/1822219.png")
    ),

ui.navset_card_tab(
    # --- TAB 1: DASHBOARD ---
    ui.nav_panel("Dashboard",
                 ui.h3("Live Market Overview", class_="tab-header"),
                 # Value Boxes (Metrics)
                 ui.layout_columns(
                     ui.value_box(
                         "Current Price", ui.output_ui("vbox_price"),
                         showcase=icon_svg("bitcoin"), theme="bg-gradient-blue-purple"
                     ),
                     ui.value_box(
                         ui.output_ui("vbox_change_label"), ui.output_ui("vbox_change"),
                         showcase=icon_svg("chart-line"), theme="white"
                     ),
                     ui.value_box(
                         "Volatility (StdDev)", ui.output_ui("vbox_vol"),
                         showcase=icon_svg("wave-square"), theme="bg-gradient-blue-purple"
                     ),
                     fill=False
                 ),
                 ui.br(),
                 ui.card(
                     ui.card_header("Price Trend Analysis"),
                     ui.output_ui("price_chart_view"),
                     full_screen=True
                 ),
                 ui.br(),
                 ui.layout_columns(
                     ui.card(ui.card_header("Candlestick Analysis"), ui.output_ui("candle_chart_view"),
                             full_screen=True),
                     col_widths=[12]
                 )
             ),

    # --- TAB 2: EDA (Integrated) ---
    ui.nav_panel("EDA",
                 ui.h3("Exploratory Data Analysis", class_="tab-header"),
                     ui.row(
                         ui.column(6, ui.card(ui.output_ui("corr_chart_view"))),
                         ui.column(6, ui.card(ui.output_ui("dist_chart_view")))
                     ),
                     ui.row(
                         ui.column(6, ui.card(ui.output_ui("stock_chart_view"))),
                         ui.column(6, ui.card(ui.output_ui("forex_chart_view")))
                     )
                 # )
                 ),

    # --- TAB 3: MODEL EVALUATION ---
    ui.nav_panel("Model Eval",
                 ui.h3("Model Scorecard", class_="tab-header"),
                 ui.layout_columns(
                     ui.value_box("RMSE", ui.output_ui("eval_rmse"), theme="danger"),
                     ui.value_box("MAPE", ui.output_ui("eval_mape"), theme="warning"),
                     ui.value_box("Directional Acc", ui.output_ui("eval_dir"), theme="success"),
                     ui.value_box("R-Squared", ui.output_ui("eval_r2"), theme="primary"),
                     fill=False
                 ),
                 ui.br(),
                 ui.row(
                     ui.column(6, ui.card(ui.card_header("Actual vs Predicted"), ui.output_ui("eval_pred_chart"))),
                     ui.column(6, ui.card(ui.card_header("Residuals"), ui.output_ui("eval_resid_chart")))
                 )
                 ),

    # --- TAB 4: MONITORING ---
    ui.nav_panel("Monitoring",
                 ui.h3("Pipeline Health Monitor", class_="tab-header"),
                 ui.layout_columns(
                     ui.value_box("Pipeline Status", ui.output_ui("mon_status_main")),
                     ui.value_box("Data Freshness", ui.output_ui("mon_freshness")),
                     # ui.value_box("Total Records", ui.output_ui("mon_count")),
                     ui.value_box("Data Refresh", ui.output_ui("mon_latency")),
                     fill=False
                 ),
                 ui.br(),
                 ui.card(
                     ui.card_header("Data Ingestion Latency Log"),
                     ui.output_table("mon_table")
                 )
                 ),

    # --- TAB 5: RAW DATA ---
    ui.nav_panel("Raw Data",
                 ui.h3("Raw Data", class_="tab-header"),
                     ui.layout_columns(
                         ui.download_button("download_csv", "Download CSV", class_="btn-primary"),
                           ui.input_select(
                                            "source_select", label = "",
                                            choices = {"crypto": "Cryptocurrency Prices", "forex": "Forex Prices"},
                                            selected="Crypto"
                                        ),
                         col_widths=[3]
                     ),
                     ui.br(),
                     ui.card(ui.output_table("raw_table"))
                 # ),
                 ),
    ui.nav_panel("Testy Michała",
    ui.pre(str(load_spark_model('hdfs://namenode:8020/models/btc_model')))
    ),
    id="tabs",
    ),
    # footer=ui.output_ui("dynamic_footer"),
    ui.output_ui("dynamic_footer"),
    title = ui.span(
        ui.img(src="https://cdn-icons-png.flaticon.com/512/1822/1822219.png", class_="app-logo"),
        "CRYPTO ANALYTICS",
        class_="app-brand"
    )
)


# --- 5. SERVER ---
def server(input, output, session):

    # 1. CENTRALIZED DATA LOADING
    data_store = reactive.Value({
        "crypto": pd.DataFrame(),
        "forex": pd.DataFrame()
    })

    # SAFETY FLAG: Prevents real-time updates from running before history is loaded
    is_initialized = reactive.Value(False)

    @reactive.Effect
    async def _init_load():
        print("Starting heavy historical data load (background)...")

        loop = asyncio.get_event_loop()

        # Run in separate thread using executor (Compatible with Python < 3.9)
        history_df = await loop.run_in_executor(None, load_historical_data)
        forex_df = await loop.run_in_executor(None, load_forex_data)

        # Update store and set flag
        data_store.set({
            "crypto": history_df,
            "forex": forex_df
        })
        is_initialized.set(True)
        print("Historical data load complete. Real-time updates enabled.")

    # 2. REAL-TIME TICKER
    @reactive.Effect
    def _update_prices():

        # Check initialization
        if not is_initialized.get():
            return

        reactive.invalidate_later(120000)

        # WRAP IN TRY-EXCEPT TO PREVENT CRASHING
        with reactive.isolate():
            try:
                now = datetime.now().strftime("%H:%M:%S")
                print(f"[{now}] DEBUG: _update_prices triggered")

                current_data = data_store.get()
                main_df = current_data.get("crypto")

                if main_df is None:
                    return

                # Fetch latest ticks
                new_ticks = get_latest_ticks()

                if not new_ticks.empty:

                    # Ensure types align before concat if necessary, but usually pandas handles it
                    # Debugging column mismatch if any

                    updated_df = pd.concat([main_df, new_ticks], ignore_index=True, sort=False)

                    updated_df = updated_df.drop_duplicates(subset=['Symbol', 'Datetime'], keep='last')

                    updated_df['FiftyDayAveragePrice'] = updated_df.groupby('Symbol')['FiftyDayAveragePrice'].ffill()
                    updated_df['TwoHundredDaysAveragePrice'] = updated_df.groupby('Symbol')[
                        'TwoHundredDaysAveragePrice'].ffill()

                    data_store.set({
                        "crypto": updated_df,
                        "forex": current_data.get("forex")
                    })

            except Exception as e:
                print(f"Error Message: {e}")
                traceback.print_exc()

    # --- HELPER: GENERIC TIME FILTER ---
    def filter_by_time(df, time_range, date_col='Datetime'):
        """Filters any dataframe by the selected time range."""
        if df is None or df.empty: return df

        cutoff_map = {
            "1H": pd.Timedelta(hours=1),
            "24H": pd.Timedelta(hours=24),
            "7D": pd.Timedelta(days=7),
            "30D": pd.Timedelta(days=30)
        }

        if time_range in cutoff_map:
            cutoff_time = df[date_col].max() - cutoff_map[time_range]
            return df[df[date_col] >= cutoff_time].copy()

        return df

    # 2. DATA FILTERING LOGIC
    @reactive.Calc
    def filtered_crypto_specific():

        data = data_store.get()
        df = data.get("crypto")
        if df is None or df.empty: return pd.DataFrame()

        # 1. Filter Asset
        df_sub = df[df['Symbol'] == input.crypto_select()].copy()

        # 2. Filter Time
        return filter_by_time(df_sub, input.time_range())

    @reactive.Calc
    def filtered_crypto_all():
        data = data_store.get()
        return filter_by_time(data.get("crypto"), input.time_range())

    @reactive.Calc
    def filtered_forex():
        data = data_store.get()
        return data.get("forex")

    #Resampled Logic for Candlesticks
    @reactive.Calc
    def resampled_crypto():
        df = filtered_crypto_specific()
        if df.empty: return df

        # Dynamic Frequency
        freq_map = {"1H": "1min", "24H": "15min", "7D": "1H", "30D": "4H", "ALL": "1D"}
        freq = freq_map.get(input.time_range(), "4H")

        df_res = df.set_index("Datetime").resample(freq).agg({
            "OpeningPrice": "first", "HighestDayPrice": "max",
            "LowestDayPrice": "min", "CurrentPrice": "last"
        }).dropna().reset_index()

        # Add SMAs
        df_res['SMA7'] = df_res['CurrentPrice'].rolling(7).mean()
        df_res['SMA30'] = df_res['CurrentPrice'].rolling(30).mean()

        return df_res

    @reactive.Calc
    def raw_data():
        source = input.source_select()

        if source == 'crypto':
            df = filtered_crypto_specific()
        # elif source == 'stock':
        #     df = filtered_stock()
        else:
            df = filtered_forex()
            df['VolumeTraded'] = df['VolumeTraded'].astype('int32')
            df = df.reindex(sorted(df.columns), axis=1)

        if df is None or df.empty:
            return pd.DataFrame({"Message": [f"No data available for {source}"]})

        for col in ['PrevPrice', 'Color', 'Datetime_Str']:
            if col in df.columns:
                df.drop([col], inplace = True, axis = 1)

        df["Datetime"] = pd.to_datetime(df["Datetime"])
        df["Datetime"] = df["Datetime"].dt.strftime('%Y-%m-%d %H:%M')
        return df.sort_values("Datetime", ascending=False)


    # --- DASHBOARD OUTPUTS ---
    @render.ui
    def vbox_price():
        df = filtered_crypto_specific()
        if df.empty: return "Loading..."
        return f"${df['CurrentPrice'].iloc[-1]:,.2f}"

    @render.ui
    def vbox_change_label():
        range_labels = {
            "1H": "Change (1h)",
            "24H": "Change (24h)",
            "7D": "Change (7d)",
            "30D": "Change (30d)",
            "ALL": "Change (All Time)"
        }
        # Default to "Change" if something unexpected happens
        return range_labels.get(input.time_range(), "Change")

    @render.ui
    def vbox_change():
        df = filtered_crypto_specific()
        if df.empty: return "-"
        start, end = df['CurrentPrice'].iloc[0], df['CurrentPrice'].iloc[-1]
        chg = ((end - start) / start) * 100

        # Add arrow and color styling
        color = "green" if chg >= 0 else "red"
        icon = "▲" if chg >= 0 else "▼"
        return ui.span(f"{icon} {abs(chg):.2f}%", style=f"color:{color}; font-weight:bold;")

    @render.ui
    def vbox_vol():
        df = filtered_crypto_specific()
        if df.empty: return "-"
        return f"${df['CurrentPrice'].std():.2f}"

    @render.ui
    def price_chart_view():
        return render_plotly_html(plot_price_trend(filtered_crypto_specific()), height="400px")

    @render.ui
    def candle_chart_view():
        return render_plotly_html(plot_candlestick(
            # filtered_crypto_specific(),
            resampled_crypto(),
            input.crypto_select(),
            input.time_range(),
            show_sma=False
        ), height="500px")

    # --- EDA OUTPUTS ---
    @render.ui
    def corr_chart_view():
        return render_plotly_html(plot_correlation_matrix(filtered_crypto_all(), input.time_range()))

    @render.ui
    def dist_chart_view():
        df = filtered_crypto_specific()
        return render_plotly_html(plot_return_distribution(df, input.crypto_select(), input.time_range()))

    @render.ui
    def stock_chart_view():
        return render_plotly_html(plot_stock_indicators(filtered_crypto_specific()))

    @render.ui
    def forex_chart_view():
        return render_plotly_html(plot_forex_volume(filtered_forex()))

    # --- MODEL MOCKUPS (Simplified for brevity) ---
    @render.ui
    def eval_rmse():
        # df = mock_eval_data()
        # if df.empty: return "-"
        # rmse = np.sqrt(((df['Actual'] - df['Predicted']) ** 2).mean())
        # return ui.div(f"${rmse:,.2f}", class_="metric-value", style="color:#ef553b")
        return "$142.50"

    @render.ui
    def eval_mape():
        # df = mock_eval_data()
        # if df.empty: return "-"
        # mape = (abs((df['Actual'] - df['Predicted']) / df['Actual']).mean()) * 100
        # return ui.div(f"{mape:.2f}%", class_="metric-value", style="color:#ffa500")
        return "2.14%"

    @render.ui
    def eval_dir():
        # # Directional Accuracy (Did we predict the sign of change correctly?)
        # df = mock_eval_data()
        # if df.empty: return "-"
        # # Simple mockup: 65% accuracy
        # return ui.div("65.2%", class_="metric-value", style="color:#00cc96")
        return "65.2%"

    @render.ui
    def eval_r2():
        return "0.89"

    @reactive.Calc
    def mock_eval_data():
        df = filtered_crypto_specific()
        if df.empty: return pd.DataFrame()

        # Simulate predictions
        eval_df = df.copy()
        eval_df['Actual'] = eval_df['CurrentPrice']
        # Add random noise for "Predicted"
        noise = np.random.normal(0, eval_df['Actual'].std() * 0.1, len(eval_df))
        eval_df['Predicted'] = eval_df['Actual'] + noise
        eval_df['Residual'] = eval_df['Actual'] - eval_df['Predicted']

        return eval_df

    @render.ui
    def eval_pred_chart():
        return render_plotly_html(plot_actual_vs_predicted(mock_eval_data()), height="300px")

    @render.ui
    def eval_resid_chart():
        return render_plotly_html(plot_residuals(mock_eval_data()), height="300px")

    # --- MONITORING & FOOTER ---
    @reactive.Calc
    def system_health():
        """Calculates overall system health based on all 3 data sources."""
        data = data_store.get()
        if not data: return "Initializing", "gray"

        status_msgs = []
        is_critical = False
        is_warning = False

        data = data_store.get()
        df = data.get("crypto")

        if df is None or df.empty:
            status_msgs.append(f"Crypto Data Empty")
            is_warning = True

        last_time = df['Datetime'].max() if 'Datetime' in df else pd.Timestamp.min
        age = pd.Timestamp.now() - last_time

        if age > pd.Timedelta(hours=24):
            is_critical = True
            status_msgs.append(f"Crypto Data: Stale (>24h)")
        elif age > pd.Timedelta(hours=4):
            is_warning = True

        if is_critical: return "CRITICAL - Data Stale", "#ef553b"
        if is_warning: return "DEGRADED - Check Feeds", "#ffa500"
        return "OPERATIONAL", "#00cc96"

    @render.ui
    def dynamic_footer():
        msg, color = system_health()
        return ui.div(
            ui.span("© 2025 Fantastic Four | "),
            ui.span(msg, style=f"color: {color}; font-weight: bold; margin-left: 5px;"),
            class_="app-footer"
        )

    @render.ui
    def sidebar_status():
        msg, color = system_health()
        return ui.div(msg, style=f"color: {color}; font-weight: bold; font-size: 0.9rem;")

    @render.ui
    def mon_status_main():
        msg, color = system_health()
        return ui.div(msg, style=f"color: {color}; font-weight:bold; font-size:1.2rem;")

    @render.ui
    def mon_freshness():
        df = data_store.get().get("crypto")
        if df is None or df.empty: return "-"
        last_time = df['Datetime'].max()
        # Format: "2 mins ago" or "5 hours ago"
        diff = pd.Timestamp.now() - last_time
        if diff.days > 0:
            val = f"{diff.days} days ago"
        elif diff.seconds > 3600:
            val = f"{diff.seconds // 3600} hours ago"
        else:
            val = f"{diff.seconds // 60} mins ago"
        return val

    @render.ui
    def mon_count():
        df = data_store.get().get("crypto")
        return f"{len(df):,}" if df is not None else "0"

    @render.ui
    def mon_latency():
        return "60 seconds"


    # @render.table
    # def mon_table():
    #     data = data_store.get()
    #     rows = []
    #     frequencies = {"crypto": "15 seconds", "stock": "60 seconds", "forex": "60 seconds"}
    #
    #     for name, df in data.items():
    #         if df is not None and not df.empty:
    #             rows.append({
    #                 "Source": name.title(),
    #                 "Latest Timestamp": df['Datetime'].max().strftime('%Y-%m-%d %H:%M'),
    #                 "Rows": len(df),
    #                 "Status": "OK",
    #                 "Frequency of Data Inflow": frequencies[name]
    #             })
    #         else:
    #             rows.append({"Source": name.title(), "Latest Timestamp": "-", "Rows": 0, "Status": "MISSING"})
    #     return pd.DataFrame(rows)

    @render.table
    def mon_table():
        df = data_store.get().get('crypto')
        mon_df = df.groupby('Symbol').agg(
            Latest_Timestamp=('Datetime', 'max'),
            Rows=('Datetime', 'count')
        ).reset_index()

        mon_df['Latest Timestamp'] = mon_df['Latest_Timestamp'].dt.strftime('%Y-%m-%d %H:%M')

        mon_df['Status'] = 'OK'

        return mon_df[['Symbol', 'Latest Timestamp', 'Rows', 'Status']]



    @render.table
    def raw_table():
        return raw_data().head(50)


    @render.download(filename="crypto_data.csv")
    def download_csv():
        yield raw_data().to_csv(index=False)


# www_dir = Path(__file__).parent / "www"
www_dir =  "/opt/shiny-app/www"

app = App(app_ui, server, static_assets=www_dir)
