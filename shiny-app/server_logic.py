import pandas as pd
import numpy as np
import asyncio
import traceback
from datetime import datetime
from shiny import ui, reactive, render
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_percentage_error

# --- Local Imports ---
from data_loader import load_historical_data, load_recent_prices_data, get_latest_ticks, load_forex_data, load_spark_model
from plots.eda_plots import (
    plot_correlation_matrix,
    plot_return_distribution,
    plot_stock_indicators,
    plot_forex_volume
)
from plots.dashboard_plots import plot_price_trend, plot_candlestick
from plots.eval_plots import plot_actual_vs_predicted, plot_residuals
from utils import render_plotly_html
from config import ALL_ASSETS, TIME_RANGE_CONFIG


def server(input, output, session):

    # 1. CENTRALIZED DATA LOADING
    data_store = reactive.Value({
        "crypto": pd.DataFrame(),
        "forex": pd.DataFrame()
    })

    # SAFETY FLAG: Prevents real-time updates from running before history is loaded
    is_initialized = reactive.Value(False)

    last_known_prices = reactive.Value({})

    @reactive.Effect
    async def _init_load():
        print("Starting heavy historical data load (background)...")

        loop = asyncio.get_event_loop()

        # 1. Load Long-Term History (Aggregates)
        history_df = await loop.run_in_executor(None, load_historical_data)

        # 2. Load Recent "Last Day" Raw Ticks (Prices + Predictions)
        print("Loading recent raw ticks (last 48h)...")
        recent_df = await loop.run_in_executor(None, load_recent_prices_data)

        # 3. Load Forex
        forex_df = await loop.run_in_executor(None, load_forex_data)

        # 4. Merge Data
        full_crypto = pd.concat([history_df, recent_df], ignore_index=True)

        if not full_crypto.empty:
            full_crypto = full_crypto.sort_values(['Symbol', 'Datetime'])

            # Deduplicate: overlapping timestamps (if any) favour the 'recent_df'
            # because it contains the 'PredictedPrice' column
            full_crypto = full_crypto.drop_duplicates(subset=['Symbol', 'Datetime'], keep='last')

            # Fill missing moving averages in the raw data by forward filling from history
            full_crypto['FiftyDayAveragePrice'] = full_crypto.groupby('Symbol')['FiftyDayAveragePrice'].ffill()
            full_crypto['TwoHundredDaysAveragePrice'] = full_crypto.groupby('Symbol')[
                'TwoHundredDaysAveragePrice'].ffill()

        # Update store and set flag
        data_store.set({
            "crypto": full_crypto,
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

        reactive.invalidate_later(30)

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

                    for col in ['FiftyDayAveragePrice', 'TwoHundredDaysAveragePrice', 'SMA7', 'SMA30']:
                        if col in updated_df.columns:
                            updated_df[col] = updated_df.groupby('Symbol')[col].ffill().bfill()

                    data_store.set({
                        "crypto": updated_df,
                        "forex": current_data.get("forex")
                    })

            except Exception as e:
                print(f"Error Message: {e}")
                traceback.print_exc()

    # --- HELPER: GENERIC TIME FILTER ---
    def filter_by_time(df, time_range, date_col='Datetime'):
        """Filters any dataframe by the selected time range using config."""
        if df is None or df.empty: return df

        config = TIME_RANGE_CONFIG.get(time_range)

        if config and config.get("offset"):
            cutoff_time = df[date_col].max() - config["offset"]
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

    def resample_df(df, time_range):
        """
        Aggregates dense data into readable candles/points based on the time window config.
        """
        if df is None or df.empty: return df

        # Get frequency from config, default to 1H if unknown
        freq = TIME_RANGE_CONFIG.get(time_range, {}).get("freq", "1H")

        # Prepare for resampling
        if not isinstance(df.index, pd.DatetimeIndex):
            dff = df.set_index('Datetime')
        else:
            dff = df.copy()

        # Define how to aggregate each column
        agg_dict = {
            'CurrentPrice': 'last',  # Close
            'OpeningPrice': 'first',  # Open
            'HighestDayPrice': 'max',  # High
            'LowestDayPrice': 'min',  # Low
        }

        # Add optional columns only if they exist
        for col in ['PredictedPrice', 'SMA7', 'SMA30', 'FiftyDayAveragePrice', 'TwoHundredDaysAveragePrice']:
            if col in dff.columns:
                agg_dict[col] = 'last'

        # Resample
        try:
            res = dff.resample(freq).agg(agg_dict)
            # Remove empty bins
            return res.dropna(subset=['CurrentPrice']).reset_index()
        except Exception:
            return df

    @reactive.Calc
    def plot_data():
        """
        Returns the filtered data RESAMPLED for efficient plotting.
        Use this for Charts, but use 'filtered_crypto_specific' for Metrics/Raw Data.
        """
        df = filtered_crypto_specific()
        return resample_df(df, input.time_range())


    # --- DASHBOARD OUTPUTS ---
    @render.ui
    def vbox_price():
        df = filtered_crypto_specific()
        if df.empty: return "Loading..."
        return f"${df['CurrentPrice'].iloc[-1]:,.2f}"

    @render.ui
    def vbox_change_label():
        return TIME_RANGE_CONFIG.get(input.time_range(), {}).get("change_label", "Change")

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
        return render_plotly_html(plot_price_trend(plot_data(), input.time_range()), height="400px")

    @render.ui
    def candle_chart_view():
        return render_plotly_html(plot_candlestick(
            plot_data(),
            input.crypto_select(),
            input.time_range(),
            show_sma=True
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
        return render_plotly_html(plot_stock_indicators(filtered_crypto_specific(), input.crypto_select()))

    @render.ui
    def forex_chart_view():
        return render_plotly_html(plot_forex_volume(filtered_forex()))

    # --- MODEL EVALUATION (REAL DATA) ---
    @reactive.Calc
    def get_eval_data():
        """
        Prepares real data for evaluation plots and metrics.
        Filters for rows where we actually have a prediction.
        """
        df = filtered_crypto_specific()
        if df is None or df.empty: return pd.DataFrame()

        # We need both Actual (CurrentPrice) and Predicted (PredictedPrice)
        if 'PredictedPrice' not in df.columns:
            return pd.DataFrame()

        # Create a clean subset with no NaNs in Prediction or Price
        eval_df = df[['Datetime', 'CurrentPrice', 'PredictedPrice']].copy()
        eval_df = eval_df.dropna(subset=['CurrentPrice', 'PredictedPrice'])

        if eval_df.empty:
            return pd.DataFrame()

        # Rename for compatibility with plot functions
        eval_df = eval_df.rename(columns={
            'CurrentPrice': 'Actual',
            'PredictedPrice': 'Predicted'
        })

        # Calculate Residuals (Actual - Predicted)
        eval_df['Residual'] = eval_df['Actual'] - eval_df['Predicted']

        return eval_df

    @render.ui
    def eval_rmse():
        df = get_eval_data()
        if df.empty: return "-"

        rmse = np.sqrt(mean_squared_error(df['Actual'], df['Predicted']))
        return ui.div(f"${rmse:,.2f}", class_="metric-value")

    @render.ui
    def eval_mape():
        df = get_eval_data()
        if df.empty: return "-"

        mape = mean_absolute_percentage_error(df['Actual'], df['Predicted']) * 100
        return ui.div(f"{mape:.2f}%", class_="metric-value")

    @render.ui
    def eval_dir():
        """
        Calculates Directional Accuracy:
        Did the model correctly predict the direction of the price movement?
        """
        df = get_eval_data()
        if df.empty or len(df) < 2: return "-"

        # Calculate step-by-step changes
        # Note: We compare Previous Actual to Current Actual vs Previous Actual to Current Predicted
        prev_actual = df['Actual'].shift(1)

        actual_change = df['Actual'] - prev_actual
        predicted_change = df['Predicted'] - prev_actual

        # Check if signs match (ignoring the first row which is NaN)
        correct_direction = np.sign(actual_change) == np.sign(predicted_change)
        accuracy = correct_direction.mean() * 100

        return ui.div(f"{accuracy:.1f}%", class_="metric-value")

    @render.ui
    def eval_r2():
        df = get_eval_data()
        if df.empty or len(df) < 2: return "-"

        r2 = r2_score(df['Actual'], df['Predicted'])
        return ui.div(f"{r2:.3f}", class_="metric-value")

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
        # Uses the real evaluation data
        return render_plotly_html(plot_actual_vs_predicted(get_eval_data()), height="300px")

    @render.ui
    def eval_resid_chart():
        # Uses the real evaluation data
        return render_plotly_html(plot_residuals(get_eval_data()), height="300px")

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
        return "30 seconds"


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

    @render.ui
    def spark_model_output():
        return ui.pre(str(load_spark_model('hdfs://namenode:8020/models/btc_model')))

    # REAL-TIME MONITORING (Specific to Chosen Asset)
    @reactive.Effect
    def _monitor_selected_asset():
        # Use the dataframe that is ALREADY filtered by input.crypto_select()
        df = filtered_crypto_specific()

        if df is None or df.empty:
            return

        # Get the currently selected symbol
        current_symbol = input.crypto_select()
        full_name = ALL_ASSETS.get(current_symbol, current_symbol)

        # Get the very latest price from the filtered data
        current_price = df['CurrentPrice'].iloc[-1]

        # Retrieve history dict
        history = last_known_prices.get()
        last_price = history.get(current_symbol)

        # Check for change if we have a history for THIS symbol
        if last_price is not None:
            change_pct = (current_price - last_price) / last_price

            # Threshold: 0.2%
            if abs(change_pct) >= 0.0003:
                # Determine Direction and Color
                if change_pct > 0:
                    direction = "SURGE 🚀"
                    # UPDATED: Use CSS class instead of inline style
                    alert_content = ui.span(
                        f"{full_name} Alert: {direction} (+{change_pct:.2%})",
                        class_="text-surge"
                    )
                    msg_type = "default"
                else:
                    direction = "CRASH 📉"
                    alert_content = ui.span(
                        f"{full_name} Alert: {direction} ({change_pct:.2%})",
                        class_="text-crash"
                    )
                    msg_type = "default" # error

                ui.notification_show(
                    alert_content,
                    type=msg_type,
                    duration=None
                )

        history[current_symbol] = current_price
        last_known_prices.set(history)
    #
    # def inject_price_change(multiplier):
    #     store = data_store.get()
    #     df = store.get("crypto")
    #
    #     if df is None or df.empty: return
    #
    #     symbol = input.crypto_select()
    #
    #     # Copy and modify last row
    #     last_row = df[df['Symbol'] == symbol].iloc[-1].copy()
    #     last_row['Datetime'] = pd.Timestamp.now()
    #     last_row['CurrentPrice'] = last_row['CurrentPrice'] * multiplier
    #
    #     new_row_df = pd.DataFrame([last_row])
    #     updated_df = pd.concat([df, new_row_df], ignore_index=True)
    #
    #     print(f"DEBUG: Injecting {multiplier}x price for {symbol}")
    #
    #     data_store.set({
    #         "crypto": updated_df,
    #         "forex": store.get("forex")
    #     })
    #
    # @reactive.Effect
    # @reactive.event(input.inject_crash)
    # def _inject_crash_data():
    #     inject_price_change(0.95)  # -5%
    #
    # @reactive.Effect
    # @reactive.event(input.inject_surge)
    # def _inject_surge_data():
    #     inject_price_change(1.05)  # +5%
