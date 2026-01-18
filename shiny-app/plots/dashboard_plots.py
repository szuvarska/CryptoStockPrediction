import plotly.graph_objects as go
import numpy as np
import pandas as pd
from plots.helper_plots import _empty_plot


def _ensure_datetime(df):
    """
    Helper to ensure 'Datetime' column is datetime objects, not strings.
    Fixes: TypeError: can only concatenate str (not "Timedelta") to str
    """
    if df is None or df.empty: return df

    # If Datetime is not already a datetime type, convert it
    if not pd.api.types.is_datetime64_any_dtype(df['Datetime']):
        df = df.copy()
        df['Datetime'] = pd.to_datetime(df['Datetime'], errors='coerce')
        # Drop rows where conversion failed (NaT)
        df = df.dropna(subset=['Datetime'])

    return df


def plot_price_trend(df, time_range):
    if df is None or df.empty:
        return _empty_plot("Waiting for data...")

    df = _ensure_datetime(df)

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
        name="Price",
        hovertemplate="$%{y:,.2f}<extra></extra>"
    ))

    if 'PredictedPrice' in df.columns:
        valid_preds = df['PredictedPrice'].dropna()

        if not valid_preds.empty:
            # 1. Get Key Values
            pred_price = valid_preds.iloc[-1]
            last_price = df['CurrentPrice'].iloc[-1]
            last_ts = df['Datetime'].max()

            # 2. Define Future Time (e.g., +2 minutes for next update)
            time_to_add = {"1H": pd.Timedelta(minutes=10),
                           "24H": pd.Timedelta(hours=6),
                           "7D": pd.Timedelta(days=1),
                           "30D": pd.Timedelta(days=7),
                           "ALL": pd.Timedelta(days=7)}
            buffer = time_to_add.get(time_range, pd.Timedelta(hours=1))
            future_ts = last_ts + buffer

            # 3. Calculate Volatility for Confidence Interval
            volatility = df['CurrentPrice'].tail(50).std()
            if pd.isna(volatility) or volatility == 0:
                volatility = pred_price * 0.005  # Fallback to 0.5%

            upper_bound = pred_price + volatility
            lower_bound = pred_price - volatility

            # 4. Draw Confidence Interval (Shaded Box in Future)
            # We define a polygon: Last_TS -> Future_TS (Top) -> Future_TS -> Last_TS (Bottom)
            fig.add_trace(go.Scatter(
                x=[last_ts, future_ts, future_ts, last_ts],
                y=[upper_bound, upper_bound, lower_bound, lower_bound],
                fill='toself',
                fillcolor='rgba(50, 100, 255, 0.15)',
                line=dict(color='rgba(255,255,255,0)'),
                hoverinfo="skip",
                showlegend=False,
                name="Confidence Interval"
            ))

            # 5. Prediction Line (Text removed from here)
            fig.add_trace(go.Scatter(
                x=[last_ts, future_ts],
                y=[pred_price, pred_price],
                mode='lines',  # Text removed, lines only
                line=dict(color='#3366cc', width=3, dash='dash'),
                name="Predicted Price",
                hovertemplate="$%{y:,.2f}<extra></extra>"
            ))

            # 6. Text Annotation (Added separately for better spacing control)
            fig.add_annotation(
                x=last_ts,
                y=pred_price,
                text=f"Predicted Price: ${pred_price:,.2f}",
                showarrow=False,
                xanchor="left",
                yanchor="bottom",
                yshift=10,  # Adds 10px vertical space between line and text
                xshift=5,
                font=dict(color="#3366cc", size=12)
            )

            # 7. Connector Line
            fig.add_trace(go.Scatter(
                x=[last_ts, last_ts],
                y=[last_price, pred_price],
                mode='lines',
                line=dict(color='#3366cc', width=1, dash='dot'),
                hoverinfo='skip',
                showlegend=False
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

    return fig


def plot_candlestick(df, symbol, time_range_label="", show_sma=True):
    if df is None or df.empty:
        return _empty_plot("Not enough data for candlesticks")

    df = _ensure_datetime(df)

    plot_df = df.copy()

    # if time_range_label in ['1H', '24H']:
    #     # Set index for resampling
    #     plot_df = plot_df.set_index('Datetime')
    #
    #     freq_map = {"1H": "1min", "24H": "10min", "7D": "1H", "30D": "6H", "ALL": "1D"}
    #     freq = freq_map.get(time_range_label, "4H")
    #
    #     # Resample OHLC
    #     # Open=first, High=max, Low=min, Close=last
    #     resampled = plot_df.resample(freq).agg({
    #         'CurrentPrice': 'last',  # Close
    #         'OpeningPrice': 'first',  # Open
    #         'HighestDayPrice': 'max',  # High
    #         'LowestDayPrice': 'min',  # Low
    #         'SMA7': 'last',  # Indicators (approx)
    #         'SMA30': 'last'
    #     })
    #
    #     # Drop empty intervals (no trades)
    #     resampled = resampled.dropna(subset=['CurrentPrice'])
    #
    #     # Reset index to get Datetime column back
    #     plot_df = resampled.reset_index()

    # Base: Candlestick
    fig = go.Figure(data=[go.Candlestick(
        x=plot_df['Datetime'],
        open=plot_df['OpeningPrice'],
        high=plot_df['HighestDayPrice'],
        low=plot_df['LowestDayPrice'],
        close=plot_df['CurrentPrice'],
        name=symbol,
        text=[
            f"Opening Price: {o:,.2f}<br>Highest Day Price: {h:,.2f}<br>Lowest Day Price: {l:,.2f}<br>Current Price: {c:,.2f}"
            for o, h, l, c in
            zip(plot_df['OpeningPrice'], plot_df['HighestDayPrice'], plot_df['LowestDayPrice'], plot_df['CurrentPrice'])],
        hoverinfo="x+text"
    )])

    # Only add SMAs if requested AND they exist in the dataframe
    if show_sma:
        if 'SMA7' in plot_df.columns:
            fig.add_trace(go.Scatter(x=plot_df['Datetime'], y=plot_df['SMA7'], mode='lines', name=f'SMA 7',
                                     line=dict(color='purple', width=1.5)))
        if 'SMA30' in plot_df.columns:
            fig.add_trace(go.Scatter(x=plot_df['Datetime'], y=plot_df['SMA30'], mode='lines', name=f'SMA 30',
                                     line=dict(color='blue', width=1.5)))

    # Layout
    fig.update_layout(
        title=dict(text=f"{time_range_label} Price Action", x=0.01),
        yaxis_title="Price ($)",
        margin=dict(l=60, r=20, t=40, b=40),  # Increased bottom margin for rangeslider
        hovermode="x unified",
        height=500,
        xaxis_rangeslider_visible=False,  # Hide the mini-slider to save space
        xaxis=dict(type='date'),
        template="plotly_white"
    )
    fig.update_xaxes(tickformat="%b %d %H:%M")
    fig.update_yaxes(tickprefix="$")

    return fig