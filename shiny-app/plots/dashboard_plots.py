import plotly.graph_objects as go
import numpy as np
from plots.helper_plots import _empty_plot


def plot_price_trend(df):
    if df is None or df.empty:
        return _empty_plot("Waiting for data...")

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

    return fig


def plot_candlestick(df, symbol, time_range_label="", show_sma=True):
    if df is None or df.empty:
        return _empty_plot("Not enough data for candlesticks")

    # Base: Candlestick
    fig = go.Figure(data=[go.Candlestick(
        x=df['Datetime'],
        open=df['OpeningPrice'],
        high=df['HighestDayPrice'],
        low=df['LowestDayPrice'],
        close=df['CurrentPrice'],
        name=symbol,
        text=[
            f"Opening Price: {o:,.2f}<br>Highest Day Price: {h:,.2f}<br>Lowest Day Price: {l:,.2f}<br>Current Price: {c:,.2f}"
            for o, h, l, c in
            zip(df['OpeningPrice'], df['HighestDayPrice'], df['LowestDayPrice'], df['CurrentPrice'])],
        hoverinfo="x+text"
    )])

    # Only add SMAs if requested AND they exist in the dataframe
    if show_sma:
        if 'SMA7' in df.columns:
            fig.add_trace(go.Scatter(x=df['Datetime'], y=df['SMA7'], mode='lines', name=f'SMA 7',
                                     line=dict(color='purple', width=1.5)))
        if 'SMA30' in df.columns:
            fig.add_trace(go.Scatter(x=df['Datetime'], y=df['SMA30'], mode='lines', name=f'SMA 30',
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