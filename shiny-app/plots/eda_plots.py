import plotly.express as px
import plotly.graph_objects as go
from plots.helper_plots import  _empty_plot


def plot_correlation_matrix(df, time_range_label=""):
    if df is None or df.empty:
        return _empty_plot("No Data for Correlation")

    # Pivot: Rows=Time, Cols=Symbol, Values=Price
    pivot_df = df.pivot_table(
        index='Datetime',
        columns='Symbol',
        values='CurrentPrice',
        aggfunc='mean'
    )

    # Correlation of Returns
    returns_df = pivot_df.pct_change().dropna()

    if returns_df.empty or returns_df.shape[1] < 2:
        return _empty_plot("Insufficient assets for correlation")

    fig = px.imshow(
        returns_df.corr(),
        text_auto=".2f",
        aspect="auto",
        color_continuous_scale="RdBu_r",
        zmin=-1, zmax=1,
        title=f"Asset Correlation ({time_range_label})",
        template="plotly_white"
    )

    fig.update_layout(
        # Increased Left Margin (l=100) for Y-axis labels
        margin=dict(l=100, r=20, t=60, b=20),
        xaxis_title="Asset",
        yaxis_title="Asset"
    )
    fig.update_yaxes(ticksuffix="  ")

    return fig


def plot_return_distribution(df, symbol, time_range_label=""):
    if df is None or df.empty:
        return _empty_plot("No Data")

    df = df.sort_values("Datetime")
    df['Return'] = df['CurrentPrice'].astype(float).pct_change() * 100
    df = df.dropna(subset=['Return'])

    fig = px.histogram(
        df,
        x="Return",
        nbins=40,
        title=f"{symbol} Return Distribution ({time_range_label})",
        template="plotly_white",
        color_discrete_sequence=["#0d6efd"]
    )

    fig.update_layout(
        xaxis_title="Daily Return (%)",
        yaxis_title="Frequency",
        margin=dict(l=60, r=20, t=60, b=20),
        bargap=0.1
    )

    return fig


def plot_stock_indicators(df_stock):
    if df_stock is None or df_stock.empty: return _empty_plot("S&P 500 Data Unavailable")

    fig = go.Figure()
    # Price Line
    fig.add_trace(go.Scatter(x=df_stock['Datetime'], y=df_stock['CurrentPrice'], mode='lines', name='S&P 500',
                             line=dict(color='black', width=2)))
    # Moving Averages
    fig.add_trace(go.Scatter(x=df_stock['Datetime'], y=df_stock['FiftyDayAveragePrice'], mode='lines', name='50-Day MA',
                             line=dict(color='green', width=1.5)))
    fig.add_trace(
        go.Scatter(x=df_stock['Datetime'], y=df_stock['TwoHundredDaysAveragePrice'], mode='lines', name='200-Day MA',
                   line=dict(color='red', width=1.5)))

    fig.update_layout(
        title="S&P 500 Market Trends (Golden Cross Check)",
        template="plotly_white",
        hovermode="x unified",
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=0.95, xanchor="right", x=1)
    )

    fig.update_yaxes(ticksuffix="  ")

    return fig


def plot_forex_volume(df_forex):
    if df_forex is None or df_forex.empty: return _empty_plot("Forex Data Unavailable")

    # Resample to daily if needed, or plot raw stream
    # Assuming daily aggregation for volume makes sense
    df_daily = df_forex.set_index('Datetime').resample('1D').sum().reset_index()

    fig = px.bar(df_daily, x='Datetime', y='VolumeTraded', title="Forex Daily Volume", template="plotly_white")
    fig.update_traces(marker_color='#636efa')
    fig.update_layout(margin=dict(l=20, r=20, t=40, b=20))
    return fig
