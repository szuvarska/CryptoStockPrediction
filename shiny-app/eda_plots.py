import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


def plot_correlation_matrix(df):
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
        return _empty_plot("Need >1 Asset for Correlation")

    fig = px.imshow(
        returns_df.corr(),
        text_auto=".2f",
        aspect="auto",
        color_continuous_scale="RdBu_r",
        zmin=-1, zmax=1,
        title="Asset Correlation Matrix (Daily Returns)",
        template="plotly_white",
        labels=dict(x="Asset", y="Asset", color="Corr")  # Rename legend/hover labels
    )

    fig.update_layout(
        # Increased Left Margin (l=100) for Y-axis labels
        margin=dict(l=100, r=20, t=60, b=20),
        xaxis_title="Asset",
        yaxis_title="Asset"
    )
    return fig


def plot_return_distribution(df, symbol):
    if df is None or df.empty:
        return _empty_plot("No Data")

    asset_df = df[df['Symbol'] == symbol].copy()
    if asset_df.empty:
        return _empty_plot(f"No Data found for {symbol}")

    asset_df = asset_df.sort_values("Datetime")
    asset_df['Return'] = asset_df['CurrentPrice'].pct_change() * 100
    asset_df = asset_df.dropna()

    fig = px.histogram(
        asset_df,
        x="Return",
        nbins=40,
        title=f"{symbol} Return Distribution",
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


def _empty_plot(text):
    fig = go.Figure()
    fig.add_annotation(text=text, showarrow=False, font={"size": 20})
    fig.update_layout(
        xaxis=dict(showgrid=False, showticklabels=False),
        yaxis=dict(showgrid=False, showticklabels=False),
        margin=dict(l=0, r=0, t=0, b=0)
    )
    return fig