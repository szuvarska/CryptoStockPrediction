import plotly.express as px
import plotly.graph_objects as go
from plots.helper_plots import  _empty_plot

def plot_actual_vs_predicted(df):
    """Plots Actual vs Predicted values over time."""
    if df is None or df.empty:
        return _empty_plot("No Data Available")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df['Datetime'],
        y=df['Actual'],
        name='Actual',
        line=dict(color='#0d6efd', width=2)
    ))
    fig.add_trace(go.Scatter(
        x=df['Datetime'],
        y=df['Predicted'],
        name='Predicted',
        line=dict(color='#ef553b', dash='dot', width=2)
    ))

    fig.update_layout(
        template="plotly_white",
        margin=dict(l=40, r=20, t=20, b=20),
        height=300,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    return fig


def plot_residuals(df):
    """Plots residual errors over time."""
    if df is None or df.empty:
        return _empty_plot("No Data Available")

    fig = px.scatter(
        df,
        x='Datetime',
        y='Residual',
        color_discrete_sequence=["#6c757d"],
        opacity=0.6
    )

    fig.add_hline(y=0, line_dash="dash", line_color="black")

    fig.update_layout(
        template="plotly_white",
        margin=dict(l=50, r=20, t=20, b=20),
        height=300,
        yaxis_title="Residuals",
        xaxis_title=None
    )
    return fig