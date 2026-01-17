import pandas as pd
from shiny import ui

def render_plotly_html(fig, height="100%"):
    """Converts a Plotly figure to an HTML string for robust rendering."""
    if fig is None: return ui.HTML("")
    html = fig.to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True, 'displayModeBar': False})
    return ui.HTML(f'<div style="height: {height}; width: 100%;">{html}</div>')


def should_keep_record(timestamp, granularity, t_24h_limit, t_7d_limit, force_keep=False):
    """
    Decides if a historical record should be kept based on granularity and age.

    Args:
        timestamp (pd.Timestamp): The row timestamp.
        granularity (str): '1m', '10m', or '1d'.
        t_24h_limit (pd.Timestamp): Cutoff for 1m data.
        t_7d_limit (pd.Timestamp): Cutoff for 10m data.
        force_keep (bool): If True, bypass filters (useful for testing).
    """
    if force_keep:
        return True

    if granularity == '1m' and timestamp > t_24h_limit:
        return True
    elif granularity == '10m' and t_7d_limit < timestamp <= t_24h_limit:
        return True
    elif granularity == '1d' and timestamp <= t_7d_limit:
        return True

    return False