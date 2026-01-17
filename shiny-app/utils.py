import pandas as pd
import numpy as np
from shiny import ui

def render_plotly_html(fig, height="100%"):
    """Converts a Plotly figure to an HTML string for robust rendering."""
    if fig is None: return ui.HTML("")
    html = fig.to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True, 'displayModeBar': False})
    return ui.HTML(f'<div style="height: {height}; width: 100%;">{html}</div>')


def filter_data_vectorized(df, t_24h_limit, t_7d_limit):
    """
    Applies retention policy filtering on the entire DataFrame at once using vectorized operations.

    Policy:
    - 1m data: Keep if > 24h ago
    - 10m data: Keep if > 7d ago AND <= 24h ago
    - 1d data: Keep if <= 7d ago
    """
    if df.empty: return df

    # Create Boolean Masks (Vectorized)
    # 1. Minute Data
    mask_1m = (df['Granularity'] == '1m') & (df['Datetime'] > t_24h_limit)

    # 2. Ten-Minute Data
    mask_10m = (df['Granularity'] == '10m') & (df['Datetime'] > t_7d_limit) & (df['Datetime'] <= t_24h_limit)

    # 3. Daily Data
    mask_1d = (df['Granularity'] == '1d') & (df['Datetime'] <= t_7d_limit)

    # Combine and Filter
    return df[mask_1m | mask_10m | mask_1d].copy()