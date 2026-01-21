# CryptoStockPrediction Dashboard (Shiny)

This folder contains the source code for the frontend application built using **Shiny for Python**. The app visualizes real-time cryptocurrency data, stock indexes, and ML predictions.

## App Structure

The application follows a modular architecture:

* **`app.py`**: The main entry point that assembles and launches the application.
* **`ui_layout.py`**: Defines the user interface structure, including the **Dashboard**, **EDA**, **Model Eval**, and **Raw Data** tabs.
* **`server_logic.py`**: Contains the reactive server backend. Handles real-time polling, dynamic chart updates, **volatility alerts (pop-ups)**, and CSV downloads.
* **`data_loader.py`**: The robust data retrieval engine. It implements a **two-phase loading strategy**:
  * **Initialization:** Uses parallel threads (`ThreadPoolExecutor`) to fetch historical data for all symbols simultaneously.
  * **Real-Time:** Performs optimized **HBase Batch Lookups** every 30s to append only the latest ticks.
* **`plots/`**: Contains Python modules for generating interactive Plotly figures:
  * `dashboard_plots.py`: Price trends, real-time predictions, and candlestick charts.
  * `eda_plots.py`: Correlation matrices and return distributions.
  * `eval_plots.py`: Model performance visuals (Actual vs Predicted).
* **`config.py`**: Centralized configuration for database hosts and global constants.
* **`tests/`**: End-to-end test suite using **Playwright**.

## How to Run

The easiest way to run the app is as part of the full stack. The app is containerized as `shiny-app` in the `apache-stack/docker-compose.yml`.

1. Navigate to `../apache-stack`.
2. Run the stack:
   ```bash
   docker compose up -d --build shiny-app
