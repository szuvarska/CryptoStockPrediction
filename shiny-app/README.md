# CryptoStockPrediction Dashboard (Shiny)

This folder contains the source code for the frontend application built using **Shiny for Python**. The app visualizes real-time cryptocurrency data, stock indexes, and ML predictions.

## App Structure

* **`app.py`**: The main entry point. Defines the UI layout and Server logic.
* **`data_loader.py`**: Handles data retrieval from **HBase** (Real-time) and **Hive/Spark** (Historical). It includes logic to detect if running in Docker or locally.
* **`plots/`**: Contains Python modules for generating Plotly figures:
  * `dashboard_plots.py`
  * `eda_plots.py`
  * `eval_plots.py`
* **`www/`**: Static resources like CSS files.
* **`tests/`**: Unit tests for the application.

## How to Run

The easiest way to run the app is as part of the full stack. The app is containerized as `shiny-app` in the `apache-stack/docker-compose.yml`.

1. Navigate to `../apache-stack`.
2. Run `docker compose up -d --build shiny-app`.
3. Open **[http://localhost:8000](http://localhost:8000)**.
