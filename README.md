# Predicting Cryptocurrency Prices Based on Stock Behaviour

🔗[Showcase](#showcase)

## 1. Project Overview

This project aims to develop a system that enables traders and investors to track and analyse
cryptocurrency and stock market data. The system collects and stores historical data on
cryptocurrency prices, traditional currency exchange rates, and stock market values,
allowing users to generate meaningful insights and trends across these financial domains.

It includes a feature that predicts the behaviour of popular cryptocurrencies such as Bitcoin,
Ethereum, and Solana based on the strength of the US Dollar and current stock prices (S&P
500). This functionality is designed to assist users in making informed decisions about when to
buy or sell specific assets.

## 2. Repository Structure

* **[`apache-stack/`](apache-stack/)**: Contains the infrastructure definitions (Docker Compose), HDFS setup scripts, and notebooks.
* **[`shiny-app/`](shiny-app/)**: Contains the source code for the visualization dashboard.

## 3. Technologies Used

* **Infrastructure**: Docker, Docker Compose
* **Ingestion**: Apache NiFi
* **Messaging**: Apache Kafka, Zookeeper
* **Storage**: HDFS, Apache Hive, Apache HBase, PostgreSQL (Metastore)
* **Processing**: Apache Spark (PySpark), Spark MLlib
* **Web App**: Shiny for Python

## 4. Getting Started

### Prerequisites

* **Docker** and **Docker Compose** installed on your machine.
* **Git** for cloning the repository.

### Quick Start

1. Navigate to the infrastructure folder:
   ```bash
   cd apache-stack
   ```
2. Start the entire stack:
   ```bash
   docker-compose up -d --build
   ```
3. Initialize HDFS directories (wait for containers to be healthy first):
   ```bash
   ./hdfs-setup.sh
   ```
4. Access the Dashboard at **[http://localhost:8000](http://localhost:8000)**.

For detailed provisioning instructions, read [apache-stack/README.md](apache-stack/README.md).
For application details, read [shiny-app/README.md](shiny-app/README.md).

## Showcase

https://github.com/user-attachments/assets/e3d0958b-de8d-4907-858e-e3efa2571c4f

<img width="1151" height="621" alt="Architecture Diagram" src="https://github.com/user-attachments/assets/da662424-df5c-4b03-a43f-b157fa6e9954" />


