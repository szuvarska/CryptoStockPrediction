#!/bin/bash

# --- CONFIGURATION ---
VENV_PATH="./crypto_ml_env"
APP_FILE="app.py"
PORT=8000
HOST="0.0.0.0"

# --- CRITICAL ENVIRONMENT VARIABLES (From your Jupyter Diagnostic) ---
export JAVA_HOME="/opt/java/openjdk"
export SPARK_HOME="/opt/spark"

# Update PATH to include Java and Spark binaries
export PATH="$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH"

echo "=================================================="
echo "Environment Setup:"
echo "JAVA_HOME:  $JAVA_HOME"
echo "SPARK_HOME: $SPARK_HOME"
echo "=================================================="

# --- ACTIVATE VIRTUAL ENVIRONMENT ---
echo "Activating virtual environment..."
source "$VENV_PATH/bin/activate"

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to activate virtual environment. Check VENV_PATH."
    exit 1
fi

# --- RUN APP ---
echo "Starting Python Shiny app..."
echo "Access locally via: http://localhost:$PORT"
shiny run --host $HOST --port $PORT $APP_FILE