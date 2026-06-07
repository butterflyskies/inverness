#!/bin/bash
set -e

echo "Starting Inverness stack..."

# Start MLflow tracking server (background)
mlflow server \
    --backend-store-uri file:///mlruns \
    --host 0.0.0.0 \
    --port 5000 &
echo "MLflow tracking server started on :5000"

# Start Prefect server (background)
prefect server start \
    --host 0.0.0.0 \
    --port 4200 &
echo "Prefect server started on :4200"

# Wait for background services to initialize
sleep 3

# Start Marimo (foreground — keeps the container alive)
echo "Starting Marimo on :2718"
exec marimo edit \
    --host 0.0.0.0 \
    --port 2718 \
    --no-token \
    /app/notebooks/
