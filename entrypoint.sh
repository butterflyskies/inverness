#!/bin/bash
set -e

echo "Starting Inverness stack..."

# Ensure writable directories exist (volume mounts may be empty)
mkdir -p /home/inverness/.prefect/ui /mlruns

# Start MLflow tracking server (background)
mlflow server \
    --backend-store-uri sqlite:///mlruns/mlflow.db \
    --default-artifact-root /mlruns/artifacts \
    --host 0.0.0.0 \
    --port 5000 &
echo "MLflow tracking server starting on :5000"

# Start Prefect server (background)
prefect server start \
    --host 0.0.0.0 \
    --port 4200 &
echo "Prefect server starting on :4200"

# Wait for services to be ready
for port in 5000 4200; do
    for i in $(seq 1 30); do
        if curl -sf "http://localhost:$port/" > /dev/null 2>&1; then
            echo "Service on :$port is ready"
            break
        fi
        sleep 1
    done
done

# Start Marimo (foreground — keeps the container alive)
echo "Starting Marimo on :2718"
exec marimo edit \
    --host 0.0.0.0 \
    --port 2718 \
    --no-token \
    /app/notebooks/
