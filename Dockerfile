FROM python:3.12-slim

LABEL maintainer="Miranda Schlese-Byrd <miranda.byrd@protonmail.com>"
LABEL description="Inverness — local-first baseball analytics platform"

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

RUN mkdir -p /data/bronze /data/silver /data/gold /mlruns

ENV MLFLOW_TRACKING_URI=file:///mlruns
ENV PREFECT_HOME=/app/config/prefect
ENV MARIMO_HOST=0.0.0.0

WORKDIR /app

EXPOSE 2718 4200 5000

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
