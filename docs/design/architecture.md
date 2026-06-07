# Inverness — Architecture

## Stack

| Component | Tool | Role |
|-----------|------|------|
| Notebooks | Marimo | Interactive exploration, dataviz, deployable as apps |
| Query engine | DuckDB (embedded) | SQL over Parquet/JSON, no server process |
| Storage format | Apache Arrow / Parquet | Columnar, analytics-optimized |
| Orchestration | Prefect | Scheduled flows, data pipeline management |
| Experiment tracking | MLflow | Model training, parameter logging, artifacts |
| Container | Docker | Runtime isolation, reproducible environment |

## Medallion pattern

```
bronze/              silver/              gold/
raw JSON from APIs → typed Parquet      → aggregated features
preserved as-is      schema-validated     modeling-ready
replay/debug         DuckDB-queryable    projection inputs
```

- **Bronze:** Raw JSON files, organized by `{source}/{date}/*.json`. This IS the document store — no DB overhead, just files. When an undocumented API changes shape, the raw is still there for replay.
- **Silver:** Prefect flows transform bronze → typed Parquet. Schema enforcement happens here. Column-oriented, Arrow-native, DuckDB reads at full speed.
- **Gold:** Aggregated feature tables for modeling. Player-seasons, rolling averages, projection inputs.

DuckDB queries all three tiers directly — it reads JSON and Parquet natively.

## Container architecture

```
┌─────────────────────────────────────┐
│           Container (runtime)       │
│                                     │
│  Marimo server ─── DuckDB (embed)   │
│  Prefect server (local)             │
│  MLflow tracking server             │
│                                     │
│  Volume mounts:                     │
│    /app/notebooks/  ← marimo .py    │
│    /app/flows/      ← prefect defs  │
│    /app/config/     ← service config│
│    /data/           ← persistent    │
│      ├── bronze/                    │
│      ├── silver/                    │
│      └── gold/                      │
│    /mlruns/         ← experiments   │
└─────────────────────────────────────┘
```

- DuckDB is embedded — runs inside Marimo and Prefect directly, no server process
- MLflow and Prefect run as lightweight local servers
- All components talk to the same `/data/` directory
- Volume mounts for code and data — container image contains only the runtime

## Data flow

1. Prefect flow fires (scheduled or manual)
2. Pulls JSON from API → writes to `/data/bronze/{source}/{date}/`
3. Transform step: bronze JSON → silver Parquet (schema validation, type coercion, dedup)
4. Optional: silver → gold aggregation (feature engineering)
5. Marimo notebooks query silver/gold via DuckDB
6. Model training logs to MLflow
7. Projections write back to gold tier

Marimo notebooks are `.py` files — a notebook that does the transform step IS a Prefect task. No rewrite from exploration to production.

## Deployment model (Model A)

```
Image (immutable, ~1-2 GB):
  python + marimo + duckdb + prefect + mlflow + deps

Volume (persistent, grows):
  notebooks/ + flows/ + config/ + data/ + mlruns/
```

- **Code changes:** `git pull` on the server, container sees new files immediately
- **Dependency changes:** rebuild image, restart container
- **Data:** persistent volume, survives container rebuilds

## DuckDB concurrency

DuckDB is single-writer. The medallion pattern avoids contention:
- Prefect writes Parquet files (no DuckDB lock)
- DuckDB reads Parquet on demand (read-only, no lock)
- Write lock only matters if gold-tier aggregation writes to DuckDB tables while Marimo queries — avoidable with scheduling

## Future: K8s migration

When the stack outgrows the home server:
- Same container image deploys to K8s
- `/data/` volume becomes a PVC
- Prefect can scale workers independently
- Add Model B (code baked into image) for reproducibility

## Source
Design session 2026-06-07. Participants: Miranda, Ariadne, Vesper.
