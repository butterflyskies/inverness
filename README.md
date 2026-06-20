# Inverness

A local-first baseball analytics platform replacing a professional Databricks workflow for solo research. DuckDB for analytical queries, Marimo for reactive notebooks, Prefect for pipeline orchestration, and MLflow for experiment tracking -- all in a single Docker container. Named for Inverness Corona, the signature feature on the Uranian moon Miranda.

## Quick Start

```bash
docker compose up --build -d
```

Three services will be available once the container is running:

| Service | Port | URL |
|---------|------|-----|
| Marimo (notebooks) | 2718 | http://localhost:2718 |
| Prefect (orchestration) | 4200 | http://localhost:4200 |
| MLflow (experiment tracking) | 5000 | http://localhost:5000 |

## Data Architecture

Data follows a medallion pattern across three tiers, stored on a persistent volume at `/data/`:

```
bronze/                silver/                gold/
raw JSON from APIs  -> typed Parquet       -> aggregated features
preserved as-is        schema-validated       modeling-ready
replay/debug           DuckDB-queryable       projection inputs
```

- **Bronze** -- Raw JSON organized by `{source}/{date}/`. Preserved as-is because baseball APIs are undocumented and change without warning. DuckDB queries JSON directly when needed.
- **Silver** -- Prefect flows transform bronze JSON into schema-validated Parquet. Column-oriented, Arrow-native, optimized for DuckDB reads.
- **Gold** -- Aggregated feature tables for modeling: player-seasons, rolling averages, projection inputs.

DuckDB is embedded (no server process) and reads all three tiers natively. The medallion pattern sidesteps DuckDB's single-writer constraint -- Prefect writes Parquet files while DuckDB reads them without contention.

## Project Structure

```
inverness/
├── Dockerfile              # Python 3.12-slim runtime image
├── entrypoint.sh           # Boots MLflow, Prefect, then Marimo (foreground)
├── compose.yaml            # Service definition and volume mounts
├── compose.override.yaml   # vesper-services network integration
├── requirements.txt        # Python dependencies
├── notebooks/              # Marimo .py notebooks (volume-mounted)
├── flows/                  # Prefect flow definitions (empty -- not yet written)
├── config/                 # Service configuration (empty -- not yet written)
├── docs/design/            # Architecture, requirements, and decision records
│   ├── architecture.md
│   ├── requirements.md
│   └── decisions.md
├── data/                   # Persistent data volume (not in repo)
│   ├── bronze/
│   ├── silver/
│   └── gold/
└── mlruns/                 # MLflow experiment data (not in repo)
```

## Dependencies

The container image bundles these core libraries:

- **marimo[sql]** -- Reactive notebooks with built-in DuckDB SQL support
- **duckdb** -- Embedded analytical query engine
- **prefect** -- Workflow orchestration
- **mlflow** -- Experiment tracking and model registry
- **polars / pyarrow** -- Data manipulation and columnar storage
- **altair** -- Declarative visualization

## Development

Code is volume-mounted, not baked into the image (Model A deployment):

- **Edit notebooks or flows** -- Changes are live immediately; no container restart needed.
- **Change Python dependencies** -- Rebuild the image: `docker compose up --build`.
- **Data** -- Persistent volume at `./data/`, survives container rebuilds.

The `compose.override.yaml` joins the container to an external `vesper-services` Docker network for home server integration. Remove or modify this file if running standalone.

## Known Issues

**MLflow 403 on hostname access.** MLflow's DNS rebinding guard rejects requests that arrive via hostname rather than `localhost`. Set the `MLFLOW_ALLOWED_HOSTS` environment variable in `compose.yaml` to your server's hostname to fix this.

**No shared Docker network by default.** The base `compose.yaml` does not define an external network. The `compose.override.yaml` can be used to add a network, which must already exist (`docker network create <network_name>`).

## Design Docs

Full architecture, requirements, and decision rationale live in `docs/design/`:

- `architecture.md` -- Stack diagram, medallion data flow, container layout, DuckDB concurrency model
- `requirements.md` -- Functional and non-functional requirements, constraints
- `decisions.md` -- Why Marimo over Jupyter, why DuckDB, why medallion, naming origin
