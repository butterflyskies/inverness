# Inverness

Local-first data science platform for baseball analytics. Named for the signature corona on Miranda (Uranus).

## What this is

A containerized stack replacing the Databricks workflow Miranda used professionally, adapted for solo research: DuckDB + Marimo + Prefect + MLflow, running on a home server.

## Architecture

See `docs/design/` for full design docs:
- `architecture.md` — stack diagram, medallion pattern, component layout
- `requirements.md` — Miranda's requirements and constraints
- `decisions.md` — why Marimo over Jupyter, why Model A, why DuckDB, alternatives rejected

For design context and motivation, recall the `inverness` scope in memory-mcp.

## Structure

- `Dockerfile` — runtime image (Python + tools, no code/data)
- `notebooks/` — Marimo `.py` notebooks (volume-mounted, not baked into image)
- `flows/` — Prefect flow definitions
- `config/` — MLflow, Prefect, DuckDB configuration
- `docs/design/` — architecture and decision docs

## Data (not in repo)

Data lives on a persistent volume mount, not in version control:
```
/data/
├── bronze/   ← raw API JSON responses
├── silver/   ← transformed Parquet files
└── gold/     ← aggregated feature tables
```

## Development

Model A deployment: code is volume-mounted, not baked into the image.
- Edit notebooks/flows: changes are live immediately
- Change dependencies: rebuild the image

## Testing philosophy

Inherited from fantrax-mcp: don't trust API documentation. Capture live responses, test against those.
