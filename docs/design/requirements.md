# Inverness — Requirements

## Problem statement

Miranda is a data scientist with 4 years of deep Databricks experience who needs her own local analytics stack for baseball research. She misses the integrated UX of enterprise data platforms but doesn't need (or want) the multi-tenant overhead.

## Stakeholder

- **Miranda Schlese-Byrd** — data scientist, Databricks power user, MLflow-fluent, baseball analytics domain expert.

## Functional requirements

### Data ingestion
- Pull JSON from multiple baseball APIs (MLB Stats API, FanGraphs, Statcast, Fantrax)
- APIs are undocumented and can change without warning — raw data must be preserved as-is for replay/debugging
- Scheduled ingestion via orchestrator (Prefect)

### Storage
- Raw JSON preserved in bronze tier (document-store flexibility)
- Transformed data in Parquet format for analytics performance (silver tier)
- Aggregated feature tables for modeling (gold tier)
- Single query engine across all tiers

### Analytics
- Interactive notebook environment for exploration and dataviz
- SQL-first query interface over tabular data
- Notebooks must be version-controllable (git-friendly format)

### Experiment tracking
- MLflow for model training, parameter logging, artifact management
- Miranda is fluent in MLflow — no learning curve acceptable for alternatives

### Orchestration
- Prefect for data flows — scheduled pulls, transforms, aggregation
- Exploration-to-production path should be frictionless (no notebook-to-script rewrite)

### Modeling (roadmap)
- Custom baseball projections — player performance forecasting, WAR prediction
- Deep learning experiments
- Homegrown alternative to ZiPS/Steamer projections

## Non-functional requirements

### Hosting
- Runs in a container on Miranda's home server
- Must not depend on a laptop being open/awake
- K8s deployment path for future scaling (potentially on shared infrastructure)

### Deployment model
- Model A: volume-mounted code, image for runtime only
- Code deploys via `git pull`, image rebuilds only for dependency changes
- Single-person stack — no multi-tenant, no governance overhead

### Consumption
- Notebooks are the primary, first-class product
- Dashboards are secondary — emerge organically from notebooks with dataviz
- Marimo's `marimo run` as the dashboard deployment path (roadmap)

## Constraints
- Solo developer — tooling must be operationally simple
- No cloud dependencies — fully local/self-hosted
- DuckDB is single-writer — architecture must avoid write contention

## Source
Design session 2026-06-07, Discord thread #mini-databricks (renamed to #inverness). Participants: Miranda, Ariadne, Vesper.
