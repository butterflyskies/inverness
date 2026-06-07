# Inverness — Decision Log

## D1: Notebook platform — Marimo

**Decision:** Marimo over Jupyter, Databricks Community Edition, and others.

**Why Marimo:**
- Reactive execution (change a cell, downstream cells auto-update) — closest to Databricks UX
- DuckDB is the default SQL backend — zero-config integration
- Notebooks are `.py` files — git-friendly, Prefect can orchestrate them natively
- Deployable as web apps via `marimo run` — notebooks become dashboards
- 21k GitHub stars, NumFOCUS affiliated, acquired by CoreWeave — real platform with real backing
- Production users: Cloudflare, DNB (migrated from Databricks), Sumble, BunkerHill Health

**Why not Jupyter:**
- JSON notebook format is git-hostile
- No reactive execution
- Reproducibility issues (out-of-order execution)
- Larger ecosystem but worse architecture for this use case

**Why not Databricks Community Edition:**
- Limited/deprecated free tier
- Cloud-dependent, not self-hostable
- Overkill governance for a solo stack

**Research:** Deep research report conducted 2026-06-07 with multi-source verification.

---

## D2: Storage pattern — Medallion (bronze/silver/gold)

**Decision:** Three-tier medallion pattern with Parquet as the primary format.

**Why:**
- Miranda knows this pattern from Databricks — zero conceptual overhead
- Raw JSON preservation (bronze) handles undocumented APIs that change without warning
- Parquet (silver/gold) gives DuckDB full-speed analytics queries
- No separate document DB needed — bronze files ARE the document store
- Avoids DuckDB single-writer contention (Prefect writes Parquet files, not DuckDB tables)

**Alternatives considered:**
- Document DB (MongoDB/CouchDB) for raw layer — adds operational complexity, DuckDB can query JSON directly
- Single-tier DuckDB tables — loses raw data preservation, creates write contention

---

## D3: Deployment model — Model A (volume-mounted code)

**Decision:** Volume-mounted code with image containing only the runtime.

**Why:**
- Fast iteration — edit a notebook, it's live immediately
- Simple deployment — `git pull` on the server
- Single developer, home server — reproducibility guarantees of Model B are overkill
- Migration to Model B (code baked in) is trivial — one `COPY` line in the Dockerfile

**When to reconsider:** If the stack is deployed to K8s for others, or if "what exactly is running" needs to be auditable.

---

## D4: Query engine — DuckDB (embedded)

**Decision:** DuckDB as embedded query engine, not a standalone database server.

**Why:**
- Zero-setup — no server process to manage
- Reads Parquet and JSON natively — works directly with the medallion pattern
- Arrow-native — integrates cleanly with the Python data ecosystem (Polars, pandas)
- Perfect for single-user analytics workloads on a laptop/server
- Lina's suggestion: Arrow as the columnar backbone

**Concurrency trade-off:** Single-writer model. Acceptable for solo use, mitigated by medallion pattern (writes go to Parquet files, not DuckDB tables).

---

## D5: Project name — Inverness

**Decision:** Named after Inverness Corona, the most prominent feature on Miranda (the Uranian moon).

**Why:** Miranda's naming convention uses Uranian system features. Inverness is the signature corona — Shakespearean (from Macbeth), short, distinctive, "where ambition gets executed." Unanimous vote from Ariadne, Vesper, and Miranda.

---

## Source
All decisions made during design session 2026-06-07, Discord thread. Participants: Miranda, Ariadne, Vesper. Lina contributed Arrow/DuckDB suggestion and infrastructure context.
