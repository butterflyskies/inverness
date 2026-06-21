"""
Transactions flow -- pull MLB transactions as an append-only event table.

Bronze: raw JSON from MLB Stats API, partitioned by date.
Silver: append-only transactions.parquet, deduplicated by (transaction_id, player_id).
"""

from datetime import date, timedelta
from pathlib import Path

import json
import httpx
import polars as pl
from prefect import flow, get_run_logger, task
from prefect.client.schemas.schedules import CronSchedule

MLB_BASE = "https://statsapi.mlb.com/api/v1"
BRONZE_ROOT = Path("/data/bronze/transactions")
SILVER_ROOT = Path("/data/silver/transactions")
SILVER_FILE = SILVER_ROOT / "transactions.parquet"

SCHEMA = {
    "transaction_id": pl.Int64,
    "player_id": pl.Int64,
    "player_name": pl.Utf8,
    "from_team_id": pl.Int64,
    "from_team_name": pl.Utf8,
    "to_team_id": pl.Int64,
    "to_team_name": pl.Utf8,
    "date": pl.Date,
    "effective_date": pl.Date,
    "resolution_date": pl.Date,
    "type_code": pl.Utf8,
    "type_desc": pl.Utf8,
    "description": pl.Utf8,
}


@task(retries=2, retry_delay_seconds=10)
async def fetch_transactions(
    client: httpx.AsyncClient, start: date, end: date
) -> dict:
    """Fetch transactions from MLB Stats API for the given date range."""
    resp = await client.get(
        f"{MLB_BASE}/transactions",
        params={"startDate": start.isoformat(), "endDate": end.isoformat()},
    )
    resp.raise_for_status()
    return resp.json()


@task
def write_bronze(raw: dict, run_date: date) -> Path:
    """Write raw JSON response to bronze layer, partitioned by date.

    Args:
        raw: Raw JSON response from MLB Stats API.
        run_date: ISO date for the partition directory.

    Returns:
        Path to the directory where data was written.
    """
    day_dir = BRONZE_ROOT / run_date.isoformat()
    day_dir.mkdir(parents=True, exist_ok=True)
    (day_dir / "transactions.json").write_text(json.dumps(raw, indent=2))
    return day_dir


@task
def flatten_transactions(raw: dict) -> pl.DataFrame:
    """Flatten raw transaction JSON into a tabular DataFrame.

    Extracts nested person, fromTeam, and toTeam fields into flat columns.
    Missing fromTeam/toTeam fields (common for assignments, releases) become null.

    Returns:
        DataFrame with the canonical transaction schema.
    """
    txns = raw.get("transactions", [])
    if not txns:
        return pl.DataFrame(schema=SCHEMA)

    rows = []
    for t in txns:
        person = t.get("person")
        if person is None:
            continue
        from_team = t.get("fromTeam")
        to_team = t.get("toTeam")
        rows.append(
            {
                "transaction_id": t["id"],
                "player_id": person["id"],
                "player_name": person.get("fullName"),
                "from_team_id": from_team["id"] if from_team else None,
                "from_team_name": from_team["name"] if from_team else None,
                "to_team_id": to_team["id"] if to_team else None,
                "to_team_name": to_team["name"] if to_team else None,
                "date": t.get("date"),
                "effective_date": t.get("effectiveDate"),
                "resolution_date": t.get("resolutionDate"),
                "type_code": t.get("typeCode"),
                "type_desc": t.get("typeDesc"),
                "description": t.get("description"),
            }
        )

    return pl.DataFrame(rows, schema=SCHEMA)


@task
def write_silver(new_df: pl.DataFrame) -> None:
    """Append new transactions to silver parquet, deduplicating on re-runs.

    Deduplication key: (transaction_id, player_id). On re-runs for the same
    date range, existing rows with matching keys are replaced by the fresh data.
    """
    logger = get_run_logger()
    SILVER_ROOT.mkdir(parents=True, exist_ok=True)

    if not len(new_df):
        logger.info("No transactions to write")
        return

    if SILVER_FILE.exists():
        existing = pl.read_parquet(SILVER_FILE)
        # Drop any rows that match the incoming keys (idempotent re-run)
        dedup_keys = new_df.select("transaction_id", "player_id")
        existing = existing.join(
            dedup_keys, on=["transaction_id", "player_id"], how="anti"
        )
        combined = pl.concat([existing, new_df], how="diagonal_relaxed")
    else:
        combined = new_df

    combined = combined.sort("date", "transaction_id")
    combined.write_parquet(SILVER_FILE)
    logger.info("Wrote %d total transactions to %s", len(combined), SILVER_FILE)


@flow(
    name="transactions",
    description="Pull MLB transactions, maintain append-only event table in silver layer.",
)
async def transactions(
    start_date: date | None = None,
    end_date: date | None = None,
):
    """Pull MLB transactions and append to silver event table.

    Args:
        start_date: Start of date range (inclusive). Defaults to yesterday.
        end_date: End of date range (inclusive). Defaults to today.
    """
    logger = get_run_logger()
    today = date.today()
    start_date = start_date or (today - timedelta(days=1))
    end_date = end_date or today
    logger.info("Transactions flow for %s to %s", start_date, end_date)

    async with httpx.AsyncClient(timeout=30.0) as client:
        raw = await fetch_transactions(client, start_date, end_date)

    txn_count = len(raw.get("transactions", []))
    logger.info("Fetched %d transactions", txn_count)

    bronze_dir = write_bronze(raw, end_date)
    logger.info("Bronze written to %s", bronze_dir)

    df = flatten_transactions(raw)
    logger.info("Flattened %d transaction rows", len(df))

    write_silver(df)
    logger.info("Done: %d transactions ingested", len(df))


if __name__ == "__main__":
    transactions.serve(
        name="transactions-daily",
        schedule=CronSchedule(cron="45 6 * * *", timezone="America/Los_Angeles"),
    )
