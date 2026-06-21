"""
Teams SCD flow -- pull MLB teams, maintain Type 2 Slowly Changing Dimensions.

Bronze: raw JSON from MLB Stats API, partitioned by date.
Silver: teams_current.parquet (latest state) + teams_history.parquet (full SCD2 history).
"""

from datetime import date
from pathlib import Path

import json
import httpx
import polars as pl
from prefect import flow, get_run_logger, task
from prefect.client.schemas.schedules import CronSchedule

BRONZE_ROOT = Path("/data/bronze/teams")
SILVER_ROOT = Path("/data/silver/teams")
MLB_BASE = "https://statsapi.mlb.com/api/v1"
CURRENT_FILE = SILVER_ROOT / "teams_current.parquet"
HISTORY_FILE = SILVER_ROOT / "teams_history.parquet"


@task(retries=2, retry_delay_seconds=10)
async def fetch_teams(client: httpx.AsyncClient) -> dict:
    """Fetches the list of active MLB teams from the Stats API. Returns the raw
    JSON response."""
    resp = await client.get(f"{MLB_BASE}/teams", params={"sportId": 1})
    resp.raise_for_status()
    return resp.json()


@task
def write_bronze(teams_data: dict, run_date: date) -> Path:
    """Writes raw teams JSON to bronze layer, partitioned by date.

    Args:
        teams_data: Output from fetch_teams.
        run_date: ISO date of the invoking flow, defaults to today when None.

    Returns:
        A directory, indexed by run_date, where the data was written.
        For example:

            /data/bronze/teams/2026-06-20
    """
    day_dir = BRONZE_ROOT / run_date.isoformat()
    day_dir.mkdir(parents=True, exist_ok=True)

    (day_dir / "teams.json").write_text(json.dumps(teams_data, indent=2))

    return day_dir


@task
def flatten_teams(teams_data: dict) -> pl.DataFrame:
    """Flattens nested team data into a flat DataFrame for SCD2 comparison."""
    rows = []
    for t in teams_data.get("teams", []):
        rows.append(
            {
                "team_id": t["id"],
                "team_name": t["name"],
                "abbreviation": t["abbreviation"],
                "division_id": t.get("division", {}).get("id"),
                "division_name": t.get("division", {}).get("name"),
                "league_id": t.get("league", {}).get("id"),
                "league_name": t.get("league", {}).get("name"),
                "venue_id": t.get("venue", {}).get("id"),
                "venue_name": t.get("venue", {}).get("name"),
            }
        )

    schema = {
        "team_id": pl.Int64,
        "team_name": pl.Utf8,
        "abbreviation": pl.Utf8,
        "division_id": pl.Int64,
        "division_name": pl.Utf8,
        "league_id": pl.Int64,
        "league_name": pl.Utf8,
        "venue_id": pl.Int64,
        "venue_name": pl.Utf8,
    }

    if not rows:
        return pl.DataFrame(schema=schema)

    return pl.DataFrame(rows, schema=schema)


TRACK_COLS = [
    "team_name",
    "abbreviation",
    "division_id",
    "division_name",
    "league_id",
    "league_name",
    "venue_id",
    "venue_name",
]


@task
def apply_scd2(today_df: pl.DataFrame, run_date: date) -> pl.DataFrame:
    """Applies SCD2 to team data, logging only changes.
    Read more: https://en.wikipedia.org/wiki/Slowly_changing_dimension

    Args:
        today_df: output of flatten_teams.
        run_date: ISO date of the invoking flow, defaults to today when None.
    Returns:
        A dataframe containing a complete list of team_ids with effective
        dates and assignments as of run_date.
    """
    logger = get_run_logger()

    today_keyed = today_df.with_columns(
        pl.lit(run_date).alias("effective_from"),
        pl.lit(None).cast(pl.Date).alias("effective_to"),
        pl.lit(True).alias("is_current"),
    )

    if not CURRENT_FILE.exists():
        logger.info("No previous state -- all %d teams are new", len(today_keyed))
        return today_keyed

    previous = pl.read_parquet(CURRENT_FILE)

    prev_current = previous.filter(pl.col("is_current"))
    prev_ids = set(prev_current["team_id"].to_list())
    today_ids = set(today_df["team_id"].to_list())

    new_ids = today_ids - prev_ids
    removed_ids = prev_ids - today_ids
    continuing_ids = prev_ids & today_ids

    logger.info(
        "Delta: %d new, %d removed, %d continuing",
        len(new_ids),
        len(removed_ids),
        len(continuing_ids),
    )

    # Find changed teams among continuing ones
    if continuing_ids:
        prev_compare = (
            prev_current.filter(pl.col("team_id").is_in(list(continuing_ids)))
            .select(["team_id"] + TRACK_COLS)
            .sort("team_id")
        )
        today_compare = (
            today_df.filter(pl.col("team_id").is_in(list(continuing_ids)))
            .select(["team_id"] + TRACK_COLS)
            .sort("team_id")
        )
        merged = prev_compare.join(today_compare, on="team_id", suffix="_new")
        changed_mask = pl.lit(False)
        for col in TRACK_COLS:
            changed_mask = changed_mask | (
                pl.col(col).cast(pl.Utf8).ne_missing(pl.col(f"{col}_new").cast(pl.Utf8))
            )
        changed_ids = set(
            merged.filter(changed_mask)["team_id"].to_list()
        )
    else:
        changed_ids = set()

    logger.info("Changed: %d teams with attribute updates", len(changed_ids))

    close_ids = removed_ids | changed_ids

    # Records that stay open unchanged
    unchanged = prev_current.filter(~pl.col("team_id").is_in(list(close_ids | new_ids)))

    # Close out old records
    closed = prev_current.filter(pl.col("team_id").is_in(list(close_ids))).with_columns(
        pl.lit(run_date).alias("effective_to"),
        pl.lit(False).alias("is_current"),
    )

    # New records for new + changed teams
    insert_ids = new_ids | changed_ids
    new_records = today_keyed.filter(pl.col("team_id").is_in(list(insert_ids)))

    result = pl.concat([unchanged, closed, new_records], how="diagonal_relaxed")
    return result


@task
def write_silver(scd_df: pl.DataFrame) -> None:
    """Writes SCD2 results to silver layer. Idempotent -- re-running for the same
    date replaces previous output."""
    logger = get_run_logger()
    SILVER_ROOT.mkdir(parents=True, exist_ok=True)

    scd_df.write_parquet(CURRENT_FILE)
    logger.info("Wrote %d records to %s", len(scd_df), CURRENT_FILE)

    if HISTORY_FILE.exists():
        existing = pl.read_parquet(HISTORY_FILE)
        # Remove any rows from today's run (idempotency on re-run):
        # drop rows whose effective_from matches today and re-append the
        # fresh SCD output (rows with effective_to == today from prior
        # runs are legitimately closed and should be preserved)
        today = scd_df["effective_from"].max()
        if today is not None:
            existing = existing.filter(pl.col("effective_from") != today)
        combined = pl.concat([existing, scd_df], how="diagonal_relaxed")
    else:
        combined = scd_df

    combined.write_parquet(HISTORY_FILE)
    logger.info("Wrote %d total records to %s", len(combined), HISTORY_FILE)


@flow(
    name="teams-scd",
    description="Pull MLB teams, maintain Type 2 SCD in silver layer.",
)
async def teams_scd(run_date: date | None = None):
    """Pull MLB teams and update SCD2 team table in silver layer."""
    logger = get_run_logger()
    run_date = run_date or date.today()
    logger.info("Teams SCD flow for %s", run_date)

    async with httpx.AsyncClient(timeout=30.0) as client:
        teams_data = await fetch_teams(client)

    team_count = len(teams_data.get("teams", []))
    logger.info("Found %d teams", team_count)

    bronze_dir = write_bronze(teams_data, run_date)
    logger.info("Bronze written to %s", bronze_dir)

    today_df = flatten_teams(teams_data)
    logger.info("Flattened %d team entries", len(today_df))

    scd_df = apply_scd2(today_df, run_date)
    write_silver(scd_df)

    current_count = scd_df.filter(pl.col("is_current")).shape[0]
    historical_count = scd_df.filter(~pl.col("is_current")).shape[0]
    logger.info(
        "Done: %d current records, %d historical records", current_count, historical_count
    )


if __name__ == "__main__":
    teams_scd.serve(
        name="teams-scd-daily",
        schedule=CronSchedule(cron="30 6 * * *", timezone="America/Los_Angeles"),
    )
