"""
Roster SCD flow -- pull MLB active rosters, maintain Type 2 Slowly Changing Dimensions.

Bronze: raw JSON from MLB Stats API, partitioned by date.
Silver: roster_current.parquet (latest state) + roster_history.parquet (full SCD2 history).
"""

from datetime import date
from pathlib import Path

import json
import httpx
import polars as pl
from prefect import flow, get_run_logger, task
from prefect.client.schemas.schedules import CronSchedule

BRONZE_ROOT = Path("/data/bronze/rosters")
SILVER_ROOT = Path("/data/silver/rosters")
MLB_BASE = "https://statsapi.mlb.com/api/v1"
CURRENT_FILE = SILVER_ROOT / "roster_current.parquet"
HISTORY_FILE = SILVER_ROOT / "roster_history.parquet"


@task(retries=2, retry_delay_seconds=10)
async def fetch_teams(client: httpx.AsyncClient) -> dict:
    """Fetches the list of active MLB teams from the Stats API. Returns the raw 
    JSON response."""
    resp = await client.get(f"{MLB_BASE}/teams", params={"sportId": 1})
    resp.raise_for_status()
    return resp.json()


@task(retries=1, retry_delay_seconds=5)
async def fetch_roster(client: httpx.AsyncClient, team_id: int) -> dict | None:
    """Fetches roster from MLB API
    
    Args:
        client: an httpx async client.
        team_id: MLB API team id (from fetch_teams).

    Returns:
        Roster json from endpoint.
        Returns None and logs warning on HTTPStatusError or RequestError.
    """
    logger = get_run_logger()
    try:
        resp = await client.get(
            f"{MLB_BASE}/teams/{team_id}/roster",
            params={"rosterType": "fullRoster"},
        )
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as exc:
        logger.warning("Failed to fetch roster for team %d: %s", team_id, exc)
        return None
    except httpx.RequestError as exc:
        logger.warning("Request error for team %d: %s", team_id, exc)
        return None


@task
def write_bronze(teams_data: dict, rosters: dict[int, dict], run_date: date) -> Path:
    """Writes teams list and rosters for each team in raw json.

    This is a bronze-tier write operation.

    Args:
        teams_data: Output from fetch_teams.
        rosters: Output from fetch_roster, aggregated as dict of dicts with
          format {teamid (int): roster (dict)}.
        run_date: ISO date of the invoking flow, defaults to today when None.

    Returns:
        A directory, indexed by run_date, where the data was written.
        For example:

            /data/bronze/rosters/2026-05-01
    """
    day_dir = BRONZE_ROOT / run_date.isoformat()
    day_dir.mkdir(parents=True, exist_ok=True)

    (day_dir / "teams.json").write_text(json.dumps(teams_data, indent=2))
    for team_id, roster_data in rosters.items():
        (day_dir / f"{team_id}.json").write_text(json.dumps(roster_data, indent=2))

    return day_dir


@task
def flatten_rosters(
    teams_data: dict, rosters: dict[int, dict], run_date: date
) -> pl.DataFrame:
    """Flattens roster data and returns dataframe for SCD2 comparison."""
    team_lookup = {t["id"]: t["name"] for t in teams_data.get("teams", [])}
    rows = []
    for team_id, roster_data in rosters.items():
        if roster_data is None:
            continue
        for entry in roster_data.get("roster", []):
            rows.append(
                {
                    "player_id": entry["person"]["id"],
                    "player_name": entry["person"]["fullName"],
                    "team_id": team_id,
                    "team_name": team_lookup.get(team_id, f"Unknown ({team_id})"),
                    "position": entry["position"]["abbreviation"],
                    "jersey_number": entry.get("jerseyNumber"),
                    "status": entry["status"]["description"],
                }
            )

    schema = {
        "player_id": pl.Int64,
        "player_name": pl.Utf8,
        "team_id": pl.Int64,
        "team_name": pl.Utf8,
        "position": pl.Utf8,
        "jersey_number": pl.Utf8,
        "status": pl.Utf8,
    }

    if not rows:
        return pl.DataFrame(schema=schema)

    return pl.DataFrame(rows, schema=schema)


TRACK_COLS = ["team_id", "team_name", "position", "jersey_number", "status"]


@task
def apply_scd2(today_df: pl.DataFrame, run_date: date) -> pl.DataFrame:
    """Applies SCD2 to compact roster data, logging only changes.
    Read more: https://en.wikipedia.org/wiki/Slowly_changing_dimension

    Args:
        today_df: output of flatten_rosters.
        run_date: ISO date of the invoking flow, defaults to today when None.
    Returns:
        A dataframe containing a complete list of player_ids with effective
        dates and assignments as of run_date.
    """
    logger = get_run_logger()

    today_keyed = today_df.with_columns(
        pl.lit(run_date).alias("effective_from"),
        pl.lit(None).cast(pl.Date).alias("effective_to"),
        pl.lit(True).alias("is_current"),
    )

    if not CURRENT_FILE.exists():
        logger.info("No previous state -- all %d players are new", len(today_keyed))
        return today_keyed

    previous = pl.read_parquet(CURRENT_FILE)

    prev_current = previous.filter(pl.col("is_current"))
    prev_ids = set(prev_current["player_id"].to_list())
    today_ids = set(today_df["player_id"].to_list())

    new_ids = today_ids - prev_ids
    removed_ids = prev_ids - today_ids
    continuing_ids = prev_ids & today_ids

    logger.info(
        "Delta: %d new, %d removed, %d continuing",
        len(new_ids),
        len(removed_ids),
        len(continuing_ids),
    )

    # Find changed players among continuing ones
    if continuing_ids:
        prev_compare = (
            prev_current.filter(pl.col("player_id").is_in(list(continuing_ids)))
            .select(["player_id"] + TRACK_COLS)
            .sort("player_id")
        )
        today_compare = (
            today_df.filter(pl.col("player_id").is_in(list(continuing_ids)))
            .select(["player_id"] + TRACK_COLS)
            .sort("player_id")
        )
        merged = prev_compare.join(today_compare, on="player_id", suffix="_new")
        changed_mask = pl.lit(False)
        for col in TRACK_COLS:
            changed_mask = changed_mask | (
                pl.col(col).cast(pl.Utf8).fill_null("__NULL__")
                != pl.col(f"{col}_new").cast(pl.Utf8).fill_null("__NULL__")
            )
        changed_ids = set(
            merged.filter(changed_mask)["player_id"].to_list()
        )
    else:
        changed_ids = set()

    logger.info("Changed: %d players with attribute updates", len(changed_ids))

    close_ids = removed_ids | changed_ids

    # Records that stay open unchanged
    unchanged = prev_current.filter(~pl.col("player_id").is_in(list(close_ids | new_ids)))

    # Close out old records
    closed = prev_current.filter(pl.col("player_id").is_in(list(close_ids))).with_columns(
        pl.lit(run_date).alias("effective_to"),
        pl.lit(False).alias("is_current"),
    )

    # New records for new + changed players
    insert_ids = new_ids | changed_ids
    new_records = today_keyed.filter(pl.col("player_id").is_in(list(insert_ids)))

    result = pl.concat([unchanged, closed, new_records], how="diagonal_relaxed")
    return result


@task
def write_silver(scd_df: pl.DataFrame) -> None:
    """Writes SCD2 results to silver layer. Idempotent — re-running for the same 
    date replaces previous output."""
    logger = get_run_logger()
    SILVER_ROOT.mkdir(parents=True, exist_ok=True)

    scd_df.write_parquet(CURRENT_FILE)
    logger.info("Wrote %d records to %s", len(scd_df), CURRENT_FILE)

    if HISTORY_FILE.exists():
        existing = pl.read_parquet(HISTORY_FILE)
        # Remove any rows from today's run (idempotency on re-run):
        # drop all rows whose effective_from or effective_to matches today
        # and re-append the fresh SCD output
        today = scd_df["effective_from"].max()
        if today is not None:
            existing = existing.filter(
                (pl.col("effective_from") != today)
                & (pl.col("effective_to").is_null() | (pl.col("effective_to") != today))
            )
        combined = pl.concat([existing, scd_df], how="diagonal_relaxed")
    else:
        combined = scd_df

    combined.write_parquet(HISTORY_FILE)
    logger.info("Wrote %d total records to %s", len(combined), HISTORY_FILE)


@flow(
    name="roster-scd",
    description="Pull MLB active rosters, maintain Type 2 SCD in silver layer.",
)
async def roster_scd(run_date: date | None = None):
    """Pull MLB rosters and update SCD2 player table in silver layer."""
    logger = get_run_logger()
    run_date = run_date or date.today()
    logger.info("Roster SCD flow for %s", run_date)

    async with httpx.AsyncClient(timeout=30.0) as client:
        teams_data = await fetch_teams(client)
        team_ids = [t["id"] for t in teams_data.get("teams", [])]
        logger.info("Found %d teams", len(team_ids))

        rosters: dict[int, dict] = {}
        for team_id in team_ids:
            result = await fetch_roster(client, team_id)
            if result is not None:
                rosters[team_id] = result

    logger.info("Fetched rosters for %d / %d teams", len(rosters), len(team_ids))

    bronze_dir = write_bronze(teams_data, rosters, run_date)
    logger.info("Bronze written to %s", bronze_dir)

    today_df = flatten_rosters(teams_data, rosters, run_date)
    logger.info("Flattened %d roster entries", len(today_df))

    scd_df = apply_scd2(today_df, run_date)
    write_silver(scd_df)

    current_count = scd_df.filter(pl.col("is_current")).shape[0]
    historical_count = scd_df.filter(~pl.col("is_current")).shape[0]
    logger.info(
        "Done: %d current records, %d historical records", current_count, historical_count
    )


if __name__ == "__main__":
    roster_scd.serve(
        name="roster-scd-daily",
        schedule=CronSchedule(cron="0 6 * * *", timezone="America/Los_Angeles"),
    )
