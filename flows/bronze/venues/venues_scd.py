"""
Venues SCD flow -- pull MLB venue data, maintain Type 2 Slowly Changing Dimensions.

Bronze: raw JSON from MLB Stats API (teams endpoint with venue hydration), partitioned by date.
Silver: venues_current.parquet (latest state) + venues_history.parquet (full SCD2 history).

Venue data comes from the teams endpoint hydrated with venue(location,fieldInfo) since the
dedicated /venues endpoint lacks location and field details.
"""

from datetime import date
from pathlib import Path

import json
import httpx
import polars as pl
from prefect import flow, get_run_logger, task
from prefect.client.schemas.schedules import CronSchedule

BRONZE_ROOT = Path("/data/bronze/venues")
SILVER_ROOT = Path("/data/silver/venues")
MLB_BASE = "https://statsapi.mlb.com/api/v1"
CURRENT_FILE = SILVER_ROOT / "venues_current.parquet"
HISTORY_FILE = SILVER_ROOT / "venues_history.parquet"


@task(retries=2, retry_delay_seconds=10)
async def fetch_venues(client: httpx.AsyncClient) -> dict:
    """Fetches MLB teams with hydrated venue data (location + fieldInfo).
    Returns the raw JSON response."""
    resp = await client.get(
        f"{MLB_BASE}/teams",
        params={"sportId": 1, "hydrate": "venue(location,fieldInfo)"},
    )
    resp.raise_for_status()
    return resp.json()


@task
def write_bronze(teams_data: dict, run_date: date) -> Path:
    """Writes raw API response to bronze layer, partitioned by date.

    Args:
        teams_data: Output from fetch_venues (full teams response with hydrated venues).
        run_date: ISO date of the invoking flow.

    Returns:
        The directory where the data was written, e.g. /data/bronze/venues/2026-06-20
    """
    day_dir = BRONZE_ROOT / run_date.isoformat()
    day_dir.mkdir(parents=True, exist_ok=True)

    (day_dir / "venues.json").write_text(json.dumps(teams_data, indent=2))

    return day_dir


@task
def flatten_venues(teams_data: dict) -> pl.DataFrame:
    """Extracts and deduplicates venue records from teams data.

    Multiple teams can share a venue (historically), so we deduplicate on venue_id.
    """
    rows = []
    seen = set()
    for team in teams_data.get("teams", []):
        venue = team.get("venue", {})
        vid = venue.get("id")
        if vid is None or vid in seen:
            continue
        seen.add(vid)

        location = venue.get("location", {})
        field_info = venue.get("fieldInfo", {})

        rows.append(
            {
                "venue_id": vid,
                "venue_name": venue.get("name"),
                "city": location.get("city"),
                "state": location.get("stateAbbrev"),
                "capacity": field_info.get("capacity"),
                "surface_type": field_info.get("turfType"),
                "roof_type": field_info.get("roofType"),
            }
        )

    schema = {
        "venue_id": pl.Int64,
        "venue_name": pl.Utf8,
        "city": pl.Utf8,
        "state": pl.Utf8,
        "capacity": pl.Int64,
        "surface_type": pl.Utf8,
        "roof_type": pl.Utf8,
    }

    if not rows:
        return pl.DataFrame(schema=schema)

    return pl.DataFrame(rows, schema=schema)


TRACK_COLS = ["venue_name", "city", "state", "capacity", "surface_type", "roof_type"]


@task
def apply_scd2(today_df: pl.DataFrame, run_date: date) -> pl.DataFrame:
    """Applies SCD2 to venue data, logging only changes.

    Args:
        today_df: Output of flatten_venues.
        run_date: ISO date of the invoking flow.

    Returns:
        A dataframe containing venue records with effective dates and SCD2 flags.
    """
    logger = get_run_logger()

    today_keyed = today_df.with_columns(
        pl.lit(run_date).alias("effective_from"),
        pl.lit(None).cast(pl.Date).alias("effective_to"),
        pl.lit(True).alias("is_current"),
    )

    if not CURRENT_FILE.exists():
        logger.info("No previous state -- all %d venues are new", len(today_keyed))
        return today_keyed

    previous = pl.read_parquet(CURRENT_FILE)

    prev_current = previous.filter(pl.col("is_current"))
    prev_ids = set(prev_current["venue_id"].to_list())
    today_ids = set(today_df["venue_id"].to_list())

    new_ids = today_ids - prev_ids
    removed_ids = prev_ids - today_ids
    continuing_ids = prev_ids & today_ids

    logger.info(
        "Delta: %d new, %d removed, %d continuing",
        len(new_ids),
        len(removed_ids),
        len(continuing_ids),
    )

    # Find changed venues among continuing ones
    if continuing_ids:
        prev_compare = (
            prev_current.filter(pl.col("venue_id").is_in(list(continuing_ids)))
            .select(["venue_id"] + TRACK_COLS)
            .sort("venue_id")
        )
        today_compare = (
            today_df.filter(pl.col("venue_id").is_in(list(continuing_ids)))
            .select(["venue_id"] + TRACK_COLS)
            .sort("venue_id")
        )
        merged = prev_compare.join(today_compare, on="venue_id", suffix="_new")
        changed_mask = pl.lit(False)
        for col in TRACK_COLS:
            changed_mask = changed_mask | (
                pl.col(col).cast(pl.Utf8).fill_null("__NULL__")
                != pl.col(f"{col}_new").cast(pl.Utf8).fill_null("__NULL__")
            )
        changed_ids = set(
            merged.filter(changed_mask)["venue_id"].to_list()
        )
    else:
        changed_ids = set()

    logger.info("Changed: %d venues with attribute updates", len(changed_ids))

    close_ids = removed_ids | changed_ids

    # Records that stay open unchanged
    unchanged = prev_current.filter(
        ~pl.col("venue_id").is_in(list(close_ids | new_ids))
    )

    # Close out old records
    closed = prev_current.filter(
        pl.col("venue_id").is_in(list(close_ids))
    ).with_columns(
        pl.lit(run_date).alias("effective_to"),
        pl.lit(False).alias("is_current"),
    )

    # New records for new + changed venues
    insert_ids = new_ids | changed_ids
    new_records = today_keyed.filter(pl.col("venue_id").is_in(list(insert_ids)))

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
        # Remove any rows from today's run (idempotency on re-run)
        today = scd_df["effective_from"].max()
        if today is not None:
            existing = existing.filter(
                (pl.col("effective_from") != today)
                & (pl.col("effective_to") != today)
            )
        combined = pl.concat([existing, scd_df], how="diagonal_relaxed")
    else:
        combined = scd_df

    combined.write_parquet(HISTORY_FILE)
    logger.info("Wrote %d total records to %s", len(combined), HISTORY_FILE)


@flow(
    name="venues-scd",
    description="Pull MLB venue data, maintain Type 2 SCD in silver layer.",
)
async def venues_scd(run_date: date | None = None):
    """Pull MLB venue info and update SCD2 venue table in silver layer."""
    logger = get_run_logger()
    run_date = run_date or date.today()
    logger.info("Venues SCD flow for %s", run_date)

    async with httpx.AsyncClient(timeout=30.0) as client:
        teams_data = await fetch_venues(client)

    teams = teams_data.get("teams", [])
    venues_seen = {t["venue"]["id"] for t in teams if "venue" in t}
    logger.info("Found %d unique venues across %d teams", len(venues_seen), len(teams))

    bronze_dir = write_bronze(teams_data, run_date)
    logger.info("Bronze written to %s", bronze_dir)

    today_df = flatten_venues(teams_data)
    logger.info("Flattened %d venue records", len(today_df))

    scd_df = apply_scd2(today_df, run_date)
    write_silver(scd_df)

    current_count = scd_df.filter(pl.col("is_current")).shape[0]
    historical_count = scd_df.filter(~pl.col("is_current")).shape[0]
    logger.info(
        "Done: %d current records, %d historical records",
        current_count,
        historical_count,
    )


if __name__ == "__main__":
    venues_scd.serve(
        name="venues-scd-weekly",
        schedule=CronSchedule(cron="0 6 * * 6", timezone="America/Los_Angeles"),
    )
