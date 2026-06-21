"""
Draft picks flow -- pull MLB draft data, maintain append-only event table.

Bronze: raw JSON from MLB Stats API, partitioned by year.
Silver: draft_picks.parquet (append-only, deduplicated by round + pick_number + year).
"""

from datetime import date
from pathlib import Path

import hashlib
import json
import httpx
import polars as pl
from prefect import flow, get_run_logger, task
from prefect.client.schemas.schedules import CronSchedule

BRONZE_ROOT = Path("/data/bronze/draft")
SILVER_ROOT = Path("/data/silver/draft")
SILVER_FILE = SILVER_ROOT / "draft_picks.parquet"
MLB_BASE = "https://statsapi.mlb.com/api/v1"

# Default to last 3 draft years on first run; subsequent runs focus on current year
DEFAULT_BACKFILL_YEARS = 3


@task(retries=2, retry_delay_seconds=10)
async def fetch_draft(client: httpx.AsyncClient, year: int) -> dict:
    """Fetch draft data for a given year from the MLB Stats API."""
    resp = await client.get(f"{MLB_BASE}/draft/{year}")
    resp.raise_for_status()
    return resp.json()


@task
def check_bronze_changed(raw: dict, year: int) -> bool:
    """Compare incoming JSON hash against the last bronze write.

    Returns True if the data is new or changed, False if identical.
    """
    logger = get_run_logger()
    year_dir = BRONZE_ROOT / str(year)
    hash_file = year_dir / ".content_hash"

    new_hash = hashlib.sha256(
        json.dumps(raw, sort_keys=True).encode()
    ).hexdigest()

    if hash_file.exists():
        old_hash = hash_file.read_text().strip()
        if old_hash == new_hash:
            logger.info("Year %d: data unchanged (hash %s…), skipping", year, new_hash[:12])
            return False

    logger.info("Year %d: new or changed data (hash %s…)", year, new_hash[:12])
    return True


@task
def write_bronze(raw: dict, year: int) -> Path:
    """Write raw draft JSON to bronze layer, keyed by year.

    Returns the directory where data was written.
    """
    year_dir = BRONZE_ROOT / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)

    bronze_file = year_dir / "draft.json"
    bronze_file.write_text(json.dumps(raw, indent=2))

    content_hash = hashlib.sha256(
        json.dumps(raw, sort_keys=True).encode()
    ).hexdigest()
    (year_dir / ".content_hash").write_text(content_hash)

    return year_dir


@task
def flatten_picks(raw: dict, year: int) -> pl.DataFrame:
    """Flatten draft JSON into a picks dataframe.

    Only includes picks where isDrafted is True (skips unfilled slots).
    """
    logger = get_run_logger()
    drafts = raw.get("drafts", {})
    rounds = drafts.get("rounds", [])

    rows = []
    for round_data in rounds:
        round_num = round_data.get("round", "")
        for pick in round_data.get("picks", []):
            if not pick.get("isDrafted", False):
                continue

            person = pick.get("person", {})
            team = pick.get("team", {})
            school = pick.get("school", {})
            position = person.get("primaryPosition", {})

            rows.append(
                {
                    "year": int(pick.get("year", year)),
                    "round": round_num,
                    "pick_number": pick.get("pickNumber"),
                    "player_id": person.get("id"),
                    "player_name": person.get("fullName"),
                    "team_id": team.get("id"),
                    "team_name": team.get("name"),
                    "school": school.get("name"),
                    "position": position.get("abbreviation"),
                    "signing_bonus": pick.get("signingBonus"),
                }
            )

    schema = {
        "year": pl.Int32,
        "round": pl.Utf8,
        "pick_number": pl.Int32,
        "player_id": pl.Int64,
        "player_name": pl.Utf8,
        "team_id": pl.Int32,
        "team_name": pl.Utf8,
        "school": pl.Utf8,
        "position": pl.Utf8,
        "signing_bonus": pl.Utf8,
    }

    if not rows:
        logger.info("Year %d: no drafted picks found", year)
        return pl.DataFrame(schema=schema)

    df = pl.DataFrame(rows, schema=schema)
    logger.info("Year %d: flattened %d picks", year, len(df))
    return df


@task
def write_silver(new_picks: pl.DataFrame) -> None:
    """Append new picks to the silver parquet, deduplicating by (year, round, pick_number).

    Append-only: existing rows are never modified. On re-run with the same data,
    the dedup key prevents duplicates.
    """
    logger = get_run_logger()

    if new_picks.is_empty():
        logger.info("No new picks to write")
        return

    SILVER_ROOT.mkdir(parents=True, exist_ok=True)
    dedup_cols = ["year", "round", "pick_number"]

    if SILVER_FILE.exists():
        existing = pl.read_parquet(SILVER_FILE)
        combined = pl.concat([existing, new_picks], how="diagonal_relaxed")
        # Keep last occurrence per dedup key so re-runs can update signing_bonus etc.
        result = combined.unique(subset=dedup_cols, keep="last").sort(
            ["year", "round", "pick_number"]
        )
        new_count = len(result) - len(existing)
        logger.info(
            "Silver: %d existing + %d net new = %d total",
            len(existing),
            new_count,
            len(result),
        )
    else:
        result = new_picks.unique(subset=dedup_cols, keep="last").sort(
            ["year", "round", "pick_number"]
        )
        logger.info("Silver: initial write with %d picks", len(result))

    result.write_parquet(SILVER_FILE)
    logger.info("Wrote %s", SILVER_FILE)


@flow(
    name="draft-picks",
    description="Pull MLB draft picks, maintain append-only event table in silver layer.",
)
async def draft_picks(
    years: list[int] | None = None,
    force: bool = False,
):
    """Pull MLB draft picks and append to silver layer.

    Args:
        years: Draft years to fetch. Defaults to current year (+ backfill on first run).
        force: If True, skip the content-hash no-op check and always write.
    """
    logger = get_run_logger()
    current_year = date.today().year

    if years is None:
        if SILVER_FILE.exists():
            years = [current_year]
        else:
            years = list(range(current_year - DEFAULT_BACKFILL_YEARS + 1, current_year + 1))

    logger.info("Draft picks flow for years: %s", years)

    all_picks: list[pl.DataFrame] = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        for year in years:
            raw = await fetch_draft(client, year)

            if not force and not check_bronze_changed(raw, year):
                continue

            bronze_dir = write_bronze(raw, year)
            logger.info("Bronze written to %s", bronze_dir)

            picks_df = flatten_picks(raw, year)
            if not picks_df.is_empty():
                all_picks.append(picks_df)

    if all_picks:
        combined = pl.concat(all_picks, how="diagonal_relaxed")
        logger.info("Total picks to append: %d", len(combined))
        write_silver(combined)
    else:
        logger.info("No new data to write -- all years unchanged or empty")


if __name__ == "__main__":
    draft_picks.serve(
        name="draft-picks-daily",
        schedule=CronSchedule(cron="0 8 * * *", timezone="America/Los_Angeles"),
    )
