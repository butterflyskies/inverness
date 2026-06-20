"""
Schedule flow -- pull MLB game schedule, maintain Type 1 overwrite table.

Bronze: raw JSON from MLB Stats API schedule endpoint, partitioned by date.
Silver: schedule.parquet (Type 1 overwrite per game_pk, with rescheduled flag).
"""

from datetime import date
from pathlib import Path

import json
import httpx
import polars as pl
from prefect import flow, get_run_logger, task
from prefect.client.schemas.schedules import CronSchedule

BRONZE_ROOT = Path("/data/bronze/schedule")
SILVER_ROOT = Path("/data/silver/schedule")
MLB_BASE = "https://statsapi.mlb.com/api/v1"
SCHEDULE_FILE = SILVER_ROOT / "schedule.parquet"


@task(retries=2, retry_delay_seconds=10)
async def fetch_schedule(
    client: httpx.AsyncClient, run_date: date
) -> dict:
    """Fetches the MLB schedule for a single date from the Stats API."""
    resp = await client.get(
        f"{MLB_BASE}/schedule",
        params={"sportId": 1, "date": run_date.isoformat()},
    )
    resp.raise_for_status()
    return resp.json()


@task
def write_bronze(schedule_data: dict, run_date: date) -> Path:
    """Writes raw schedule JSON to bronze layer, partitioned by date.

    Args:
        schedule_data: Raw JSON response from the schedule endpoint.
        run_date: ISO date of the invoking flow.

    Returns:
        The directory where the data was written, e.g.
        /data/bronze/schedule/2026-06-19
    """
    day_dir = BRONZE_ROOT / run_date.isoformat()
    day_dir.mkdir(parents=True, exist_ok=True)
    (day_dir / "schedule.json").write_text(json.dumps(schedule_data, indent=2))
    return day_dir


@task
def flatten_schedule(schedule_data: dict) -> pl.DataFrame:
    """Flattens the nested schedule JSON into a tabular DataFrame.

    Extracts one row per game with the columns required for the silver table.
    The rescheduled flag is set to False here; apply_type1 handles detection.
    """
    rows = []
    for date_entry in schedule_data.get("dates", []):
        for game in date_entry.get("games", []):
            rows.append(
                {
                    "game_pk": game["gamePk"],
                    "game_date": game["gameDate"],
                    "game_type": game["gameType"],
                    "status": game["status"]["detailedState"],
                    "home_team_id": game["teams"]["home"]["team"]["id"],
                    "home_team_name": game["teams"]["home"]["team"]["name"],
                    "away_team_id": game["teams"]["away"]["team"]["id"],
                    "away_team_name": game["teams"]["away"]["team"]["name"],
                    "home_score": game["teams"]["home"].get("score"),
                    "away_score": game["teams"]["away"].get("score"),
                    "venue_id": game["venue"]["id"],
                    "venue_name": game["venue"]["name"],
                    "rescheduled": False,
                }
            )

    schema = {
        "game_pk": pl.Int64,
        "game_date": pl.Utf8,
        "game_type": pl.Utf8,
        "status": pl.Utf8,
        "home_team_id": pl.Int64,
        "home_team_name": pl.Utf8,
        "away_team_id": pl.Int64,
        "away_team_name": pl.Utf8,
        "home_score": pl.Int64,
        "away_score": pl.Int64,
        "venue_id": pl.Int64,
        "venue_name": pl.Utf8,
        "rescheduled": pl.Boolean,
    }

    if not rows:
        return pl.DataFrame(schema=schema)

    return pl.DataFrame(rows, schema=schema)


@task
def apply_type1(today_df: pl.DataFrame) -> pl.DataFrame:
    """Applies Type 1 (overwrite) merge into the silver schedule table.

    For each game_pk in today_df:
    - If the game_pk exists in the previous table and gameDate has changed,
      set rescheduled = True on the new row.
    - Otherwise, overwrite the row as-is.
    - Rows not in today_df are preserved unchanged.

    Args:
        today_df: Output of flatten_schedule for today's pull.

    Returns:
        The complete merged schedule DataFrame.
    """
    logger = get_run_logger()

    if not SCHEDULE_FILE.exists():
        logger.info(
            "No previous schedule -- inserting %d games as new", len(today_df)
        )
        return today_df

    previous = pl.read_parquet(SCHEDULE_FILE)
    incoming_pks = set(today_df["game_pk"].to_list())

    # Find games whose game_date changed (rescheduled detection)
    prev_dates = (
        previous.filter(pl.col("game_pk").is_in(list(incoming_pks)))
        .select("game_pk", pl.col("game_date").alias("prev_game_date"))
    )

    if len(prev_dates) > 0:
        merged = today_df.join(prev_dates, on="game_pk", how="left")
        rescheduled_mask = (
            merged["prev_game_date"].is_not_null()
            & (merged["game_date"] != merged["prev_game_date"])
        )
        rescheduled_count = rescheduled_mask.sum()
        if rescheduled_count > 0:
            logger.info("Detected %d rescheduled game(s)", rescheduled_count)

        # Also carry forward rescheduled=True from previous rows if the date
        # hasn't changed back -- once rescheduled, stays rescheduled.
        prev_rescheduled = (
            previous.filter(
                pl.col("game_pk").is_in(list(incoming_pks))
                & pl.col("rescheduled")
            )
            .select("game_pk")
        )
        prev_rescheduled_pks = set(prev_rescheduled["game_pk"].to_list())

        today_df = (
            merged.with_columns(
                (
                    rescheduled_mask
                    | pl.col("game_pk").is_in(list(prev_rescheduled_pks))
                ).alias("rescheduled")
            )
            .drop("prev_game_date")
        )
    else:
        rescheduled_count = 0

    # Rows in previous that are NOT in today's pull -- preserve them
    retained = previous.filter(~pl.col("game_pk").is_in(list(incoming_pks)))

    new_count = len(incoming_pks - set(previous["game_pk"].to_list()))
    updated_count = len(incoming_pks) - new_count

    logger.info(
        "Type 1 merge: %d new, %d updated (%d rescheduled), %d retained",
        new_count,
        updated_count,
        rescheduled_count,
        len(retained),
    )

    return pl.concat([retained, today_df], how="diagonal_relaxed")


@task
def write_silver(schedule_df: pl.DataFrame) -> None:
    """Writes the merged schedule to the silver parquet file."""
    logger = get_run_logger()
    SILVER_ROOT.mkdir(parents=True, exist_ok=True)
    schedule_df.write_parquet(SCHEDULE_FILE)
    logger.info("Wrote %d games to %s", len(schedule_df), SCHEDULE_FILE)


@flow(
    name="schedule",
    description="Pull MLB game schedule, maintain Type 1 overwrite table in silver layer.",
)
async def schedule(run_date: date | None = None):
    """Pull MLB schedule and update Type 1 schedule table in silver layer."""
    logger = get_run_logger()
    run_date = run_date or date.today()
    logger.info("Schedule flow for %s", run_date)

    async with httpx.AsyncClient(timeout=30.0) as client:
        schedule_data = await fetch_schedule(client, run_date)

    total_games = schedule_data.get("totalGames", 0)
    logger.info("API returned %d games", total_games)

    bronze_dir = write_bronze(schedule_data, run_date)
    logger.info("Bronze written to %s", bronze_dir)

    today_df = flatten_schedule(schedule_data)
    logger.info("Flattened %d games", len(today_df))

    merged_df = apply_type1(today_df)
    write_silver(merged_df)

    logger.info("Done: %d total games in schedule table", len(merged_df))


if __name__ == "__main__":
    schedule.serve(
        name="schedule-daily",
        schedule=CronSchedule(cron="0 7 * * *", timezone="America/Los_Angeles"),
    )
