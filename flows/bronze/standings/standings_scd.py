"""
Standings SCD flow -- pull MLB standings, maintain Type 2 Slowly Changing Dimensions.

Bronze: raw JSON from MLB Stats API, partitioned by date.
Silver: standings_current.parquet (latest state) + standings_history.parquet (full SCD2 history).
"""

from datetime import date
from pathlib import Path

import json
import httpx
import polars as pl
from prefect import flow, get_run_logger, task
from prefect.client.schemas.schedules import CronSchedule

BRONZE_ROOT = Path("/data/bronze/standings")
SILVER_ROOT = Path("/data/silver/standings")
STANDINGS_URL = "https://statsapi.mlb.com/api/v1/standings"
CURRENT_FILE = SILVER_ROOT / "standings_current.parquet"
HISTORY_FILE = SILVER_ROOT / "standings_history.parquet"

DIVISION_NAMES = {
    200: "AL West",
    201: "AL East",
    202: "AL Central",
    203: "NL West",
    204: "NL East",
    205: "NL Central",
}

LEAGUE_NAMES = {
    103: "American League",
    104: "National League",
}


@task(retries=2, retry_delay_seconds=10)
async def fetch_standings(client: httpx.AsyncClient) -> dict:
    resp = await client.get(
        STANDINGS_URL,
        params={"leagueId": "103,104", "hydrate": "division"},
    )
    resp.raise_for_status()
    return resp.json()


@task
def write_bronze(standings_data: dict, run_date: date) -> Path:
    day_dir = BRONZE_ROOT / run_date.isoformat()
    day_dir.mkdir(parents=True, exist_ok=True)
    (day_dir / "standings.json").write_text(json.dumps(standings_data, indent=2))
    return day_dir


@task
def flatten_standings(standings_data: dict) -> pl.DataFrame:
    rows = []
    for record in standings_data.get("records", []):
        division_id = record["division"]["id"]
        division_name = record.get("division", {}).get("name") or DIVISION_NAMES.get(
            division_id, f"Unknown ({division_id})"
        )
        league_id = record["league"]["id"]

        for team in record.get("teamRecords", []):
            rows.append(
                {
                    "team_id": team["team"]["id"],
                    "team_name": team["team"]["name"],
                    "division_id": division_id,
                    "division_name": division_name,
                    "league_id": league_id,
                    "division_rank": team["divisionRank"],
                    "games_back": team["gamesBack"],
                    "wild_card_rank": team.get("wildCardRank"),
                    "wild_card_games_back": team["wildCardGamesBack"],
                    "wins": team["wins"],
                    "losses": team["losses"],
                    "winning_percentage": team["winningPercentage"],
                    "streak": team["streak"]["streakCode"],
                }
            )

    schema = {
        "team_id": pl.Int64,
        "team_name": pl.Utf8,
        "division_id": pl.Int64,
        "division_name": pl.Utf8,
        "league_id": pl.Int64,
        "division_rank": pl.Utf8,
        "games_back": pl.Utf8,
        "wild_card_rank": pl.Utf8,
        "wild_card_games_back": pl.Utf8,
        "wins": pl.Int64,
        "losses": pl.Int64,
        "winning_percentage": pl.Utf8,
        "streak": pl.Utf8,
    }

    if not rows:
        return pl.DataFrame(schema=schema)

    return pl.DataFrame(rows, schema=schema)


TRACK_COLS = [
    "division_rank",
    "games_back",
    "wild_card_rank",
    "wild_card_games_back",
    "wins",
    "losses",
    "winning_percentage",
    "streak",
]


@task
def apply_scd2(today_df: pl.DataFrame, run_date: date) -> pl.DataFrame:
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
                pl.col(col).cast(pl.Utf8).fill_null("__NULL__")
                != pl.col(f"{col}_new").cast(pl.Utf8).fill_null("__NULL__")
            )
        changed_ids = set(merged.filter(changed_mask)["team_id"].to_list())
    else:
        changed_ids = set()

    logger.info("Changed: %d teams with attribute updates", len(changed_ids))

    close_ids = removed_ids | changed_ids

    unchanged = prev_current.filter(
        ~pl.col("team_id").is_in(list(close_ids | new_ids))
    )

    closed = prev_current.filter(
        pl.col("team_id").is_in(list(close_ids))
    ).with_columns(
        pl.lit(run_date).alias("effective_to"),
        pl.lit(False).alias("is_current"),
    )

    insert_ids = new_ids | changed_ids
    new_records = today_keyed.filter(pl.col("team_id").is_in(list(insert_ids)))

    result = pl.concat([unchanged, closed, new_records], how="diagonal_relaxed")
    return result


@task
def write_silver(scd_df: pl.DataFrame) -> None:
    logger = get_run_logger()
    SILVER_ROOT.mkdir(parents=True, exist_ok=True)

    scd_df.write_parquet(CURRENT_FILE)
    logger.info("Wrote %d records to %s", len(scd_df), CURRENT_FILE)

    if HISTORY_FILE.exists():
        existing = pl.read_parquet(HISTORY_FILE)
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
    name="standings-scd",
    description="Pull MLB standings, maintain Type 2 SCD in silver layer.",
)
async def standings_scd(run_date: date | None = None):
    logger = get_run_logger()
    run_date = run_date or date.today()
    logger.info("Standings SCD flow for %s", run_date)

    async with httpx.AsyncClient(timeout=30.0) as client:
        standings_data = await fetch_standings(client)

    division_count = len(standings_data.get("records", []))
    team_count = sum(
        len(r.get("teamRecords", [])) for r in standings_data.get("records", [])
    )
    logger.info("Fetched standings: %d divisions, %d teams", division_count, team_count)

    bronze_dir = write_bronze(standings_data, run_date)
    logger.info("Bronze written to %s", bronze_dir)

    today_df = flatten_standings(standings_data)
    logger.info("Flattened %d standings entries", len(today_df))

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
    standings_scd.serve(
        name="standings-scd-daily",
        schedule=CronSchedule(cron="15 6 * * *", timezone="America/Los_Angeles"),
    )
