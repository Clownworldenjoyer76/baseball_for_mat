#!/usr/bin/env python3
"""
Generate data/raw/mlb_schedule_today.csv with correct team abbreviations
using data/manual/team_directory.csv (exact headers).

Also enriches data/raw/todaysgames_normalized.csv (if present) with:
  - game_id (MLB gamePk)
  - home_team_id / away_team_id
  - home_team_abbr / away_team_abbr
  - venue_id / venue_name
"""

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from zoneinfo import ZoneInfo

# ---- Paths (DO NOT CHANGE) ----
OUT_CSV = Path("data/raw/mlb_schedule_today.csv")
GAMES_NORM = Path("data/raw/todaysgames_normalized.csv")
TEAM_DIR = Path("data/manual/team_directory.csv")

API = "https://statsapi.mlb.com/api/v1/schedule"

# ---- Helpers ----
def _today_et() -> str:
    return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")


def _load_team_directory() -> pd.DataFrame:
    if not TEAM_DIR.exists():
        print(f"ERROR: missing {TEAM_DIR}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(TEAM_DIR)
    expected = {"Team Name", "Team ID", "Abbreviation"}
    have = set(df.columns.tolist())
    if not expected.issubset(have):
        print(
            "ERROR: team_directory.csv must have EXACT headers: "
            "'Team Name','Team ID','Abbreviation'",
            file=sys.stderr,
        )
        sys.exit(1)

    # Ensure correct types
    df["Team ID"] = pd.to_numeric(df["Team ID"], errors="coerce").astype("Int64")
    df["Abbreviation"] = df["Abbreviation"].astype(str)
    df["Team Name"] = df["Team Name"].astype(str)
    return df


def _fetch_schedule(date_str: str) -> list[dict]:
    params = {
        "sportId": 1,
        "date": date_str,
        "hydrate": "probablePitcher,team,venue",
        "language": "en",
    }
    r = requests.get(API, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    dates = data.get("dates", [])
    return dates[0].get("games", []) if dates else []


def _build_rows(games: list[dict], id_to_abbr: dict[int, str]) -> list[dict]:
    rows: list[dict] = []
    for g in games:
        game_pk = g.get("gamePk")
        game_date_iso = g.get("gameDate", "")
        try:
            # Normalize to America/New_York wall time HH:MM AM/PM
            dt_utc = datetime.fromisoformat(game_date_iso.replace("Z", "+00:00"))
            dt_et = dt_utc.astimezone(ZoneInfo("America/New_York"))
            game_time_local = dt_et.strftime("%I:%M %p").lstrip("0")
        except Exception:
            game_time_local = ""

        teams = g.get("teams", {}) or {}
        home = teams.get("home", {}) or {}
        away = teams.get("away", {}) or {}
        home_team = (home.get("team") or {})
        away_team = (away.get("team") or {})
        venue = g.get("venue") or {}

        home_id = home_team.get("id")
        away_id = away_team.get("id")
        home_name = home_team.get("name") or ""
        away_name = away_team.get("name") or ""

        # Map abbreviations STRICTLY by Team ID via team_directory.csv
        home_abbr = id_to_abbr.get(int(home_id)) if home_id is not None else None
        away_abbr = id_to_abbr.get(int(away_id)) if away_id is not None else None

        rows.append(
            {
                "date_et": dt_et.strftime("%Y-%m-%d") if game_time_local else "",
                "game_id": game_pk,
                "game_time_local": game_time_local,
                "home_team_id": home_id,
                "home_team_name": home_name,
                "home_team_abbr": home_abbr,
                "away_team_id": away_id,
                "away_team_name": away_name,
                "away_team_abbr": away_abbr,
                "venue_id": venue.get("id"),
                "venue_name": venue.get("name"),
            }
        )
    return rows


def _write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "date_et",
        "game_id",
        "game_time_local",
        "home_team_id",
        "home_team_name",
        "home_team_abbr",
        "away_team_id",
        "away_team_name",
        "away_team_abbr",
        "venue_id",
        "venue_name",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _enrich_todaysgames_normalized(rows_df: pd.DataFrame) -> None:
    if not GAMES_NORM.exists():
        return

    tg = pd.read_csv(GAMES_NORM)

    # Expect these columns from earlier workflow
    # home_team / away_team are abbreviations after normalization
    needed = {"home_team", "away_team"}
    if not needed.issubset(set(tg.columns)):
        return

    # Build join keys from schedule rows
    slim = rows_df[
        [
            "home_team_abbr",
            "away_team_abbr",
            "game_id",
            "home_team_id",
            "away_team_id",
            "venue_id",
            "venue_name",
            "game_time_local",
        ]
    ].drop_duplicates()

    merged = tg.merge(
        slim,
        left_on=["home_team", "away_team"],
        right_on=["home_team_abbr", "away_team_abbr"],
        how="left",
    )

    # Write back in place
    merged.to_csv(GAMES_NORM, index=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="YYYY-MM-DD in America/New_York")
    args = ap.parse_args()

    date_str = args.date or _today_et()

    team_df = _load_team_directory()
    id_to_abbr = dict(zip(team_df["Team ID"].astype(int), team_df["Abbreviation"]))

    games = _fetch_schedule(date_str)
    rows = _build_rows(games, id_to_abbr)
    _write_csv(rows, OUT_CSV)

    # Enrich normalized games file if present
    rows_df = pd.DataFrame(rows)
    _enrich_todaysgames_normalized(rows_df)

    print(
        f"✅ fetch_mlb_ids: wrote {len(rows)} rows -> {OUT_CSV} (ET date={date_str})",
        file=sys.stderr,
    )
    if GAMES_NORM.exists():
        print(
            f"✅ fetch_mlb_ids: enriched {GAMES_NORM} with game_id/meta; rows={len(pd.read_csv(GAMES_NORM))}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
