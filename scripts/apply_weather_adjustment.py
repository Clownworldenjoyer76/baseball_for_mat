#!/usr/bin/env python3
# scripts/apply_weather_adjustment.py
import pandas as pd
from pathlib import Path

# Inputs
BATTERS_HOME = "data/adjusted/batters_home.csv"
BATTERS_AWAY = "data/adjusted/batters_away.csv"
SCHED_FILE   = "data/bets/mlb_sched.csv"
WEATHER_FILE = "data/weather_adjustments.csv"

# Outputs
OUTPUT_HOME = "data/adjusted/batters_home_weather.csv"
OUTPUT_AWAY = "data/adjusted/batters_away_weather.csv"
LOG_HOME    = "log_weather_home.txt"
LOG_AWAY    = "log_weather_away.txt"


def _norm(s: str) -> str:
    return str(s).strip().lower()


def _require(df: pd.DataFrame, cols: list[str], where: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{where}: missing columns {missing}")


def _build_weather_long() -> pd.DataFrame:
    sched = pd.read_csv(SCHED_FILE)
    wx    = pd.read_csv(WEATHER_FILE)

    _require(sched, ["home_team", "away_team"], SCHED_FILE)
    _require(wx,   ["home_team", "away_team"], WEATHER_FILE)

    # Detect venue columns (schedule vs. weather)
    sched_venue = (
        "venue_name" if "venue_name" in sched.columns else
        ("venue" if "venue" in sched.columns else None)
    )
    wx_venue = (
        "venue" if "venue" in wx.columns else
        ("venue_name" if "venue_name" in wx.columns else None)
    )

    # Normalize team strings
    for c in ("home_team", "away_team"):
        if c in sched.columns:
            sched[c] = sched[c].astype(str).str.strip()
        if c in wx.columns:
            wx[c] = wx[c].astype(str).str.strip()

    # Merge schedule ↔ weather (prefer venue-aware join; fallback to teams-only)
    if sched_venue and wx_venue:
        wx_sched = sched.merge(
            wx,
            left_on=["home_team", "away_team", sched_venue],
            right_on=["home_team", "away_team", wx_venue],
            how="left",
        )
    else:
        wx_sched = sched.merge(
            wx,
            on=["home_team", "away_team"],
            how="left",
        )

    # Columns to retain if present
    keep_candidates = [
        "game_id", "date",
        "venue_name", "venue", "location",
        "matched_forecast_day", "matched_forecast_time",
        "temperature", "wind_speed", "wind_direction",
        "humidity", "precipitation", "condition", "notes",
        "game_time_et", "fetched_at", "weather_factor",
        "home_team", "away_team",
    ]
    keep_cols = [c for c in keep_candidates if c in wx_sched.columns]

    # Long-form (one row per (team, side))
    home_long = wx_sched[keep_cols].copy()
    if "home_team" in home_long.columns:
        home_long = home_long.rename(columns={"home_team": "team"})
    home_long["side"] = "home"

    away_long = wx_sched[keep_cols].copy()
    if "away_team" in away_long.columns:
        away_long = away_long.rename(columns={"away_team": "team"})
    away_long["side"] = "away"

    # Remove the opposite-team column if both exist to avoid confusion
    drop_if_present = []
    if "away_team" in home_long.columns:
        drop_if_present.append("away_team")
    if "home_team" in away_long.columns:
        drop_if_present.append("home_team")
    home_long = home_long.drop(columns=drop_if_present, errors="ignore")
    away_long = away_long.drop(columns=drop_if_present, errors="ignore")

    wx_long = pd.concat([home_long, away_long], ignore_index=True)
    wx_long["team_key"] = wx_long["team"].map(_norm)
    return wx_long


def _attach_weather(
    batters: pd.DataFrame,
    side: str,
    wx_long: pd.DataFrame
) -> pd.DataFrame:
    _require(batters, ["team", "woba"], f"batters_{side}.csv")

    out = batters.copy()
    out["team_key"] = out["team"].map(_norm)

    wx_side = wx_long[wx_long["side"] == side].copy()
    wx_side = wx_side.drop(columns=["team"], errors="ignore")

    out = out.merge(
        wx_side,
        on="team_key",
        how="left",
        suffixes=("", "_wx"),
    )

    # Initialize and adjust
    out["adj_woba_weather"] = out["woba"]
    if "temperature" in out.columns:
        t = out["temperature"]
        out.loc[t.notna() & (t >= 85), "adj_woba_weather"] *= 1.03
        out.loc[t.notna() & (t <= 50), "adj_woba_weather"] *= 0.97

    return out.drop(columns=["team_key"], errors="ignore")


def _write_log(df: pd.DataFrame, path: str) -> None:
    if "last_name, first_name" not in df.columns:
        return
    top5 = df.sort_values("adj_woba_weather", ascending=False).head(5)
    with open(path, "w") as f:
        for _, r in top5.iterrows():
            f.write(
                f"{r['last_name, first_name']} - "
                f"{r['team']} - "
                f"{r['adj_woba_weather']:.3f}\n"
            )


def main() -> None:
    bat_home = pd.read_csv(BATTERS_HOME)
    bat_away = pd.read_csv(BATTERS_AWAY)

    wx_long = _build_weather_long()

    adj_home = _attach_weather(bat_home, "home", wx_long)
    adj_away = _attach_weather(bat_away, "away", wx_long)

    Path(OUTPUT_HOME).parent.mkdir(parents=True, exist_ok=True)
    adj_home.to_csv(OUTPUT_HOME, index=False)
    adj_away.to_csv(OUTPUT_AWAY, index=False)

    _write_log(adj_home, LOG_HOME)
    _write_log(adj_away, LOG_AWAY)


if __name__ == "__main__":
    main()
