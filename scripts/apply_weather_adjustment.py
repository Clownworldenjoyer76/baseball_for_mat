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

def _scheduled_teams() -> set[str]:
    sched = pd.read_csv(SCHED_FILE, dtype=str)
    _require(sched, ["home_team","away_team"], SCHED_FILE)
    teams = pd.concat([sched["home_team"], sched["away_team"]], ignore_index=True).dropna().map(_norm).unique()
    return set(teams)

def _build_weather_long() -> pd.DataFrame:
    sched = pd.read_csv(SCHED_FILE, dtype=str)
    wx    = pd.read_csv(WEATHER_FILE, dtype=str)

    _require(sched, ["home_team","away_team"], SCHED_FILE)
    _require(wx,   ["home_team","away_team"], WEATHER_FILE)

    sched_venue = "venue_name" if "venue_name" in sched.columns else ("venue" if "venue" in sched.columns else None)
    wx_venue    = "venue" if "venue" in wx.columns else ("venue_name" if "venue_name" in wx.columns else None)

    for c in ("home_team","away_team"):
        if c in sched.columns: sched[c] = sched[c].astype(str).str.strip()
        if c in wx.columns:    wx[c]    = wx[c].astype(str).str.strip()

    if sched_venue and wx_venue:
        wx_sched = sched.merge(
            wx,
            left_on=["home_team","away_team", sched_venue],
            right_on=["home_team","away_team", wx_venue],
            how="left",
        )
    else:
        wx_sched = sched.merge(wx, on=["home_team","away_team"], how="left")

    keep_candidates = [
        "game_id","date",
        "venue_name","venue","location",
        "matched_forecast_day","matched_forecast_time",
        "temperature","wind_speed","wind_direction",
        "humidity","precipitation","condition","notes",
        "game_time_et","fetched_at","weather_factor",
        # team keys handled separately below
    ]
    keep_base = [c for c in keep_candidates if c in wx_sched.columns]

    # Build home_long without away_team; rename home_team -> team
    cols_home = keep_base + (["home_team"] if "home_team" in wx_sched.columns else [])
    home_long = wx_sched[cols_home].copy()
    if "home_team" in home_long.columns:
        home_long = home_long.rename(columns={"home_team":"team"})
    home_long["side"] = "home"

    # Build away_long without home_team; rename away_team -> team
    cols_away = keep_base + (["away_team"] if "away_team" in wx_sched.columns else [])
    away_long = wx_sched[cols_away].copy()
    if "away_team" in away_long.columns:
        away_long = away_long.rename(columns={"away_team":"team"})
    away_long["side"] = "away"

    # Ensure unique columns before concat
    home_long = home_long.loc[:, ~home_long.columns.duplicated()]
    away_long = away_long.loc[:, ~away_long.columns.duplicated()]

    wx_long = pd.concat([home_long, away_long], ignore_index=True, sort=False)
    wx_long["team_key"] = wx_long["team"].map(_norm)
    return wx_long

def _attach_weather(batters: pd.DataFrame, side: str, wx_long: pd.DataFrame) -> pd.DataFrame:
    _require(batters, ["team","woba"], f"batters_{side}.csv")
    out = batters.copy()
    out["team_key"] = out["team"].map(_norm)

    wx_side = wx_long[wx_long["side"] == side].drop(columns=["team"], errors="ignore")
    out = out.merge(wx_side, on="team_key", how="left", suffixes=("", "_wx"))

    out["adj_woba_weather"] = out["woba"]
    if "temperature" in out.columns:
        t = pd.to_numeric(out["temperature"], errors="coerce")
        out.loc[t.notna() & (t >= 85), "adj_woba_weather"] *= 1.03
        out.loc[t.notna() & (t <= 50), "adj_woba_weather"] *= 0.97

    return out.drop(columns=["team_key"], errors="ignore")

def _write_log(df: pd.DataFrame, path: str) -> None:
    if "last_name, first_name" not in df.columns:
        return
    top5 = df.sort_values("adj_woba_weather", ascending=False).head(5)
    with open(path, "w") as f:
        for _, r in top5.iterrows():
            f.write(f"{r['last_name, first_name']} - {r['team']} - {r['adj_woba_weather']:.3f}\n")

def main() -> None:
    bat_home = pd.read_csv(BATTERS_HOME)
    bat_away = pd.read_csv(BATTERS_AWAY)

    wx_long = _build_weather_long()
    sched_teams = _scheduled_teams()

    adj_home = _attach_weather(bat_home, "home", wx_long)
    adj_away = _attach_weather(bat_away, "away", wx_long)

    adj_home = adj_home[adj_home["team"].map(_norm).isin(sched_teams)].reset_index(drop=True)
    adj_away = adj_away[adj_away["team"].map(_norm).isin(sched_teams)].reset_index(drop=True)

    Path(OUTPUT_HOME).parent.mkdir(parents=True, exist_ok=True)
    adj_home.to_csv(OUTPUT_HOME, index=False)
    adj_away.to_csv(OUTPUT_AWAY, index=False)

    _write_log(adj_home, LOG_HOME)
    _write_log(adj_away, LOG_AWAY)

if __name__ == "__main__":
    main()
