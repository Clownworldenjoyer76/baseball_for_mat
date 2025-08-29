#!/usr/bin/env python3
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
LOG_HOME = "log_weather_home.txt"
LOG_AWAY = "log_weather_away.txt"

def _norm(s): return str(s).strip()

def _require(df, cols, where):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{where}: missing columns {missing}")

def _build_weather_by_team():
    sched = pd.read_csv(SCHED_FILE)
    wx    = pd.read_csv(WEATHER_FILE)

    _require(sched, ["home_team","away_team","venue_name"], SCHED_FILE)
    _require(wx,   ["home_team","away_team","venue","location","temperature",
                    "wind_speed","wind_direction","humidity","precipitation",
                    "condition","fetched_at","weather_factor","game_time_et"], WEATHER_FILE)

    # Merge weather onto schedule by game (anchors the matchup)
    wx_sched = sched.merge(
        wx,
        on=["home_team","away_team","venue_name"],
        how="left",
        validate="one_to_one"
    )

    # Long-form: one row per (team, side)
    keep_cols = ["game_id","date","venue_name","location","matched_forecast_day",
                 "matched_forecast_time","temperature","wind_speed","wind_direction",
                 "humidity","precipitation","condition","notes","game_time_et",
                 "fetched_at","weather_factor","home_team","away_team"]

    for c in keep_cols:
        if c not in wx_sched.columns:
            wx_sched[c] = pd.NA

    home_long = wx_sched[keep_cols + ["home_team"]].rename(columns={"home_team":"team"})
    home_long["side"] = "home"

    away_long = wx_sched[keep_cols + ["away_team"]].rename(columns={"away_team":"team"})
    away_long["side"] = "away"

    wx_long = pd.concat([home_long, away_long], ignore_index=True)
    wx_long["team_key"] = wx_long["team"].map(_norm)
    return wx_long

def _attach_weather(batters: pd.DataFrame, side: str, wx_long: pd.DataFrame) -> pd.DataFrame:
    _require(batters, ["team","woba"], f"batters_{side}.csv")
    bat = batters.copy()
    bat["team_key"] = bat["team"].map(_norm)

    wx_side = wx_long[wx_long["side"] == side].copy()
    out = bat.merge(
        wx_side.drop(columns=["team"], errors="ignore"),
        on="team_key",
        how="left",
        suffixes=("", "_wx")
    )

    # Initialize and adjust
    out["adj_woba_weather"] = out["woba"]
    if "temperature" in out.columns:
        temp = out["temperature"]
        out.loc[temp.notna() & (temp >= 85), "adj_woba_weather"] *= 1.03
        out.loc[temp.notna() & (temp <= 50), "adj_woba_weather"] *= 0.97

    return out.drop(columns=["team_key"], errors="ignore")

def _write_log(df: pd.DataFrame, path: str):
    if "last_name, first_name" not in df.columns:
        return
    top5 = df.sort_values("adj_woba_weather", ascending=False).head(5)
    with open(path, "w") as f:
        for _, r in top5.iterrows():
            f.write(f"{r['last_name, first_name']} - {r['team']} - {r['adj_woba_weather']:.3f}\n")

def main():
    # Load inputs
    bat_home = pd.read_csv(BATTERS_HOME)
    bat_away = pd.read_csv(BATTERS_AWAY)

    wx_long = _build_weather_by_team()

    # Attach weather
    adj_home = _attach_weather(bat_home, "home", wx_long)
    adj_away = _attach_weather(bat_away, "away", wx_long)

    # Save
    Path(OUTPUT_HOME).parent.mkdir(parents=True, exist_ok=True)
    adj_home.to_csv(OUTPUT_HOME, index=False)
    adj_away.to_csv(OUTPUT_AWAY, index=False)

    # Logs
    _write_log(adj_home, LOG_HOME)
    _write_log(adj_away, LOG_AWAY)

if __name__ == "__main__":
    main()
