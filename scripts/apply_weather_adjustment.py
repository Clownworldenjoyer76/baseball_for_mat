#!/usr/bin/env python3
# /home/runner/work/baseball_for_mat/baseball_for_mat/scripts/apply_weather_adjustment.py
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

def _load_schedule() -> pd.DataFrame:
    sched = pd.read_csv(SCHED_FILE, dtype=str)
    _require(sched, ["game_id", "home_team", "away_team"], SCHED_FILE)
    sched["home_team_key"] = sched["home_team"].map(_norm)
    sched["away_team_key"] = sched["away_team"].map(_norm)
    # venue key (optional)
    if "venue_name" in sched.columns:
        sched["venue_key"] = sched["venue_name"].map(_norm)
    elif "venue" in sched.columns:
        sched["venue_key"] = sched["venue"].map(_norm)
    else:
        sched["venue_key"] = pd.NA
    return sched

def _build_weather_by_game(sched: pd.DataFrame) -> pd.DataFrame:
    wx = pd.read_csv(WEATHER_FILE, dtype=str)
    _require(wx, ["home_team", "away_team"], WEATHER_FILE)
    wx["home_team_key"] = wx["home_team"].map(_norm)
    wx["away_team_key"] = wx["away_team"].map(_norm)
    if "venue" in wx.columns:
        wx["venue_key"] = wx["venue"].map(_norm)
    elif "venue_name" in wx.columns:
        wx["venue_key"] = wx["venue_name"].map(_norm)
    else:
        wx["venue_key"] = pd.NA
    # venue-aware merge first, fallback to teams-only
    if sched["venue_key"].notna().any() and wx["venue_key"].notna().any():
        merged = wx.merge(
            sched[["game_id", "date", "home_team_key", "away_team_key", "venue_key"]],
            on=["home_team_key", "away_team_key", "venue_key"],
            how="left",
        )
    else:
        merged = wx.merge(
            sched[["game_id", "date", "home_team_key", "away_team_key"]],
            on=["home_team_key", "away_team_key"],
            how="left",
        )
    keep = {
        "venue", "venue_name", "location", "matched_forecast_day",
        "matched_forecast_time", "temperature", "wind_speed",
        "wind_direction", "humidity", "precipitation", "condition",
        "notes", "game_time_et", "fetched_at", "weather_factor",
    }
    wx_cols = [c for c in merged.columns if c in keep]
    out = merged[["game_id", "date"] + wx_cols].drop_duplicates("game_id")
    return out.reset_index(drop=True)

def _attach_game_id_side_agnostic(batters: pd.DataFrame,
                                  sched: pd.DataFrame) -> pd.DataFrame:
    _require(batters, ["team", "woba"], "batters")
    b = batters.copy()
    b["team_key"] = b["team"].map(_norm)
    # match against home
    m_home = b.merge(
        sched[["game_id", "date", "home_team_key"]],
        left_on="team_key",
        right_on="home_team_key",
        how="left",
    ).rename(columns={"game_id": "gid_home", "date": "date_home"})
    m_home = m_home.drop(columns=["home_team_key"])
    # match against away
    m_away = b.merge(
        sched[["game_id", "date", "away_team_key"]],
        left_on="team_key",
        right_on="away_team_key",
        how="left",
    ).rename(columns={"game_id": "gid_away", "date": "date_away"})
    m_away = m_away.drop(columns=["away_team_key"])
    # coalesce game_id + date
    b = b.join(m_home[["gid_home", "date_home"]]).join(m_away[["gid_away", "date_away"]])
    b["game_id"] = b["gid_home"].where(b["gid_home"].notna(), b["gid_away"])
    b["date"]    = b["date_home"].where(b["date_home"].notna(), b["date_away"])
    b = b.drop(columns=["gid_home", "date_home", "gid_away", "date_away"])
    return b

def _attach_weather_by_game(batters_with_gid: pd.DataFrame,
                            wx_by_game: pd.DataFrame) -> pd.DataFrame:
    out = batters_with_gid.merge(wx_by_game, on=["game_id", "date"], how="left")
    out["adj_woba_weather"] = out["woba"]
    if "temperature" in out.columns:
        t = pd.to_numeric(out["temperature"], errors="coerce")
        out.loc[t.notna() & (t >= 85), "adj_woba_weather"] *= 1.03
        out.loc[t.notna() & (t <= 50), "adj_woba_weather"] *= 0.97
    return out

def _write_log(df: pd.DataFrame, path: str) -> None:
    if "last_name, first_name" not in df.columns:
        return
    top5 = df.sort_values("adj_woba_weather", ascending=False).head(5)
    with open(path, "w") as f:
        for _, r in top5.iterrows():
            f.write(f"{r['last_name, first_name']} - {r['team']} - {r['adj_woba_weather']:.3f}\n")

def main() -> None:
    sched = _load_schedule()
    wx_by_game = _build_weather_by_game(sched)
    bat_home = pd.read_csv(BATTERS_HOME)
    bat_away = pd.read_csv(BATTERS_AWAY)
    home_gid = _attach_game_id_side_agnostic(bat_home, sched)
    away_gid = _attach_game_id_side_agnostic(bat_away, sched)
    adj_home = _attach_weather_by_game(home_gid, wx_by_game)
    adj_away = _attach_weather_by_game(away_gid, wx_by_game)
    # keep only rows with a matched game_id
    adj_home = adj_home[adj_home["game_id"].notna()].reset_index(drop=True)
    adj_away = adj_away[adj_away["game_id"].notna()].reset_index(drop=True)
    Path(OUTPUT_HOME).parent.mkdir(parents=True, exist_ok=True)
    adj_home.to_csv(OUTPUT_HOME, index=False)
    adj_away.to_csv(OUTPUT_AWAY, index=False)
    _write_log(adj_home, LOG_HOME)
    _write_log(adj_away, LOG_AWAY)

if __name__ == "__main__":
    main()
