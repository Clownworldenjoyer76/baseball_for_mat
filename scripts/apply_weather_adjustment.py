#!/usr/bin/env python3
# /home/runner/work/baseball_for_mat/baseball_for_mat/scripts/apply_weather_adjustment.py
import pandas as pd
from pathlib import Path

# Inputs
BATTERS_HOME = "data/adjusted/batters_home.csv"
BATTERS_AWAY = "data/adjusted/batters_away.csv"
SCHED_FILE   = "data/bets/mlb_sched.csv"
WEATHER_FILE = "data/weather_adjustments.csv"
MAP_FILE     = "data/Data/team_name_map.csv"   # columns: name, team

# Outputs
OUTPUT_HOME = "data/adjusted/batters_home_weather.csv"
OUTPUT_AWAY = "data/adjusted/batters_away_weather.csv"
LOG_HOME    = "data/adjusted/log_weather_home.txt"
LOG_AWAY    = "data/adjusted/log_weather_away.txt"

# --- helpers ---
def _require(df: pd.DataFrame, cols: list[str], where: str) -> None:
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise ValueError(f"{where}: missing columns {miss}")

def _norm(s: str) -> str:
    return str(s).strip().lower()

def _as_key(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").astype(str)

def _build_team_map(path: str) -> dict:
    m = pd.read_csv(path)
    _require(m, ["name","team"], path)
    # keys lower-cased full names; values canonical short names
    return { _norm(n): str(t).strip() for n,t in zip(m["name"], m["team"]) }

def _canon(series: pd.Series, name_map: dict) -> pd.Series:
    return series.map(lambda s: name_map.get(_norm(s), str(s).strip()))

# --- loaders ---
def _load_schedule(name_map: dict) -> pd.DataFrame:
    sched = pd.read_csv(SCHED_FILE, dtype=str)
    _require(sched, ["game_id","date","home_team","away_team"], SCHED_FILE)
    sched["game_id"] = _as_key(sched["game_id"])
    sched["date"]    = _as_key(sched["date"])
    sched["home_canon"] = _canon(sched["home_team"], name_map)
    sched["away_canon"] = _canon(sched["away_team"], name_map)
    return sched[["game_id","date","home_canon","away_canon"]]

def _build_weather_by_game(name_map: dict, sched: pd.DataFrame) -> pd.DataFrame:
    wx = pd.read_csv(WEATHER_FILE, dtype=str)
    _require(wx, ["home_team","away_team"], WEATHER_FILE)
    wx["home_canon"] = _canon(wx["home_team"], name_map)
    wx["away_canon"] = _canon(wx["away_team"], name_map)

    # join by canonical team names to fetch game_id/date
    m = wx.merge(
        sched[["game_id","date","home_canon","away_canon"]],
        on=["home_canon","away_canon"],
        how="left"
    )
    m["game_id"] = _as_key(m["game_id"])
    m["date"]    = _as_key(m["date"])

    keep = {
        "venue","venue_name","location","matched_forecast_day",
        "matched_forecast_time","temperature","wind_speed",
        "wind_direction","humidity","precipitation","condition",
        "notes","game_time_et","fetched_at","weather_factor"
    }
    wx_cols = [c for c in m.columns if c in keep]
    out = m[["game_id","date"] + wx_cols].drop_duplicates(subset=["game_id"]).reset_index(drop=True)
    return out

def _attach_weather_by_game(batters: pd.DataFrame, wx_by_game: pd.DataFrame) -> pd.DataFrame:
    _require(batters, ["team","woba","game_id","date"], "batters input")
    bat = batters.copy()
    bat["game_id"] = _as_key(bat["game_id"])
    bat["date"]    = _as_key(bat["date"])

    wx = wx_by_game.copy()
    wx["game_id"] = _as_key(wx["game_id"])
    wx["date"]    = _as_key(wx["date"])

    out = bat.merge(wx, on=["game_id","date"], how="left")

    # numeric-safe adjustment
    woba_num = pd.to_numeric(out["woba"], errors="coerce")
    out["adj_woba_weather"] = woba_num
    if "temperature" in out.columns:
        t = pd.to_numeric(out["temperature"], errors="coerce")
        hot_mask  = t.notna() & (t >= 85)
        cold_mask = t.notna() & (t <= 50)
        out.loc[hot_mask,  "adj_woba_weather"] = woba_num.loc[hot_mask]  * 1.03
        out.loc[cold_mask, "adj_woba_weather"] = woba_num.loc[cold_mask] * 0.97

    return out

def _write_log(df: pd.DataFrame, path: str) -> None:
    if "last_name, first_name" not in df.columns or "adj_woba_weather" not in df.columns:
        return
    top5 = df.sort_values("adj_woba_weather", ascending=False).head(5)
    with open(path, "w") as f:
        for _, r in top5.iterrows():
            try:
                f.write(f"{r['last_name, first_name']} - {r['team']} - {float(r['adj_woba_weather']):.3f}\n")
            except Exception:
                f.write(f"{r['last_name, first_name']} - {r['team']} - {r['adj_woba_weather']}\n")

# --- main ---
def main() -> None:
    name_map = _build_team_map(MAP_FILE)
    sched = _load_schedule(name_map)
    wx_by_game = _build_weather_by_game(name_map, sched)

    bh = pd.read_csv(BATTERS_HOME, dtype=str)
    ba = pd.read_csv(BATTERS_AWAY, dtype=str)

    adj_home = _attach_weather_by_game(bh, wx_by_game)
    adj_away = _attach_weather_by_game(ba, wx_by_game)

    adj_home = adj_home[adj_home["game_id"].astype(str) != ""].reset_index(drop=True)
    adj_away = adj_away[adj_away["game_id"].astype(str) != ""].reset_index(drop=True)

    Path(OUTPUT_HOME).parent.mkdir(parents=True, exist_ok=True)
    adj_home.to_csv(OUTPUT_HOME, index=False)
    adj_away.to_csv(OUTPUT_AWAY, index=False)

    _write_log(adj_home, LOG_HOME)
    _write_log(adj_away, LOG_AWAY)

if __name__ == "__main__":
    main()
