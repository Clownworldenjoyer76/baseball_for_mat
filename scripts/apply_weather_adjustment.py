#!/usr/bin/env python3
# /home/runner/work/baseball_for_mat/baseball_for_mat/scripts/apply_weather_adjustment.py

import pandas as pd
from pathlib import Path

BATTERS_HOME = "data/adjusted/batters_home.csv"
BATTERS_AWAY = "data/adjusted/batters_away.csv"
WEATHER_FILE = "data/weather_adjustments.csv"

OUTPUT_HOME = "data/adjusted/batters_home_weather.csv"
OUTPUT_AWAY = "data/adjusted/batters_away_weather.csv"
LOG_HOME    = "data/adjusted/log_weather_home.txt"
LOG_AWAY    = "data/adjusted/log_weather_away.txt"

def _require(df: pd.DataFrame, cols: list[str], where: str) -> None:
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise ValueError(f"{where}: missing columns {miss}")

def _as_key(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").astype(str)

def _attach_weather_by_game(batters: pd.DataFrame, wx: pd.DataFrame) -> pd.DataFrame:
    _require(batters, ["game_id", "date", "team", "woba"], "batters input")
    b = batters.copy()
    b["game_id"] = _as_key(b["game_id"])
    b["date"]    = _as_key(b["date"])

    _require(wx, ["game_id", "date"], WEATHER_FILE)
    w = wx.copy()
    w["game_id"] = _as_key(w["game_id"])
    w["date"]    = _as_key(w["date"])

    out = b.merge(w, on=["game_id", "date"], how="left")

    woba_num = pd.to_numeric(out["woba"], errors="coerce")
    out["adj_woba_weather"] = woba_num

    if "temperature" in out.columns:
        t = pd.to_numeric(out["temperature"], errors="coerce")
        hot = t.notna() & (t >= 85)
        cold = t.notna() & (t <= 50)
        out.loc[hot,  "adj_woba_weather"] = woba_num.loc[hot]  * 1.03
        out.loc[cold, "adj_woba_weather"] = woba_num.loc[cold] * 0.97

    return out

def _write_log(df: pd.DataFrame, path: str) -> None:
    if "last_name, first_name" not in df.columns:
        return
    if "adj_woba_weather" not in df.columns:
        return
    top5 = df.sort_values("adj_woba_weather", ascending=False).head(5)
    with open(path, "w") as f:
        for _, r in top5.iterrows():
            try:
                f.write(f"{r['last_name, first_name']} - {r['team']} - "
                        f"{float(r['adj_woba_weather']):.3f}\n")
            except Exception:
                f.write(f"{r['last_name, first_name']} - {r['team']} - "
                        f"{r['adj_woba_weather']}\n")

def main():
    bh = pd.read_csv(BATTERS_HOME, dtype=str)
    ba = pd.read_csv(BATTERS_AWAY, dtype=str)
    wx = pd.read_csv(WEATHER_FILE, dtype=str)

    adj_home = _attach_weather_by_game(bh, wx)
    adj_away = _attach_weather_by_game(ba, wx)

    adj_home = adj_home[adj_home["game_id"].astype(str) != ""].reset_index(drop=True)
    adj_away = adj_away[adj_away["game_id"].astype(str) != ""].reset_index(drop=True)

    Path(OUTPUT_HOME).parent.mkdir(parents=True, exist_ok=True)
    adj_home.to_csv(OUTPUT_HOME, index=False)
    adj_away.to_csv(OUTPUT_AWAY, index=False)

    _write_log(adj_home, LOG_HOME)
    _write_log(adj_away, LOG_AWAY)

if __name__ == "__main__":
    main()
