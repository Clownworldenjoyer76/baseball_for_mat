#!/usr/bin/env python3
# /home/runner/work/baseball_for_mat/baseball_for_mat/scripts/apply_park_adjustment.py
import pandas as pd
from pathlib import Path

# Inputs
BATTERS_HOME = "data/adjusted/batters_home.csv"
BATTERS_AWAY = "data/adjusted/batters_away.csv"
SCHED_FILE   = "data/bets/mlb_sched.csv"
GAMES_FILE   = "data/raw/todaysgames_normalized.csv"
PF_DAY_FILE  = "data/Data/park_factors_day.csv"
PF_NGT_FILE  = "data/Data/park_factors_night.csv"
MAP_FILE     = "data/Data/team_name_map.csv"   # columns: name, team

# Outputs
OUT_HOME = "data/adjusted/batters_home_park.csv"
OUT_AWAY = "data/adjusted/batters_away_park.csv"
LOG_HOME = "data/adjusted/log_park_home.txt"
LOG_AWAY = "data/adjusted/log_park_away.txt"

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
    # away_canon not strictly required here, but harmless:
    sched["away_canon"] = _canon(sched["away_team"], name_map)
    return sched[["game_id","date","home_canon"]]

def _load_time_of_day() -> pd.DataFrame:
    games = pd.read_csv(GAMES_FILE, dtype=str)
    _require(games, ["home_team","game_time"], GAMES_FILE)
    games["home_team"] = games["home_team"].astype(str)
    ts = pd.to_datetime(games["game_time"], format="%I:%M %p", errors="coerce")
    games["hour"] = ts.dt.hour
    games["time_of_day"] = games["hour"].apply(lambda x: "day" if (pd.notna(x) and x < 18) else "night")
    # keep one row per home team with time_of_day
    return games[["home_team","time_of_day"]].drop_duplicates("home_team")

def _load_park_factors(name_map: dict) -> pd.DataFrame:
    pf_day = pd.read_csv(PF_DAY_FILE, dtype=str)
    pf_ngt = pd.read_csv(PF_NGT_FILE, dtype=str)
    _require(pf_day, ["home_team","Park Factor"], PF_DAY_FILE)
    _require(pf_ngt, ["home_team","Park Factor"], PF_NGT_FILE)
    pf_day = pf_day.assign(time_of_day="day")
    pf_ngt = pf_ngt.assign(time_of_day="night")
    pf = pd.concat([pf_day, pf_ngt], ignore_index=True)
    # canonicalize park factor home_team using the same map
    pf["home_canon"] = _canon(pf["home_team"], name_map)
    return pf[["home_canon","time_of_day","Park Factor"]].drop_duplicates(["home_canon","time_of_day"])

# --- transforms ---
def _attach_home_team_and_tod(batters: pd.DataFrame,
                              sched: pd.DataFrame,
                              tod: pd.DataFrame,
                              name_map: dict) -> pd.DataFrame:
    _require(batters, ["team","woba","game_id","date"], "batters input")
    x = batters.copy()
    x["game_id"] = _as_key(x["game_id"])
    x["date"]    = _as_key(x["date"])

    # attach canonical home team by game_id
    x = x.merge(sched, on="game_id", how="left")  # brings home_canon
    # attach time_of_day by matching canonical home team to raw 'home_team' in GAMES_FILE (canonicalize that too)
    tod2 = tod.copy()
    tod2["home_canon"] = _canon(tod2["home_team"], name_map)
    x = x.merge(tod2[["home_canon","time_of_day"]], on="home_canon", how="left")
    return x

def _apply_park(df: pd.DataFrame, pf: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["Park Factor"], errors="ignore")
    df = df.merge(pf, on=["home_canon","time_of_day"], how="left")

    # numeric-safe adjustment
    woba_num = pd.to_numeric(df["woba"], errors="coerce")
    pf_num   = pd.to_numeric(df["Park Factor"], errors="coerce")
    adj = woba_num.copy()
    mask = pf_num.notna()
    adj.loc[mask] = woba_num.loc[mask] * pf_num.loc[mask]
    df["adj_woba_park"] = adj
    return df

def _write_log(df: pd.DataFrame, path: str) -> None:
    if all(c in df.columns for c in ["last_name, first_name","team","adj_woba_park"]):
        top5 = df.sort_values("adj_woba_park", ascending=False).head(5)
        with open(path, "w") as f:
            for _, r in top5.iterrows():
                try:
                    f.write(f"{r['last_name, first_name']} - {r['team']} - {float(r['adj_woba_park']):.3f}\n")
                except Exception:
                    f.write(f"{r['last_name, first_name']} - {r['team']} - {r['adj_woba_park']}\n")

# --- main ---
def main() -> None:
    name_map = _build_team_map(MAP_FILE)
    sched = _load_schedule(name_map)
    tod   = _load_time_of_day()
    pf    = _load_park_factors(name_map)

    bh = pd.read_csv(BATTERS_HOME, dtype=str)
    ba = pd.read_csv(BATTERS_AWAY, dtype=str)

    bh2 = _attach_home_team_and_tod(bh, sched, tod, name_map)
    ba2 = _attach_home_team_and_tod(ba, sched, tod, name_map)

    bh3 = _apply_park(bh2, pf)
    ba3 = _apply_park(ba2, pf)

    bh3 = bh3[bh3["game_id"].astype(str) != ""].reset_index(drop=True)
    ba3 = ba3[ba3["game_id"].astype(str) != ""].reset_index(drop=True)

    Path(OUT_HOME).parent.mkdir(parents=True, exist_ok=True)
    bh3.to_csv(OUT_HOME, index=False)
    ba3.to_csv(OUT_AWAY, index=False)

    _write_log(bh3, LOG_HOME)
    _write_log(ba3, LOG_AWAY)

if __name__ == "__main__":
    main()
