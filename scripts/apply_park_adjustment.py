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

def _load_schedule(name_map: dict) -> pd.DataFrame:
    sched = pd.read_csv(SCHED_FILE, dtype=str)
    _require(sched, ["game_id","date","home_team","away_team"], SCHED_FILE)
    sched["game_id"] = _as_key(sched["game_id"])
    sched["date"]    = _as_key(sched["date"])
    sched["home_canon"] = _canon(sched["home_team"], name_map)
    return sched[["game_id","date","home_canon"]]

def _load_time_of_day(name_map: dict) -> pd.DataFrame:
    games = pd.read_csv(GAMES_FILE, dtype=str)
    _require(games, ["home_team","game_time"], GAMES_FILE)
    games["home_canon"] = _canon(games["home_team"], name_map)
    ts = pd.to_datetime(games["game_time"], format="%I:%M %p", errors="coerce")
    games["hour"] = ts.dt.hour
    games["time_of_day"] = games["hour"].apply(lambda x: "day" if (pd.notna(x) and x < 18) else "night")
    return games[["home_canon","time_of_day"]].drop_duplicates("home_canon")

def _load_park_factors(name_map: dict) -> pd.DataFrame:
    pf_day = pd.read_csv(PF_DAY_FILE, dtype=str).assign(time_of_day="day")
    pf_ngt = pd.read_csv(PF_NGT_FILE, dtype=str).assign(time_of_day="night")
    _require(pf_day, ["home_team","Park Factor"], PF_DAY_FILE)
    _require(pf_ngt, ["home_team","Park Factor"], PF_NGT_FILE)
    pf = pd.concat([pf_day, pf_ngt], ignore_index=True)
    pf["home_canon"] = _canon(pf["home_team"], name_map)
    return pf[["home_canon","time_of_day","Park Factor"]].drop_duplicates(["home_canon","time_of_day"])

def _attach_home_team_tod_and_date(batters: pd.DataFrame,
                                   sched: pd.DataFrame,
                                   tod: pd.DataFrame) -> pd.DataFrame:
    _require(batters, ["team","woba","game_id","date"], "batters input")
    x = batters.copy()
    x["game_id"] = _as_key(x["game_id"])
    x["date"]    = _as_key(x["date"])
    # bring canonical home team + date by game_id
    x = x.merge(sched, on="game_id", how="left")  # adds home_canon, date (preserves original date too)
    # bring time_of_day by canonical home team
    x = x.merge(tod, on="home_canon", how="left")
    # ensure a single date column is retained (prefer schedule date)
    x["date"] = x["date_y"].where(x.get("date_y").notna(), x.get("date_x"))
    x = x.drop(columns=[c for c in x.columns if c.endswith("_x") or c.endswith("_y")], errors="ignore")
    return x

def _apply_park(df: pd.DataFrame, pf: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["Park Factor"], errors="ignore")
    df = df.merge(pf, on=["home_canon","time_of_day"], how="left")
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

def main() -> None:
    name_map = _build_team_map(MAP_FILE)
    sched = _load_schedule(name_map)
    tod   = _load_time_of_day(name_map)
    pf    = _load_park_factors(name_map)

    bh = pd.read_csv(BATTERS_HOME, dtype=str)
    ba = pd.read_csv(BATTERS_AWAY, dtype=str)

    bh2 = _attach_home_team_tod_and_date(bh, sched, tod)
    ba2 = _attach_home_team_tod_and_date(ba, sched, tod)

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
