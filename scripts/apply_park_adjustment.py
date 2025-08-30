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

# Outputs
OUT_HOME = "data/adjusted/batters_home_park.csv"
OUT_AWAY = "data/adjusted/batters_away_park.csv"
LOG_HOME = "data/adjusted/log_park_home.txt"
LOG_AWAY = "data/adjusted/log_park_away.txt"

def _norm(s: str) -> str:
    return str(s).strip().lower()

def _require(df: pd.DataFrame, cols: list[str], where: str) -> None:
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise ValueError(f"{where}: missing columns {miss}")

def _load_schedule() -> pd.DataFrame:
    sched = pd.read_csv(SCHED_FILE, dtype=str)
    _require(sched, ["game_id", "date", "home_team", "away_team"], SCHED_FILE)
    sched["home_key"] = sched["home_team"].map(_norm)
    sched["away_key"] = sched["away_team"].map(_norm)
    return sched[["game_id", "date", "home_team", "home_key", "away_team", "away_key"]]

def _load_time_of_day() -> pd.DataFrame:
    games = pd.read_csv(GAMES_FILE, dtype=str)
    _require(games, ["home_team", "game_time"], GAMES_FILE)
    games["home_key"] = games["home_team"].map(_norm)
    ts = pd.to_datetime(games["game_time"], format="%I:%M %p", errors="coerce")
    games["hour"] = ts.dt.hour
    games["time_of_day"] = games["hour"].apply(lambda x: "day" if (pd.notna(x) and x < 18) else "night")
    return games[["home_key", "time_of_day"]].drop_duplicates("home_key")

def _load_park_factors() -> pd.DataFrame:
    pf_day = pd.read_csv(PF_DAY_FILE)
    pf_ngt = pd.read_csv(PF_NGT_FILE)
    _require(pf_day, ["home_team", "Park Factor"], PF_DAY_FILE)
    _require(pf_ngt, ["home_team", "Park Factor"], PF_NGT_FILE)
    pf_day = pf_day.assign(time_of_day="day")
    pf_ngt = pf_ngt.assign(time_of_day="night")
    pf = pd.concat([pf_day, pf_ngt], ignore_index=True)
    pf["home_key"] = pf["home_team"].map(_norm)
    return pf[["home_key", "time_of_day", "Park Factor"]].drop_duplicates(["home_key", "time_of_day"])

def _attach_home_team_and_tod(batters: pd.DataFrame,
                              sched: pd.DataFrame,
                              tod: pd.DataFrame) -> pd.DataFrame:
    _require(batters, ["team", "woba", "game_id", "date"], "batters input")
    # bring home team for this game_id
    x = batters.merge(sched[["game_id", "home_team", "home_key"]], on="game_id", how="left")
    # join time_of_day by home_key
    x = x.merge(tod, on="home_key", how="left")
    return x

def _apply_park(df: pd.DataFrame, pf: pd.DataFrame) -> pd.DataFrame:
    # join park factor by (home_key, time_of_day)
    df = df.drop(columns=["Park Factor"], errors="ignore")
    df = df.merge(pf, on=["home_key", "time_of_day"], how="left")
    # adjust
    df["adj_woba_park"] = df["woba"]
    pf_num = pd.to_numeric(df["Park Factor"], errors="coerce")
    mask = pf_num.notna()
    df.loc[mask, "adj_woba_park"] = df.loc[mask, "woba"] * pf_num
    return df

def _write_log(df: pd.DataFrame, path: str) -> None:
    if all(c in df.columns for c in ["last_name, first_name", "team", "adj_woba_park"]):
        top5 = df.sort_values("adj_woba_park", ascending=False).head(5)
        with open(path, "w") as f:
            for _, r in top5.iterrows():
                f.write(f"{r['last_name, first_name']} - {r['team']} - {r['adj_woba_park']:.3f}\n")

def main() -> None:
    sched = _load_schedule()
    tod   = _load_time_of_day()
    pf    = _load_park_factors()

    bh = pd.read_csv(BATTERS_HOME)
    ba = pd.read_csv(BATTERS_AWAY)

    # attach home team + time_of_day using game_id
    bh2 = _attach_home_team_and_tod(bh, sched, tod)
    ba2 = _attach_home_team_and_tod(ba, sched, tod)

    # apply park via home_key + time_of_day
    bh3 = _apply_park(bh2, pf)
    ba3 = _apply_park(ba2, pf)

    # keep only mapped games
    bh3 = bh3[bh3["game_id"].notna()].reset_index(drop=True)
    ba3 = ba3[ba3["game_id"].notna()].reset_index(drop=True)

    Path(OUT_HOME).parent.mkdir(parents=True, exist_ok=True)
    bh3.to_csv(OUT_HOME, index=False)
    ba3.to_csv(OUT_AWAY, index=False)

    _write_log(bh3, LOG_HOME)
    _write_log(ba3, LOG_AWAY)

if __name__ == "__main__":
    main()
