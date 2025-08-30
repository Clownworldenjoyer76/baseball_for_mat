#!/usr/bin/env python3
# /home/runner/work/baseball_for_mat/baseball_for_mat/scripts/attach_game_ids.py
import pandas as pd
from pathlib import Path

# Inputs
BATTERS_HOME = "data/adjusted/batters_home.csv"
BATTERS_AWAY = "data/adjusted/batters_away.csv"
SCHED_FILE   = "data/bets/mlb_sched.csv"

# Outputs (overwrite inputs in place)
OUT_HOME = "data/adjusted/batters_home.csv"
OUT_AWAY = "data/adjusted/batters_away.csv"

def _norm(s: str) -> str:
    return str(s).strip().lower()

def _require(df: pd.DataFrame, cols: list[str], where: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{where}: missing columns {missing}")

def _load_schedule() -> pd.DataFrame:
    sched = pd.read_csv(SCHED_FILE, dtype=str)
    _require(sched, ["game_id", "date", "home_team", "away_team"], SCHED_FILE)
    sched["home_key"] = sched["home_team"].map(_norm)
    sched["away_key"] = sched["away_team"].map(_norm)
    return sched[["game_id", "date", "home_team", "away_team", "home_key", "away_key"]]

def _attach_gid(batters: pd.DataFrame, sched: pd.DataFrame, label: str) -> pd.DataFrame:
    _require(batters, ["team"], f"batters_{label}")
    b = batters.copy()
    b["team_key"] = b["team"].map(_norm)

    # Match as home
    m_home = b.merge(
        sched[["game_id", "date", "home_key"]],
        left_on="team_key", right_on="home_key", how="left"
    ).rename(columns={"game_id": "gid_home", "date": "date_home"}).drop(columns=["home_key"])

    # Match as away
    m_away = b.merge(
        sched[["game_id", "date", "away_key"]],
        left_on="team_key", right_on="away_key", how="left"
    ).rename(columns={"game_id": "gid_away", "date": "date_away"}).drop(columns=["away_key"])

    # Coalesce to single game_id/date
    b["game_id"] = m_home["gid_home"].where(m_home["gid_home"].notna(), m_away["gid_away"])
    b["date"]    = m_home["date_home"].where(m_home["date_home"].notna(), m_away["date_away"])

    # Drop rows not scheduled today (no game_id)
    before = len(b)
    b = b[b["game_id"].notna()].copy()
    b.drop(columns=["team_key"], inplace=True, errors="ignore")
    b.reset_index(drop=True, inplace=True)

    print(f"{label}: {before} → {len(b)} after attaching game_id/date (dropped {before-len(b)})")
    return b

def main() -> None:
    Path(OUT_HOME).parent.mkdir(parents=True, exist_ok=True)

    sched = _load_schedule()

    bh = pd.read_csv(BATTERS_HOME)
    bh = _attach_gid(bh, sched, "home")
    bh.to_csv(OUT_HOME, index=False)

    ba = pd.read_csv(BATTERS_AWAY)
    ba = _attach_gid(ba, sched, "away")
    ba.to_csv(OUT_AWAY, index=False)

if __name__ == "__main__":
    main()
