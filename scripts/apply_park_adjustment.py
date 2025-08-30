#!/usr/bin/env python3
# /home/runner/work/baseball_for_mat/baseball_for_mat/scripts/apply_park_adjustment.py
import pandas as pd
from pathlib import Path
import subprocess

# === Inputs ===
BATTERS_HOME = "data/adjusted/batters_home.csv"
BATTERS_AWAY = "data/adjusted/batters_away.csv"
SCHED_FILE   = "data/bets/mlb_sched.csv"
GAMES_FILE   = "data/raw/todaysgames_normalized.csv"
PF_DAY_FILE  = "data/Data/park_factors_day.csv"
PF_NGT_FILE  = "data/Data/park_factors_night.csv"

# === Outputs ===
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
    sched["home_team_key"] = sched["home_team"].map(_norm)
    sched["away_team_key"] = sched["away_team"].map(_norm)
    return sched

def _attach_game_id_side_agnostic(batters: pd.DataFrame, sched: pd.DataFrame) -> pd.DataFrame:
    _require(batters, ["team", "woba"], "batters")
    b = batters.copy()
    b["team_key"] = b["team"].map(_norm)

    m_home = b.merge(
        sched[["game_id", "date", "home_team_key"]],
        left_on="team_key", right_on="home_team_key", how="left"
    ).rename(columns={"game_id": "gid_home", "date": "date_home"}).drop(columns=["home_team_key"])

    m_away = b.merge(
        sched[["game_id", "date", "away_team_key"]],
        left_on="team_key", right_on="away_team_key", how="left"
    ).rename(columns={"game_id": "gid_away", "date": "date_away"}).drop(columns=["away_team_key"])

    b = b.join(m_home[["gid_home", "date_home"]]).join(m_away[["gid_away", "date_away"]])
    b["game_id"] = b["gid_home"].where(b["gid_home"].notna(), b["gid_away"])
    b["date"]    = b["date_home"].where(b["date_home"].notna(), b["date_away"])
    b = b.drop(columns=["gid_home", "date_home", "gid_away", "date_away"])

    # Bring home_team (for park) via game_id
    b = b.merge(
        sched[["game_id", "home_team", "home_team_key"]],
        on="game_id", how="left"
    )
    return b

def _load_time_of_day() -> pd.DataFrame:
    games = pd.read_csv(GAMES_FILE, dtype=str)
    _require(games, ["home_team", "game_time"], GAMES_FILE)
    games["home_team_key"] = games["home_team"].map(_norm)
    # Parse time; classify <18 as day else night
    ts = pd.to_datetime(games["game_time"], format="%I:%M %p", errors="coerce")
    games["hour"] = ts.dt.hour
    games["time_of_day"] = games["hour"].apply(lambda x: "day" if (pd.notna(x) and x < 18) else "night")
    return games[["home_team_key", "time_of_day"]].drop_duplicates("home_team_key")

def _load_park_factors() -> pd.DataFrame:
    pf_day = pd.read_csv(PF_DAY_FILE)
    pf_ngt = pd.read_csv(PF_NGT_FILE)
    _require(pf_day, ["home_team", "Park Factor"], PF_DAY_FILE)
    _require(pf_ngt, ["home_team", "Park Factor"], PF_NGT_FILE)

    pf_day = pf_day.assign(time_of_day="day")
    pf_ngt = pf_ngt.assign(time_of_day="night")
    pf = pd.concat([pf_day, pf_ngt], ignore_index=True)
    pf["home_team_key"] = pf["home_team"].map(_norm)
    pf = pf[["home_team_key", "time_of_day", "Park Factor"]].drop_duplicates(["home_team_key","time_of_day"])
    return pf

def _apply_park(b_with_gid: pd.DataFrame, tod: pd.DataFrame, pf: pd.DataFrame) -> pd.DataFrame:
    df = b_with_gid.copy()
    # Join time_of_day by home team (from schedule via game_id)
    df = df.merge(tod, on="home_team_key", how="left")
    # Join park factor by (home_team_key, time_of_day)
    df = df.drop(columns=["Park Factor"], errors="ignore")
    df = df.merge(pf, on=["home_team_key", "time_of_day"], how="left")

    # Adjust
    _require(df, ["woba"], "batters for park adjustment")
    df["adj_woba_park"] = df["woba"]
    pf_num = pd.to_numeric(df["Park Factor"], errors="coerce")
    has_pf = pf_num.notna()
    df.loc[has_pf, "adj_woba_park"] = df.loc[has_pf, "woba"] * pf_num

    return df

def _finalize(df: pd.DataFrame) -> pd.DataFrame:
    # Keep only rows mapped to a game
    df = df[df["game_id"].notna()].reset_index(drop=True)
    return df

def _write_log(df: pd.DataFrame, path: str) -> None:
    if all(c in df.columns for c in ["last_name, first_name", "team", "adj_woba_park"]):
        top5 = df.sort_values("adj_woba_park", ascending=False).head(5)
        with open(path, "w") as f:
            for _, r in top5.iterrows():
                f.write(f"{r['last_name, first_name']} - {r['team']} - {r['adj_woba_park']:.3f}\n")

def _commit_outputs():
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", "data/adjusted/*.csv", "data/adjusted/*.txt"], check=True)
        subprocess.run(["git", "commit", "--allow-empty", "-m", "Update adjusted park batters"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Committed adjusted park files.")
    except subprocess.CalledProcessError:
        print("⚠️ Git commit/push skipped.")

def main():
    # Load references
    sched = _load_schedule()
    tod   = _load_time_of_day()
    pf    = _load_park_factors()

    # Home
    bat_home = pd.read_csv(BATTERS_HOME)
    home_gid = _attach_game_id_side_agnostic(bat_home, sched)
    home_adj = _apply_park(home_gid, tod, pf)
    home_out = _finalize(home_adj)
    Path(OUT_HOME).parent.mkdir(parents=True, exist_ok=True)
    home_out.to_csv(OUT_HOME, index=False)
    _write_log(home_out, LOG_HOME)

    # Away
    bat_away = pd.read_csv(BATTERS_AWAY)
    away_gid = _attach_game_id_side_agnostic(bat_away, sched)
    away_adj = _apply_park(away_gid, tod, pf)
    away_out = _finalize(away_adj)
    away_out.to_csv(OUT_AWAY, index=False)
    _write_log(away_out, LOG_AWAY)

    _commit_outputs()

if __name__ == "__main__":
    main()
