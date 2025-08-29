#!/usr/bin/env python3
# scripts/apply_park_adjustment.py
import pandas as pd
from pathlib import Path
import subprocess

SCHED_FILE = "data/bets/mlb_sched.csv"

def _norm(s: str) -> str:
    return str(s).strip().lower()

def _scheduled_teams() -> set[str]:
    sched = pd.read_csv(SCHED_FILE, dtype=str)
    if not {"home_team","away_team"}.issubset(sched.columns):
        raise ValueError(f"{SCHED_FILE}: missing columns ['home_team','away_team']")
    teams = pd.concat([sched["home_team"], sched["away_team"]], ignore_index=True).dropna().map(_norm).unique()
    return set(teams)

def load_game_times():
    games = pd.read_csv("data/raw/todaysgames_normalized.csv", dtype=str)
    games["hour"] = pd.to_datetime(games["game_time"], format="%I:%M %p", errors="coerce").dt.hour
    games["time_of_day"] = games["hour"].apply(lambda x: "day" if (pd.notna(x) and x < 18) else "night")
    return games[["home_team", "time_of_day"]]

def load_park_factors(time_of_day):
    path = f"data/Data/park_factors_{time_of_day}.csv"
    return pd.read_csv(path)[["home_team", "Park Factor"]]

def apply_park_adjustments(batters: pd.DataFrame, games: pd.DataFrame, label: str) -> pd.DataFrame:
    if "team" not in batters.columns:
        raise ValueError("Missing 'team' column in batters input")
    if label not in ("home","away"):
        raise ValueError("label must be 'home' or 'away'")

    g = games.copy()
    g["home_team_key"] = g["home_team"].map(_norm)

    df = batters.copy()
    df["team_key"] = df["team"].map(_norm)

    if label == "home":
        df["home_team_key"] = df["team_key"]
    else:
        if "opponent" in df.columns:
            df["home_team_key"] = df["opponent"].map(_norm)
        else:
            df["home_team_key"] = pd.NA

    df = df.merge(g[["home_team_key","time_of_day"]], on="home_team_key", how="left")

    if "Park Factor" not in df.columns:
        df["Park Factor"] = pd.NA

    for tod in ("day","night"):
        pf = load_park_factors(tod)
        pf["home_team_key"] = pf["home_team"].map(_norm)
        pf = pf[["home_team_key","Park Factor"]]
        mask = df["time_of_day"].eq(tod)
        if mask.any():
            tmp = df.loc[mask, ["home_team_key"]].merge(pf, on="home_team_key", how="left")
            df.loc[mask, "Park Factor"] = tmp["Park Factor"].values

    if "woba" not in df.columns:
        raise ValueError("Missing 'woba' column in batters input")
    df["adj_woba_park"] = df["woba"]
    pf_num = pd.to_numeric(df["Park Factor"], errors="coerce")
    df.loc[pf_num.notna(), "adj_woba_park"] = df.loc[pf_num.notna(), "woba"] * pf_num

    return df

def save_outputs(batters: pd.DataFrame, label: str):
    out_path = Path("data/adjusted")
    out_path.mkdir(parents=True, exist_ok=True)
    outfile = out_path / f"batters_{label}_park.csv"
    logfile = out_path / f"log_park_{label}.txt"

    batters.to_csv(outfile, index=False)

    if all(c in batters.columns for c in ["last_name, first_name","team","adj_woba_park"]):
        top5 = batters[["last_name, first_name", "team", "adj_woba_park"]].sort_values(by="adj_woba_park", ascending=False).head(5)
        with open(logfile, "w") as f:
            f.write(f"Top 5 adjusted batters ({label}):\n")
            f.write(top5.to_string(index=False))

def commit_outputs():
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", "data/adjusted/*.csv", "data/adjusted/*.txt"], check=True)
        subprocess.run(["git", "commit", "--allow-empty", "-m", "Force commit: adjusted park batters + log"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Committed and pushed adjusted park files.")
    except subprocess.CalledProcessError:
        print("⚠️ Git commit/push skipped or failed.")

def main():
    games = load_game_times()
    sched_teams = _scheduled_teams()

    batters_home = pd.read_csv("data/adjusted/batters_home.csv")
    adjusted_home = apply_park_adjustments(batters_home, games, "home")
    adjusted_home = adjusted_home[adjusted_home["team"].map(_norm).isin(sched_teams)].reset_index(drop=True)
    print("Home batters adjusted:", len(adjusted_home))
    save_outputs(adjusted_home, "home")

    batters_away = pd.read_csv("data/adjusted/batters_away.csv")
    adjusted_away = apply_park_adjustments(batters_away, games, "away")
    adjusted_away = adjusted_away[adjusted_away["team"].map(_norm).isin(sched_teams)].reset_index(drop=True)
    print("Away batters adjusted:", len(adjusted_away))
    save_outputs(adjusted_away, "away")

    commit_outputs()

if __name__ == "__main__":
    main()
