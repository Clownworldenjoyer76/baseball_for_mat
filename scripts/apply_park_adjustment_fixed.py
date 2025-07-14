import pandas as pd
from pathlib import Path
import subprocess

def load_park_factors():
    day = pd.read_csv("data/Data/park_factors_day.csv")
    night = pd.read_csv("data/Data/park_factors_night.csv")
    return day, night

def determine_day_night(game_time_str):
    try:
        hour = int(game_time_str.split(":")[0])
        return "day" if hour < 18 else "night"
    except Exception:
        return "day"

def assign_stadium(batters, games, label):
    opponent_key = "home_team" if label == "away" else "away_team"
    merged = pd.merge(batters, games, left_on="team", right_on=opponent_key, how="left")
    merged["time_of_day"] = merged["game_time"].apply(determine_day_night)
    return merged

def apply_park_adjustments(batters, park_factors):
    if "woba" not in batters.columns:
        batters["woba"] = 0.320

    batters = pd.merge(batters, park_factors, on="stadium", how="left")
    if "park_factor" in batters.columns:
        batters["adj_woba_park"] = batters["woba"] * batters["park_factor"]
    else:
        batters["adj_woba_park"] = batters["woba"]
    return batters

def save_outputs(batters, label):
    out_path = Path("data/adjusted")
    out_path.mkdir(parents=True, exist_ok=True)
    outfile = out_path / f"batters_{label}_park.csv"
    logfile = out_path / f"log_park_{label}.txt"
    batters.to_csv(outfile, index=False)
    with open(logfile, "w") as f:
        f.write(str(batters[["last_name, first_name", "team", "adj_woba_park"]].head()))

def commit_outputs():
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", "data/adjusted/*.csv", "data/adjusted/*.txt"], check=True)
        subprocess.run(["git", "commit", "-m", "Auto-commit: park adjusted batters + log"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Committed and pushed adjusted park files.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit failed: {e}")

def main():
    games = pd.read_csv("data/raw/todaysgames_normalized.csv")
    day_pf, night_pf = load_park_factors()

    for label in ["home", "away"]:
        batters = pd.read_csv(f"data/adjusted/batters_{label}_adjusted.csv")
        batters = assign_stadium(batters, games, label)

        # Apply correct park factors
        for idx, row in batters.iterrows():
            pf_source = day_pf if row["time_of_day"] == "day" else night_pf
            stadium = row["stadium"]
            match = pf_source[pf_source["stadium"] == stadium]
            batters.loc[idx, "park_factor"] = match["park_factor"].values[0] if not match.empty else 1.0

        adjusted = apply_park_adjustments(batters, pd.DataFrame(batters[["stadium", "park_factor"]].drop_duplicates()))
        save_outputs(adjusted, label)

    commit_outputs()

if __name__ == "__main__":
    main()
