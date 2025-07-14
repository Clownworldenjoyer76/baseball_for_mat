
import pandas as pd
from datetime import datetime
import os

def log(message):
    print(f"[LOG] {message}")

def load_games():
    log("Loading games from CSV...")
    games = pd.read_csv("data/raw/todaysgames_normalized.csv")
    log(f"Games loaded: {len(games)} rows")
    games["hour"] = pd.to_datetime(games["game_time"], errors='coerce').dt.hour
    games["time_of_day"] = games["hour"].apply(lambda x: "day" if x < 18 else "night")
    log("Added time_of_day column based on game_time")
    return games[["home_team", "time_of_day"]]

def load_park_factors(time_of_day):
    log(f"Loading park factors for {time_of_day} games...")
    if time_of_day == "day":
        df = pd.read_csv("data/Data/park_factors_day.csv")
    else:
        df = pd.read_csv("data/Data/park_factors_night.csv")
    log(f"Park factors loaded: {len(df)} rows")
    return df[["home_team", "Park Factor"]]

def apply_adjustment(batters_file, games, label):
    log(f"Applying park factor adjustment for: {label}")
    batters = pd.read_csv(batters_file)
    log(f"Loaded batters: {len(batters)} rows")

    opponent_map = games.set_index("home_team").to_dict()["time_of_day"]
    batters["opponent"] = batters["team"].map(opponent_map)
    log("Mapped opponent time_of_day")

    adjustments = []

    for time in ["day", "night"]:
        log(f"Processing {time} games...")
        park_factors = load_park_factors(time)
        merged = batters[batters["opponent"] == time].merge(
            park_factors, how="left", left_on="opponent", right_on="home_team"
        )
        log(f"Merged rows for {time}: {len(merged)}")
        if not merged.empty:
            merged["adj_woba_park"] = merged["woba"] * merged["Park Factor"]
            merged.drop(columns=["Park Factor", "home_team"], inplace=True)
            adjustments.append(merged)

    if adjustments:
        result = pd.concat(adjustments)
        log(f"Final result length: {len(result)}")
    else:
        log("No data after adjustments, creating dummy row")
        result = pd.DataFrame([{
            "last_name, first_name": "UNKNOWN",
            "team": "UNKNOWN",
            "adj_woba_park": 0.320
        }])

    os.makedirs("data/adjusted", exist_ok=True)
    log_file = f"data/adjusted/log_park_{label}.txt"
    output_file = f"data/adjusted/batters_{label}_park.csv"
    result.to_csv(output_file, index=False)
    log(f"Saved CSV to {output_file}")

    with open(log_file, "w") as logf:
        logf.write(f"Applied park factor adjustment to {len(result)} rows.\n")
        logf.write(f"Columns: {', '.join(result.columns)}\n")
        logf.write(f"Sample data:\n{result.head().to_string(index=False)}\n")
    log(f"Wrote log to {log_file}")

def main():
    log("Starting park adjustment script...")
    games = load_games()
    apply_adjustment("data/cleaned/batters_normalized_cleaned.csv", games, "home")
    apply_adjustment("data/cleaned/batters_normalized_cleaned.csv", games, "away")
    log("Script complete.")

if __name__ == "__main__":
    main()
