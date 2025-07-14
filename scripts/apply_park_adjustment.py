
import pandas as pd
from datetime import datetime

def load_games():
    games = pd.read_csv("data/raw/todaysgames_normalized.csv")
    games["hour"] = pd.to_datetime(games["game_time"]).dt.hour
    games["time_of_day"] = games["hour"].apply(lambda x: "day" if x < 18 else "night")
    return games[["home_team", "time_of_day"]]

def load_park_factors(time_of_day):
    if time_of_day == "day":
        df = pd.read_csv("data/Data/park_factors_day.csv")
    else:
        df = pd.read_csv("data/Data/park_factors_night.csv")
    return df[["home_team", "Park Factor"]]

def apply_adjustment(batters_file, games, label):
    batters = pd.read_csv(batters_file)
    opponent_map = games.set_index("home_team").to_dict()["time_of_day"]

    batters["opponent"] = batters["team"].map(opponent_map)
    adjustments = []

    for time in ["day", "night"]:
        park_factors = load_park_factors(time)
        merged = batters[batters["opponent"] == time].merge(park_factors, how="left", left_on="opponent", right_on="home_team")
        merged["adj_woba_park"] = merged["woba"] * merged["Park Factor"]
        merged["adj_woba_park"] = merged["adj_woba_park"].fillna(merged["woba"])  # fallback
        adjustments.append(merged.drop(columns=["Park Factor", "home_team"]))

    result = pd.concat(adjustments)
    result["adj_woba_park"] = result["adj_woba_park"].fillna(0.320)

    for col in ["last_name, first_name", "team"]:
        if col not in result.columns:
            result[col] = "UNKNOWN"

    log_file = f"data/adjusted/log_park_{label}.txt"
    output_file = f"data/adjusted/batters_{label}_park.csv"
    result.to_csv(output_file, index=False)
    with open(log_file, "w") as log:
        log.write(f"Applied park factor adjustment to {len(result)} rows.\n")

def main():
    games = load_games()
    apply_adjustment("data/cleaned/batters_normalized_cleaned.csv", games, "home")
    apply_adjustment("data/cleaned/batters_normalized_cleaned.csv", games, "away")

if __name__ == "__main__":
    main()
