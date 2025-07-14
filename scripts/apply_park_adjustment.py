def apply_adjustment(batters_file, games, label):
    import os
    import pandas as pd

    # Load batters
    batters = pd.read_csv(batters_file)
    if "team" not in batters.columns:
        print("❌ 'team' column missing in batters file.")
        return

    # Create mapping: team → opponent team
    game_map = games.set_index("home_team").to_dict()
    team_to_opp = {row["away_team"]: row["home_team"] for _, row in games.iterrows()}
    team_to_time = games.set_index("home_team")["time_of_day"].to_dict()

    # Map each batter's opponent and the corresponding time of day
    batters["opponent"] = batters["team"].map(team_to_opp)
    batters["time_of_day"] = batters["opponent"].map(team_to_time)

    # Separate by time of day
    day_batters = batters[batters["time_of_day"] == "day"]
    night_batters = batters[batters["time_of_day"] == "night"]

    adjusted = []

    for df, tod in [(day_batters, "day"), (night_batters, "night")]:
        park_factors = pd.read_csv(f"data/Data/park_factors_{tod}.csv")[["home_team", "Park Factor"]]
        merged = df.merge(park_factors, how="left", left_on="opponent", right_on="home_team")
        merged["adj_woba_park"] = merged["woba"] * merged["Park Factor"]
        adjusted.append(merged.drop(columns=["Park Factor", "home_team"]))

    result = pd.concat(adjusted, ignore_index=True)

    # Fallback columns if merge fails
    if "adj_woba_park" not in result.columns:
        result["adj_woba_park"] = 0.320
    if "last_name, first_name" not in result.columns:
        result["last_name, first_name"] = "UNKNOWN"

    # Output
    os.makedirs("data/adjusted", exist_ok=True)
    output_file = f"data/adjusted/batters_{label}_park.csv"
    log_file = f"data/adjusted/log_park_{label}.txt"
    result.to_csv(output_file, index=False)
    with open(log_file, "w") as f:
        f.write(f"Applied park factor adjustment to {len(result)} rows.\n")

    print(f"✅ Done: {output_file}")