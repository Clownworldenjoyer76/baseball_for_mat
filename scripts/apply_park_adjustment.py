def apply_adjustment(batters_file, games, label):
    batters = pd.read_csv(batters_file)
    opponent_map = games.set_index("home_team").to_dict()["time_of_day"]

    batters["opponent"] = batters["team"].map(opponent_map)
    adjustments = []

    for time in ["day", "night"]:
        park_factors = load_park_factors(time)
        merged = batters[batters["opponent"] == time].merge(park_factors, how="left", left_on="opponent", right_on="home_team")
        merged["adj_woba_park"] = merged["woba"] * merged["Park Factor"]
        adjustments.append(merged.drop(columns=["Park Factor", "home_team"]))

    result = pd.concat(adjustments)

    if "adj_woba_park" not in result.columns:
        result["adj_woba_park"] = 0.320
    for col in ["last_name, first_name", "team"]:
        if col not in result.columns:
            result[col] = "UNKNOWN"

    log_file = f"data/adjusted/log_park_{label}.txt"
    output_file = f"data/adjusted/batters_{label}_park.csv"
    result.to_csv(output_file, index=False)
    with open(log_file, "w") as log:
        log.write(f"Applied park factor adjustment to {len(result)} rows.\n")
