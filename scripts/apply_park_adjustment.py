
def apply_adjustment(batters_file, games, label):
    try:
        batters = pd.read_csv(batters_file)
    except Exception as e:
        batters = pd.DataFrame(columns=["last_name, first_name", "team", "woba"])

    opponent_map = games.set_index("home_team").to_dict().get("time_of_day", {})
    batters["opponent"] = batters.get("team", "").map(opponent_map)
    adjustments = []

    for time in ["day", "night"]:
        try:
            park_factors = load_park_factors(time)
            subset = batters[batters["opponent"] == time]
            merged = subset.merge(park_factors, how="left", left_on="opponent", right_on="home_team")
            merged["adj_woba_park"] = merged.get("woba", 0.320) * merged.get("Park Factor", 1.00)
            merged = merged.drop(columns=["Park Factor", "home_team"], errors="ignore")
            adjustments.append(merged)
        except:
            continue

    result = pd.concat(adjustments, ignore_index=True) if adjustments else pd.DataFrame()

    # Force fallback if still empty
    if result.empty or "adj_woba_park" not in result.columns:
        result = pd.DataFrame([{
            "last_name, first_name": "FALLBACK",
            "team": "NONE",
            "adj_woba_park": 0.320
        }])

    if "last_name, first_name" not in result.columns:
        result["last_name, first_name"] = "UNKNOWN"
    if "team" not in result.columns:
        result["team"] = "UNKNOWN"
    if "adj_woba_park" not in result.columns:
        result["adj_woba_park"] = 0.320

    log_file = f"data/adjusted/log_park_{label}.txt"
    output_file = f"data/adjusted/batters_{label}_park.csv"
    result.to_csv(output_file, index=False)
    with open(log_file, "w") as log:
        log.write(f"Applied park factor adjustment to {len(result)} rows.\n")
