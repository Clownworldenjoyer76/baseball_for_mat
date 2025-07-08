import pandas as pd
from pathlib import Path

def normalize_name(name):
    return name.strip().lower()

def tag_file(input_path, output_path, unmatched_path, player_type, player_map):
    df = pd.read_csv(input_path)

    df["lookup_name"] = df["last_name, first_name"].apply(normalize_name)

    # Ensure player_map is indexed by normalized name
    player_map = player_map.copy()
    player_map["lookup_name"] = player_map["name"].apply(normalize_name)
    player_map = player_map.set_index("lookup_name")

    df["team"] = df["lookup_name"].map(player_map["team"])
    df["type"] = player_type

    matched = df[df["team"].notna()].drop(columns=["lookup_name"])
    unmatched = df[df["team"].isna()].drop(columns=["team", "type", "lookup_name"])

    matched.to_csv(output_path, index=False)
    unmatched.to_csv(unmatched_path, index=False)

    return len(df), len(matched), len(unmatched)

def main():
    batter_input = "data/master/batters.csv"
    pitcher_input = "data/master/pitchers.csv"
    batter_output = "data/tagged/batters_tagged.csv"
    pitcher_output = "data/tagged/pitchers_tagged.csv"
    unmatched_batters = "data/output/unmatched_batters.csv"
    unmatched_pitchers = "data/output/unmatched_pitchers.csv"
    totals_path = "data/output/player_totals.txt"

    # Clear previous unmatched files
    Path(unmatched_batters).write_text("")
    Path(unmatched_pitchers).write_text("")

    player_map = pd.read_csv("data/processed/player_team_master.csv")[["name", "team"]].drop_duplicates()

    total_batters, matched_batters, unmatched_batters_count = tag_file(
        batter_input, batter_output, unmatched_batters, "batter", player_map
    )
    total_pitchers, matched_pitchers, unmatched_pitchers_count = tag_file(
        pitcher_input, pitcher_output, unmatched_pitchers, "pitcher", player_map
    )

    with open(totals_path, "w") as f:
        f.write(f"Batters total: {total_batters}, matched: {matched_batters}, unmatched: {unmatched_batters_count}\n")
        f.write(f"Pitchers total: {total_pitchers}, matched: {matched_pitchers}, unmatched: {unmatched_pitchers_count}\n")

if __name__ == "__main__":
    main()
