import pandas as pd
import os

def load_lookup_table(path):
    df = pd.read_csv(path)
    df["name"] = df["name"].str.strip().str.lower()
    return df.set_index("name")[["team", "type"]]

def tag_file(input_path, output_path, unmatched_path, player_map, expected_type):
    df = pd.read_csv(input_path)
    df["lookup_name"] = df["last_name, first_name"].str.strip().str.lower()
    df["team"] = df["lookup_name"].map(player_map["team"])
    df["type"] = df["lookup_name"].map(player_map["type"])
    matched = df[df["team"].notna() & (df["type"] == expected_type)].drop(columns=["lookup_name"])
    unmatched = df[df["team"].isna() | (df["type"] != expected_type)].drop(columns=["lookup_name"])
    matched.to_csv(output_path, index=False)
    unmatched.to_csv(unmatched_path, index=False)
    return len(df), len(matched), len(unmatched)

def write_totals(total_batters, matched_batters, unmatched_batters, total_pitchers, matched_pitchers, unmatched_pitchers):
    os.makedirs("data/output", exist_ok=True)
    summary = f"""Total batters in CSV: {total_batters}
✅ batters_tagged.csv created with {matched_batters} rows
⚠️ Unmatched batters (missing team): {unmatched_batters} written to unmatched_batters.csv

Total pitchers in CSV: {total_pitchers}
✅ pitchers_tagged.csv created with {matched_pitchers} rows
⚠️ Unmatched pitchers (missing team): {unmatched_pitchers} written to unmatched_pitchers.csv
"""
    with open("data/output/player_totals.txt", "w") as f:
        f.write(summary)
    print(summary)

def main():
    os.makedirs("data/tagged", exist_ok=True)
    os.makedirs("data/output", exist_ok=True)

    lookup_path = "data/processed/player_team_master.csv"
    player_map = load_lookup_table(lookup_path)

    total_batters, matched_batters, unmatched_batters = tag_file(
        "data/master/batters.csv",
        "data/tagged/batters_tagged.csv",
        "data/tagged/unmatched_batters.csv",
        player_map,
        "batter"
    )

    total_pitchers, matched_pitchers, unmatched_pitchers = tag_file(
        "data/master/pitchers.csv",
        "data/tagged/pitchers_tagged.csv",
        "data/tagged/unmatched_pitchers.csv",
        player_map,
        "pitcher"
    )

    write_totals(total_batters, matched_batters, unmatched_batters, total_pitchers, matched_pitchers, unmatched_pitchers)

if __name__ == "__main__":
    main()
