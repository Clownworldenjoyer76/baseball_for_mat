import pandas as pd
import os

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    return name.lower().replace(".", "").replace(",", "").replace("jr", "").replace("ii", "").replace("iii", "").strip()

def load_team_map(file_path):
    return pd.read_csv(file_path).set_index("name")["team"].to_dict()

def tag_players(df, col_name, team_map, output_path, unmatched_path):
    df["normalized_name"] = df[col_name].apply(normalize_name)
    matched_teams = []
    unmatched_rows = []

    for _, row in df.iterrows():
        name = row["normalized_name"]
        if name in team_map:
            matched_teams.append(team_map[name])
        else:
            # Fuzzy matching removed for performance
            matched_teams.append(None)
            unmatched_rows.append(row)

    df["team"] = matched_teams
    matched_df = df[df["team"].notna()]
    unmatched_df = pd.DataFrame(unmatched_rows)

    matched_df.drop(columns=["normalized_name"], inplace=True)
    matched_df.to_csv(output_path, index=False)
    unmatched_df.to_csv(unmatched_path, index=False)

    return len(unmatched_df)

def write_totals(batter_input, pitcher_input, bat_count, pitch_count):
    total_batters = len(pd.read_csv(batter_input))
    total_pitchers = len(pd.read_csv(pitcher_input))

    output = f"""Total batters in CSV: {total_batters}
Total pitchers in CSV: {total_pitchers}

✅ batters_tagged.csv created with {total_batters - bat_count} rows
⚠️ Unmatched batters (missing team): {bat_count} written to unmatched_batters.csv

✅ pitchers_tagged.csv created with {total_pitchers - pitch_count} rows
⚠️ Unmatched pitchers (missing team): {pitch_count} written to unmatched_pitchers.csv
"""
    print(output)
    with open("data/output/player_totals.txt", "w") as f:
        f.write(output)

def main():
    try:
        os.makedirs("data/tagged", exist_ok=True)
        os.makedirs("data/output", exist_ok=True)

        team_map = load_team_map("data/Data/team_name_map.csv")

        batter_input = "data/master/batters.csv"
        pitcher_input = "data/master/pitchers.csv"

        bat_count = tag_players(
            pd.read_csv(batter_input),
            "name",
            team_map,
            "data/tagged/batters_tagged.csv",
            "data/output/unmatched_batters.csv"
        )

        pitch_count = tag_players(
            pd.read_csv(pitcher_input),
            "name",
            team_map,
            "data/tagged/pitchers_tagged.csv",
            "data/output/unmatched_pitchers.csv"
        )

        write_totals(batter_input, pitcher_input, bat_count, pitch_count)
    except Exception as e:
        print("🔥 ERROR:", e)
        raise

if __name__ == "__main__":
    main()
