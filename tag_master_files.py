
import pandas as pd
import os

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    return name.strip().lower().replace(".", "").replace(",", "").replace("jr", "").replace("ii", "").replace("iii", "")

def tag_file(input_path, player_type, team_map, output_tagged, output_unmatched):
    df = pd.read_csv(input_path)
    df["normalized_name"] = df["last_name, first_name"].apply(normalize_name)

    matched = []
    unmatched = []

    for _, row in df.iterrows():
        name = row["normalized_name"]
        match = team_map.get(name)
        if match and match["type"] == player_type:
            row["team"] = match["team"]
            matched.append(row.drop(labels=["normalized_name"]))
        else:
            unmatched.append(row.drop(labels=["normalized_name"]))

    tagged_df = pd.DataFrame(matched)
    unmatched_df = pd.DataFrame(unmatched)

    tagged_df.to_csv(output_tagged, index=False)
    unmatched_df.to_csv(output_unmatched, index=False)

    return len(df), len(tagged_df), len(unmatched_df)

def write_totals(bat_total, bat_tagged, bat_unmatched, pitch_total, pitch_tagged, pitch_unmatched, output_file):
    output = f"""Total batters in CSV: {bat_total}
Total pitchers in CSV: {pitch_total}

✅ batters_tagged.csv created with {bat_tagged} rows
⚠️ Unmatched batters (missing team): {bat_unmatched} written to unmatched_batters.csv

✅ pitchers_tagged.csv created with {pitch_tagged} rows
⚠️ Unmatched pitchers (missing team): {pitch_unmatched} written to unmatched_pitchers.csv
"""
    print(output)
    with open(output_file, "w") as f:
        f.write(output)

def main():
    os.makedirs("data/tagged", exist_ok=True)
    os.makedirs("data/output", exist_ok=True)

    master_df = pd.read_csv("data/processed/player_team_master.csv")
    master_df["normalized_name"] = master_df["name"].apply(normalize_name)
    team_map = {
        row["normalized_name"]: {"team": row["team"], "type": row["type"]}
        for _, row in master_df.iterrows()
    }

    bat_total, bat_tagged, bat_unmatched = tag_file(
        "data/master/batters.csv",
        "batter",
        team_map,
        "data/tagged/batters_tagged.csv",
        "data/output/unmatched_batters.csv"
    )

    pitch_total, pitch_tagged, pitch_unmatched = tag_file(
        "data/master/pitchers.csv",
        "pitcher",
        team_map,
        "data/tagged/pitchers_tagged.csv",
        "data/output/unmatched_pitchers.csv"
    )

    write_totals(
        bat_total, bat_tagged, bat_unmatched,
        pitch_total, pitch_tagged, pitch_unmatched,
        "data/output/player_totals.txt"
    )

if __name__ == "__main__":
    main()
