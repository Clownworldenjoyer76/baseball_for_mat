import os
import pandas as pd

csv_folder = "data/team_csvs"
output_file = "data/processed/player_team_master.csv"

rows = []

for filename in os.listdir(csv_folder):
    file_path = os.path.join(csv_folder, filename)
    if not filename.endswith(".csv"):
        continue

    if filename.startswith("batters_"):
        team = filename.replace("batters_", "").replace(".csv", "")
        df = pd.read_csv(file_path)
        if "name" in df.columns:
            for name in df["name"].dropna():
                rows.append({"name": name.strip(), "team": team, "type": "batter"})

    elif filename.startswith("pitchers_"):
        team = filename.replace("pitchers_", "").replace(".csv", "")
        df = pd.read_csv(file_path)
        if "last_name, first_name" in df.columns:
            for name in df["last_name, first_name"].dropna():
                rows.append({"name": name.strip(), "team": team, "type": "pitcher"})

master_df = pd.DataFrame(rows)
master_df = master_df.sort_values(["team", "type", "name"])
os.makedirs("data/processed", exist_ok=True)
master_df.to_csv(output_file, index=False)
