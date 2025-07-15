import os
import pandas as pd

files = {
    "batters": "data/tagged/batters_normalized.csv",
    "pitchers": "data/tagged/pitchers_normalized.csv"
}
output_dir = "data/cleaned"
os.makedirs(output_dir, exist_ok=True)

for label, path in files.items():
    if os.path.exists(path):
        df = pd.read_csv(path)
        before = len(df)
        df = df.drop_duplicates(subset=["last_name, first_name", "team", "type"])
        after = len(df)

        # Standardize team names using team_name_master.csv
        if label == "batters":
            print("🔗 Mapping team codes to standard team names...")
            try:
                team_map = pd.read_csv("data/Data/team_name_master.csv")
                team_map = team_map[['team_code', 'team_name']].dropna()
                team_map_dict = dict(zip(team_map['team_code'].str.strip(), team_map['team_name'].str.strip()))
                df['team'] = df['team'].astype(str).str.strip().map(team_map_dict)
            except Exception as e:
                print(f"⚠️ Failed to map team names: {e}")

        df.to_csv(f"{output_dir}/{label}_normalized_cleaned.csv", index=False)
        print(f"🧼 {label.capitalize()} deduplicated: {before} → {after}")
