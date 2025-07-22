import pandas as pd

BATTERS_FILE = "data/cleaned/batters_today.csv"
TEAM_MASTER_FILE = "data/Data/team_name_master.csv"
OUTPUT_FILE = "data/cleaned/batters_today.csv"

def main():
    print("📥 Loading batters_today and team_name_master...")
    batters = pd.read_csv(BATTERS_FILE)
    team_map = pd.read_csv(TEAM_MASTER_FILE)

    if 'team' not in batters.columns:
        raise ValueError("Missing 'team' column in batters_today.csv.")
    if 'team_code' not in team_map.columns or 'team_name' not in team_map.columns:
        raise ValueError("Missing 'team_code' or 'team_name' in team_name_master.csv.")

    team_dict = dict(zip(team_map['team_code'], team_map['team_name']))

    print("🔁 Replacing team codes with official team names...")
    batters['team'] = batters['team'].map(team_dict).fillna(batters['team'])

    print(f"✅ Normalized teams in {len(batters)} rows. Saving to {OUTPUT_FILE}...")
    batters.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    main()
