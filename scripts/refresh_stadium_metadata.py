import pandas as pd

# File paths
MASTER_PATH = "data/Data/stadium_master.csv"
GAMES_PATH = "data/raw/todaysgames_normalized.csv"
OUTPUT_PATH = "data/Data/stadium_metadata.csv"

def refresh_stadium_metadata():
    # Load full stadium reference
    master_df = pd.read_csv(MASTER_PATH)
    if "team_name" not in master_df.columns:
        raise ValueError("Missing 'team_name' column in stadium_master.csv")

    # Load today's games
    games_df = pd.read_csv(GAMES_PATH)
    if "home_team" not in games_df.columns:
        raise ValueError("Missing 'home_team' column in todaysgames_normalized.csv")

    # Get list of home teams playing today
    home_teams_today = games_df["home_team"].dropna().unique()

    # Filter master to only today's home teams
    filtered_df = master_df[master_df["team_name"].isin(home_teams_today)].copy()

    # Overwrite stadium_metadata.csv
    filtered_df.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Saved updated stadium metadata for {len(filtered_df)} home teams to {OUTPUT_PATH}")

if __name__ == "__main__":
    refresh_stadium_metadata()
