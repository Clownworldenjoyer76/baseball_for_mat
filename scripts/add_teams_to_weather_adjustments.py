import pandas as pd

WEATHER_ADJ_FILE = "data/weather_adjustments.csv"
TEAMS_FILE = "data/weather_teams.csv"
OUTPUT_FILE = WEATHER_ADJ_FILE  # overwrite in-place

def main():
    weather_df = pd.read_csv(WEATHER_ADJ_FILE)
    teams_df = pd.read_csv(TEAMS_FILE)

    # Ensure all string matching
    weather_df["stadium"] = weather_df["stadium"].astype(str).str.strip().str.lower()
    teams_df["home_team"] = teams_df["home_team"].astype(str).str.strip().str.lower()
    teams_df["away_team"] = teams_df["away_team"].astype(str).str.strip().str.lower()

    # Merge: add away_team where stadium = home_team
    merged_df = weather_df.merge(
        teams_df,
        left_on="stadium",
        right_on="home_team",
        how="left"
    )

    # Optional: restore title case if needed
    merged_df["home_team"] = merged_df["home_team"].str.title()
    merged_df["away_team"] = merged_df["away_team"].str.title()

    # Overwrite the file
    merged_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Added home_team and away_team to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
