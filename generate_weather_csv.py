import pandas as pd

# File paths
PITCHERS_FILE = "data/daily/todays_pitchers.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
OUTPUT_FILE = "data/weather_input.csv"

def normalize(df, column):
    df[column] = df[column].astype(str).str.strip().str.lower()
    return df

def main():
    tp = pd.read_csv(PITCHERS_FILE)
    sm = pd.read_csv(STADIUM_FILE)

    tp = normalize(tp, "home_team")
    sm = normalize(sm, "home_team")

    if "game_time" not in tp.columns:
        print("Missing 'game_time' in pitchers file.")
        return

    # Select required columns
    tp_trimmed = tp[["home_team", "game_time"]]
    sm_trimmed = sm[[
        "home_team", "venue", "city", "state", "timezone", "is_dome", "latitude", "longitude"
    ]]

    # Merge only on home_team
    combined = pd.merge(tp_trimmed, sm_trimmed, on="home_team", how="inner")

    combined.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Created: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()