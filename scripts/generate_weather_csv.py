import pandas as pd
from pathlib import Path

GAMES_FILE = "data/raw/todaysgames_normalized.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
OUTPUT_FILE = "data/weather_input.csv"
SUMMARY_FILE = "data/weather_summary.txt"

def generate_weather_csv():
    try:
        games_df = pd.read_csv(GAMES_FILE)
        stadium_df = pd.read_csv(STADIUM_FILE)
    except FileNotFoundError as e:
        print(f"❌ File not found: {e}")
        return
    except Exception as e:
        print(f"❌ Error reading input files: {e}")
        return

    # Normalize team name casing for matching
    games_df['home_team'] = games_df['home_team'].str.upper()
    stadium_df['home_team'] = stadium_df['home_team'].str.upper()

    merged = pd.merge(games_df, stadium_df, on='home_team', how='left')

    if merged.empty:
        print("❌ Merge failed: No matching rows.")
        return

    merged.to_csv(OUTPUT_FILE, index=False)

    summary = (
        f"✅ Weather input file generated\n"
        f"🔢 Rows: {len(merged)}\n"
        f"📁 Output: {OUTPUT_FILE}\n"
        f"📄 Games file: {GAMES_FILE}\n"
        f"🏟️ Stadium file: {STADIUM_FILE}"
    )

    print(summary)
    Path(SUMMARY_FILE).write_text(summary)

if __name__ == "__main__":
    generate_weather_csv()
