import pandas as pd

LINEUPS_FILE = "data/raw/lineups_normalized.csv"
BATTERS_FILE = "data/cleaned/batters_normalized_cleaned.csv"
OUTPUT_FILE = "data/cleaned/batters_today.csv"

def main():
    lineups = pd.read_csv(LINEUPS_FILE)
    batters = pd.read_csv(BATTERS_FILE)

    playing_names = set(lineups['last_name, first_name'].str.strip())
    batters_today = batters[batters['last_name, first_name'].str.strip().isin(playing_names)]

    batters_today.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Filtered batters written to {OUTPUT_FILE} ({len(batters_today)} rows)")

if __name__ == "__main__":
    main()