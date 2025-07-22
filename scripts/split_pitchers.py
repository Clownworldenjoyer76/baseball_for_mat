import pandas as pd
from pathlib import Path

# Input files
PITCHERS_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
REF_PITCHERS_FILE = "data/Data/pitchers.csv"
OUTPUT_DIR = "data/adjusted"

def normalize_string(s):
    return str(s).strip().lower()

def main():
    print("📥 Loading input files...")
    pitchers = pd.read_csv(PITCHERS_FILE)
    games = pd.read_csv(GAMES_FILE)
    ref_pitchers = pd.read_csv(REF_PITCHERS_FILE)

    print("🧹 Normalizing team and pitcher names...")
    pitchers['team'] = pitchers['team'].apply(normalize_string)
    pitchers['name'] = pitchers['name'].apply(normalize_string)
    games['home_team'] = games['home_team'].apply(normalize_string)
    games['away_team'] = games['away_team'].apply(normalize_string)
    games['pitcher_home'] = games['pitcher_home'].apply(normalize_string)
    games['pitcher_away'] = games['pitcher_away'].apply(normalize_string)

    print("🔍 Filtering pitchers by matchups...")
    home_pitchers = pd.merge(
        games[['home_team', 'pitcher_home']],
        pitchers,
        left_on=['home_team', 'pitcher_home'],
        right_on=['team', 'name'],
        how='inner'
    )

    away_pitchers = pd.merge(
        games[['away_team', 'pitcher_away']],
        pitchers,
        left_on=['away_team', 'pitcher_away'],
        right_on=['team', 'name'],
        how='inner'
    )

    print("🧾 Restoring name casing using reference data...")
    ref_pitchers['normalized'] = ref_pitchers['last_name, first_name'].str.strip().str.lower()
    name_map = dict(zip(ref_pitchers['normalized'], ref_pitchers['last_name, first_name']))

    def restore_format(df, pitcher_col):
        df['name'] = df['name'].apply(lambda x: name_map.get(x, x))
        if pitcher_col in df.columns:
            df[pitcher_col] = df[pitcher_col].apply(lambda x: name_map.get(x, x))
        return df

    home_pitchers = restore_format(home_pitchers, 'pitcher_home')
    away_pitchers = restore_format(away_pitchers, 'pitcher_away')

    print("💾 Saving results...")
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    home_pitchers.to_csv(f"{OUTPUT_DIR}/pitchers_home.csv", index=False)
    away_pitchers.to_csv(f"{OUTPUT_DIR}/pitchers_away.csv", index=False)

    print(f"✅ Saved {len(home_pitchers)} home pitchers and {len(away_pitchers)} away pitchers.")

if __name__ == "__main__":
    main()
