import pandas as pd
from pathlib import Path

# Input file paths
HOME_FILE = Path("data/end_chain/final/batter_home_final.csv")
AWAY_FILE = Path("data/end_chain/final/batter_away_final.csv")
FILLER_FILE = Path("data/end_chain/final/normalize_end/all_bat_col.csv")

# List of columns to inject if missing or NaN
TARGET_COLUMNS = [
    "adj_woba_combined", "zone_swing_miss_percent", "out_of_zone_swing_miss_percent",
    "gb_percent", "fb_percent", "innings_pitched", "strikeouts",
    "babip", "walks", "b_rbi"
]

def fill_missing_columns(df_target: pd.DataFrame, df_source: pd.DataFrame) -> pd.DataFrame:
    for col in TARGET_COLUMNS:
        if col in df_source.columns:
            if col not in df_target.columns:
                df_target[col] = None
            df_target[col] = df_target[col].combine_first(df_source[col])
    return df_target

def main():
    print("🔄 Loading input files...")
    df_home = pd.read_csv(HOME_FILE)
    df_away = pd.read_csv(AWAY_FILE)
    df_filler = pd.read_csv(FILLER_FILE)

    if "player_id" not in df_home.columns or "player_id" not in df_away.columns:
        raise KeyError("❌ Missing 'player_id' column in one of the batter files.")

    if "player_id" not in df_filler.columns:
        raise KeyError("❌ Missing 'player_id' column in all_bat_col.csv")

    df_filler = df_filler.set_index("player_id")

    print("🛠️ Filling batter_home_final.csv...")
    df_home = df_home.set_index("player_id")
    df_home = fill_missing_columns(df_home, df_filler)
    df_home.reset_index().to_csv(HOME_FILE, index=False)
    print(f"✅ Updated: {HOME_FILE}")

    print("🛠️ Filling batter_away_final.csv...")
    df_away = df_away.set_index("player_id")
    df_away = fill_missing_columns(df_away, df_filler)
    df_away.reset_index().to_csv(AWAY_FILE, index=False)
    print(f"✅ Updated: {AWAY_FILE}")

if __name__ == "__main__":
    main()
