import pandas as pd
from pathlib import Path

# File paths
INPUT_FILE = Path("data/Data/batters.csv")
OUTPUT_FILE = Path("data/end_chain/final/normalize_end/all_bat_col.csv")

# Rename mapping (aliases → expected)
RENAME_MAP = {
    "xwoba": "adj_woba_combined",
    "z_swing_miss_percent": "zone_swing_miss_percent",
    "oz_swing_miss_percent": "out_of_zone_swing_miss_percent",
    "groundballs_percent": "gb_percent",
    "flyballs_percent": "fb_percent",
    "strikeout": "strikeouts",
    "walk": "walks",
    # keep 'babip', 'b_rbi' as-is
}

def main():
    df = pd.read_csv(INPUT_FILE)

    # Only rename if all alias columns exist
    missing = [col for col in RENAME_MAP.keys() if col not in df.columns]
    if missing:
        print(f"⚠️ Missing expected input columns: {missing}")
        return

    df = df.rename(columns=RENAME_MAP)

    # Ensure output directory exists
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved normalized batter projection data to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
